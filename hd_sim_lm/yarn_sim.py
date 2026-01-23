import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

from hdfs_sim import DataNode, HDFSCluster
from mapreduce_sim import (
    MapReduceEngine,
    build_sample_text,
    wordcount_mapper,
    wordcount_reducer,
)


@dataclass
class Container:
    container_id: str
    node_id: str
    cores: int
    memory_mb: int
    task_id: str


@dataclass
class Task:
    task_id: str
    duration_s: float
    remaining_s: float
    task_type: str


@dataclass
class JobSpec:
    name: str
    input_path: str
    map_tasks: List[Task]
    reduce_tasks: List[Task]


class NodeManager:
    """Manages node-local resources and reports heartbeats to the RM."""

    def __init__(self, node_id: str, total_cores: int, total_memory_mb: int) -> None:
        self.node_id = node_id
        self.total_cores = total_cores
        self.total_memory_mb = total_memory_mb
        self.available_cores = total_cores
        self.available_memory_mb = total_memory_mb
        self.running: Dict[str, Container] = {}

    def can_allocate(self, cores: int, memory_mb: int) -> bool:
        return self.available_cores >= cores and self.available_memory_mb >= memory_mb

    def allocate(self, container: Container) -> None:
        if not self.can_allocate(container.cores, container.memory_mb):
            raise ValueError("Insufficient resources on node")
        self.available_cores -= container.cores
        self.available_memory_mb -= container.memory_mb
        self.running[container.container_id] = container

    def release(self, container_id: str) -> None:
        container = self.running.pop(container_id)
        self.available_cores += container.cores
        self.available_memory_mb += container.memory_mb

    def heartbeat(self) -> Dict[str, int]:
        return {
            "available_cores": self.available_cores,
            "available_memory_mb": self.available_memory_mb,
            "running_containers": len(self.running),
        }


class ResourceManager:
    """Schedules containers across NodeManagers using FIFO or Fair scheduling."""

    def __init__(self, scheduler: str = "fifo") -> None:
        if scheduler not in {"fifo", "fair"}:
            raise ValueError("Scheduler must be 'fifo' or 'fair'")
        self.scheduler = scheduler
        self.nodes: Dict[str, NodeManager] = {}
        self.queue: Deque["ApplicationMaster"] = deque()

    def register_node(self, node: NodeManager) -> None:
        self.nodes[node.node_id] = node

    def submit_application(self, app_master: "ApplicationMaster") -> None:
        self.queue.append(app_master)

    def schedule(self) -> None:
        if not self.queue:
            return
        if self.scheduler == "fifo":
            self._schedule_fifo()
        else:
            self._schedule_fair()

    def _schedule_fifo(self) -> None:
        current = self.queue[0]
        if current.is_complete():
            self.queue.popleft()
            return
        self._allocate_for_app(current)
        if current.is_complete():
            self.queue.popleft()

    def _schedule_fair(self) -> None:
        for _ in range(len(self.queue)):
            app = self.queue.popleft()
            if not app.is_complete():
                self._allocate_for_app(app)
                self.queue.append(app)

    def _allocate_for_app(self, app: "ApplicationMaster") -> None:
        while app.has_pending_tasks():
            allocation = self._find_allocation()
            if allocation is None:
                break
            node_id, cores, memory_mb = allocation
            container = app.request_container(node_id, cores, memory_mb)
            self.nodes[node_id].allocate(container)

    def _find_allocation(self) -> Optional[tuple]:
        for node in self.nodes.values():
            if node.can_allocate(1, 256):
                return node.node_id, 1, 256
        return None


class ApplicationMaster:
    """Tracks job progress and requests containers from the RM."""

    def __init__(self, job: JobSpec, engine: MapReduceEngine) -> None:
        self.job = job
        self.engine = engine
        self.app_id = f"app-{uuid.uuid4().hex[:8]}"
        self.pending_maps: Deque[Task] = deque(job.map_tasks)
        self.pending_reduces: Deque[Task] = deque(job.reduce_tasks)
        self.running_tasks: Dict[str, Task] = {}
        self.completed_tasks: List[str] = []
        self.output: Optional[Dict[str, List[tuple]]] = None

    def has_pending_tasks(self) -> bool:
        return bool(self.pending_maps or (not self.pending_maps and self.pending_reduces))

    def request_container(self, node_id: str, cores: int, memory_mb: int) -> Container:
        if self.pending_maps:
            task = self.pending_maps.popleft()
        else:
            task = self.pending_reduces.popleft()
        self.running_tasks[task.task_id] = task
        container_id = f"container-{uuid.uuid4().hex[:8]}"
        return Container(container_id=container_id, node_id=node_id, cores=cores, memory_mb=memory_mb, task_id=task.task_id)

    def tick(self, delta_s: float, node_managers: Dict[str, NodeManager]) -> None:
        finished: List[str] = []
        for task_id, task in self.running_tasks.items():
            task.remaining_s -= delta_s
            if task.remaining_s <= 0:
                finished.append(task_id)
        for task_id in finished:
            self.running_tasks.pop(task_id)
            self.completed_tasks.append(task_id)
            for node in node_managers.values():
                for container in list(node.running.values()):
                    if container.task_id == task_id:
                        node.release(container.container_id)
        if self.is_complete() and self.output is None:
            self.output = self.engine.submit_job(wordcount_mapper, wordcount_reducer, self.job.input_path)

    def is_complete(self) -> bool:
        return not (self.pending_maps or self.pending_reduces or self.running_tasks)

    def progress(self) -> float:
        total = len(self.job.map_tasks) + len(self.job.reduce_tasks)
        if total == 0:
            return 1.0
        return len(self.completed_tasks) / total


class YarnCluster:
    """Coordinates RM, NMs, and AMs for job scheduling."""

    def __init__(self, resource_manager: ResourceManager, nodes: List[NodeManager]) -> None:
        self.rm = resource_manager
        self.nodes = nodes
        for node in nodes:
            self.rm.register_node(node)
        self.applications: List[ApplicationMaster] = []

    def submit_job(self, app_master: ApplicationMaster) -> None:
        self.applications.append(app_master)
        self.rm.submit_application(app_master)

    def run_until_complete(self, tick_s: float = 0.2, max_ticks: int = 500) -> None:
        for _ in range(max_ticks):
            self.rm.schedule()
            for app in self.applications:
                app.tick(tick_s, self.rm.nodes)
            if all(app.is_complete() for app in self.applications):
                return
            time.sleep(tick_s)
        raise RuntimeError("Simulation did not complete within tick limit")

    def status_report(self) -> List[str]:
        reports = []
        for app in self.applications:
            reports.append(f"{app.app_id} {app.job.name} progress={app.progress():.0%}")
        return reports


def build_wordcount_job(name: str, input_path: str, map_count: int, slow_map: bool = False) -> JobSpec:
    map_tasks = []
    for index in range(map_count):
        duration = 0.3 if not slow_map else (1.5 if index == 0 else 0.3)
        map_tasks.append(Task(task_id=f"{name}-map-{index}", duration_s=duration, remaining_s=duration, task_type="map"))
    reduce_tasks = [
        Task(task_id=f"{name}-reduce-0", duration_s=0.4, remaining_s=0.4, task_type="reduce"),
    ]
    return JobSpec(name=name, input_path=input_path, map_tasks=map_tasks, reduce_tasks=reduce_tasks)


def main() -> None:
    datanodes = [
        DataNode("dn-1", cache_capacity=6, disk_latency=0.01),
        DataNode("dn-2", cache_capacity=6, disk_latency=0.01),
        DataNode("dn-3", cache_capacity=6, disk_latency=0.01),
    ]
    hdfs = HDFSCluster(datanodes, block_size=512, replication=2)
    build_sample_text("wordcount.txt")
    hdfs.put("wordcount.txt")

    engine = MapReduceEngine(hdfs, num_reducers=2, network_latency_per_kb=0.0003)

    rm = ResourceManager(scheduler="fair")
    nodes = [
        NodeManager("nm-1", total_cores=2, total_memory_mb=2048),
        NodeManager("nm-2", total_cores=2, total_memory_mb=2048),
    ]
    cluster = YarnCluster(rm, nodes)

    job_a = build_wordcount_job("job-a", "wordcount.txt", map_count=4)
    job_b = build_wordcount_job("job-b", "wordcount.txt", map_count=4)
    job_c = build_wordcount_job("job-c", "wordcount.txt", map_count=4, slow_map=True)

    for job in (job_a, job_b, job_c):
        cluster.submit_job(ApplicationMaster(job, engine))

    print("Submitting 3 WordCount jobs (fair scheduler)...")
    while not all(app.is_complete() for app in cluster.applications):
        cluster.rm.schedule()
        for app in cluster.applications:
            app.tick(0.2, cluster.rm.nodes)
        print(" | ".join(cluster.status_report()))
        time.sleep(0.2)

    print("\nLong-tail job output sample:")
    for app in cluster.applications:
        if app.job.name == "job-c":
            print(app.output)


if __name__ == "__main__":
    main()
