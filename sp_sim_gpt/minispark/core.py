import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

Partition = int
KeyValue = Tuple[Any, Any]


@dataclass
class Dependency:
    parent: "RDD"
    dep_type: str  # "narrow" or "wide"


@dataclass
class Task:
    stage_id: int
    partition: int
    rdd: "RDD"


@dataclass
class Stage:
    stage_id: int
    rdd: "RDD"
    parents: List["Stage"]
    is_shuffle: bool


class ShuffleManager:
    def __init__(self) -> None:
        self._data: Dict[Tuple[int, int], List[KeyValue]] = defaultdict(list)

    def write(self, shuffle_id: int, partition: int, records: Iterable[KeyValue]) -> None:
        self._data[(shuffle_id, partition)].extend(records)

    def read(self, shuffle_id: int, partition: int) -> List[KeyValue]:
        return list(self._data.get((shuffle_id, partition), []))


class RDD:
    """Minimal RDD abstraction with lineage and dependencies."""

    _next_id = 0

    def __init__(self, num_partitions: int, deps: Optional[List[Dependency]] = None, lineage: str = "") -> None:
        self.id = RDD._next_id
        RDD._next_id += 1
        self.num_partitions = num_partitions
        self.deps = deps or []
        self.lineage = lineage or f"RDD[{self.id}]"

    def compute(self, partition: Partition, context: "TaskContext") -> Iterable[Any]:
        raise NotImplementedError

    def map(self, func: Callable[[Any], Any]) -> "RDD":
        return MapRDD(self, func)

    def filter(self, func: Callable[[Any], bool]) -> "RDD":
        return FilterRDD(self, func)

    def flatMap(self, func: Callable[[Any], Iterable[Any]]) -> "RDD":
        return FlatMapRDD(self, func)

    def mapPartitions(self, func: Callable[[Iterable[Any]], Iterable[Any]]) -> "RDD":
        return MapPartitionsRDD(self, func)

    def keyBy(self, func: Callable[[Any], Any]) -> "RDD":
        return KeyByRDD(self, func)

    def reduceByKey(self, func: Callable[[Any, Any], Any]) -> "RDD":
        return ReduceByKeyRDD(self, func)

    def join(self, other: "RDD") -> "RDD":
        return JoinRDD(self, other)

    def collect(self, driver: "Driver") -> List[Any]:
        return driver.run_job(self, action="collect")

    def count(self, driver: "Driver") -> int:
        return driver.run_job(self, action="count")

    def take(self, driver: "Driver", n: int) -> List[Any]:
        return driver.run_job(self, action="take", action_arg=n)

    def saveAsTextFile(self, driver: "Driver", path: str) -> None:
        driver.run_job(self, action="save", action_arg=path)


class ParallelizeRDD(RDD):
    def __init__(self, data: List[Any], num_partitions: int) -> None:
        self._data = data
        super().__init__(num_partitions=num_partitions, lineage=f"Parallelize[{num_partitions}]")

    def compute(self, partition: Partition, context: "TaskContext") -> Iterable[Any]:
        size = len(self._data)
        chunk = max(1, size // self.num_partitions)
        start = partition * chunk
        end = size if partition == self.num_partitions - 1 else (partition + 1) * chunk
        return self._data[start:end]


class TextFileRDD(RDD):
    def __init__(self, path: str, num_partitions: int) -> None:
        self._path = path
        super().__init__(num_partitions=num_partitions, lineage=f"TextFile[{os.path.basename(path)}]")

    def compute(self, partition: Partition, context: "TaskContext") -> Iterable[str]:
        with open(self._path, "r", encoding="utf-8") as handle:
            lines = handle.readlines()
        size = len(lines)
        chunk = max(1, size // self.num_partitions)
        start = partition * chunk
        end = size if partition == self.num_partitions - 1 else (partition + 1) * chunk
        return [line.strip() for line in lines[start:end]]


class UnaryRDD(RDD):
    def __init__(self, parent: RDD, func: Callable, name: str) -> None:
        self.parent = parent
        self.func = func
        deps = [Dependency(parent=parent, dep_type="narrow")]
        super().__init__(num_partitions=parent.num_partitions, deps=deps, lineage=f"{name} <- {parent.lineage}")


class MapRDD(UnaryRDD):
    def __init__(self, parent: RDD, func: Callable[[Any], Any]) -> None:
        super().__init__(parent, func, name="Map")

    def compute(self, partition: Partition, context: "TaskContext") -> Iterable[Any]:
        return [self.func(x) for x in context.compute_parent(self.parent, partition)]


class FilterRDD(UnaryRDD):
    def __init__(self, parent: RDD, func: Callable[[Any], bool]) -> None:
        super().__init__(parent, func, name="Filter")

    def compute(self, partition: Partition, context: "TaskContext") -> Iterable[Any]:
        return [x for x in context.compute_parent(self.parent, partition) if self.func(x)]


class FlatMapRDD(UnaryRDD):
    def __init__(self, parent: RDD, func: Callable[[Any], Iterable[Any]]) -> None:
        super().__init__(parent, func, name="FlatMap")

    def compute(self, partition: Partition, context: "TaskContext") -> Iterable[Any]:
        output: List[Any] = []
        for item in context.compute_parent(self.parent, partition):
            output.extend(list(self.func(item)))
        return output


class MapPartitionsRDD(UnaryRDD):
    def __init__(self, parent: RDD, func: Callable[[Iterable[Any]], Iterable[Any]]) -> None:
        super().__init__(parent, func, name="MapPartitions")

    def compute(self, partition: Partition, context: "TaskContext") -> Iterable[Any]:
        return list(self.func(context.compute_parent(self.parent, partition)))


class KeyByRDD(UnaryRDD):
    def __init__(self, parent: RDD, func: Callable[[Any], Any]) -> None:
        super().__init__(parent, func, name="KeyBy")

    def compute(self, partition: Partition, context: "TaskContext") -> Iterable[KeyValue]:
        return [(self.func(x), x) for x in context.compute_parent(self.parent, partition)]


class ReduceByKeyRDD(RDD):
    def __init__(self, parent: RDD, func: Callable[[Any, Any], Any]) -> None:
        self.parent = parent
        self.func = func
        deps = [Dependency(parent=parent, dep_type="wide")]
        super().__init__(num_partitions=parent.num_partitions, deps=deps, lineage=f"ReduceByKey <- {parent.lineage}")

    def compute(self, partition: Partition, context: "TaskContext") -> Iterable[KeyValue]:
        data = context.read_shuffle(self.parent.id, partition)
        grouped: Dict[Any, Any] = {}
        for key, value in data:
            if key in grouped:
                grouped[key] = self.func(grouped[key], value)
            else:
                grouped[key] = value
        return list(grouped.items())


class JoinRDD(RDD):
    def __init__(self, left: RDD, right: RDD) -> None:
        self.left = left
        self.right = right
        deps = [Dependency(parent=left, dep_type="wide"), Dependency(parent=right, dep_type="wide")]
        super().__init__(num_partitions=max(left.num_partitions, right.num_partitions), deps=deps, lineage=f"Join <- ({left.lineage}, {right.lineage})")

    def compute(self, partition: Partition, context: "TaskContext") -> Iterable[KeyValue]:
        left_data = context.read_shuffle(self.left.id, partition)
        right_data = context.read_shuffle(self.right.id, partition)
        right_index: Dict[Any, List[Any]] = defaultdict(list)
        for key, value in right_data:
            right_index[key].append(value)
        output: List[KeyValue] = []
        for key, value in left_data:
            for right_value in right_index.get(key, []):
                output.append((key, (value, right_value)))
        return output


class TaskContext:
    def __init__(self, scheduler: "Scheduler") -> None:
        self.scheduler = scheduler

    def compute_parent(self, parent: RDD, partition: Partition) -> Iterable[Any]:
        return self.scheduler.compute_partition(parent, partition)

    def read_shuffle(self, shuffle_id: int, partition: Partition) -> List[KeyValue]:
        return self.scheduler.shuffle_manager.read(shuffle_id, partition)


class Scheduler:
    def __init__(self, executors: List["Executor"]) -> None:
        self.executors = executors
        self.shuffle_manager = ShuffleManager()
        self.failed_partitions: Dict[Tuple[int, int], bool] = {}

    def inject_failure(self, rdd_id: int, partition: int) -> None:
        self.failed_partitions[(rdd_id, partition)] = True

    def build_stages(self, rdd: RDD) -> Stage:
        parents = []
        is_shuffle = False
        for dep in rdd.deps:
            parent_stage = self.build_stages(dep.parent)
            parents.append(parent_stage)
            if dep.dep_type == "wide":
                is_shuffle = True
        stage = Stage(stage_id=rdd.id, rdd=rdd, parents=parents, is_shuffle=is_shuffle)
        return stage

    def log_dag(self, stage: Stage, depth: int = 0) -> None:
        indent = "  " * depth
        logger.info("%sStage %s (RDD %s) shuffle=%s", indent, stage.stage_id, stage.rdd.id, stage.is_shuffle)
        for parent in stage.parents:
            self.log_dag(parent, depth + 1)

    def run(self, rdd: RDD, action: str, action_arg: Optional[Any] = None) -> Any:
        stage = self.build_stages(rdd)
        logger.info("DAG structure:")
        self.log_dag(stage)
        stages = self._collect_stages(stage)
        logger.info("Stage split result (shuffle boundaries marked): %s", [(s.stage_id, s.is_shuffle) for s in stages])
        logger.info("Lineage for RDD %s: %s", rdd.id, rdd.lineage)
        results = []
        for partition in range(rdd.num_partitions):
            results.extend(self.execute_task(Task(stage.stage_id, partition, rdd)))
        if action == "collect":
            return results
        if action == "count":
            return len(results)
        if action == "take":
            return results[: int(action_arg)]
        if action == "save":
            self._save_results(results, str(action_arg))
            return None
        raise ValueError(f"Unknown action {action}")

    def execute_task(self, task: Task) -> List[Any]:
        executor = self.executors[task.partition % len(self.executors)]
        logger.info("Scheduling task: stage=%s partition=%s -> executor=%s", task.stage_id, task.partition, executor.executor_id)
        logger.info("Lineage path for partition %s (RDD %s): %s", task.partition, task.rdd.id, task.rdd.lineage)
        context = TaskContext(self)
        return executor.execute(task, context)

    def compute_partition(self, rdd: RDD, partition: Partition) -> Iterable[Any]:
        if isinstance(rdd, ReduceByKeyRDD) or isinstance(rdd, JoinRDD):
            return rdd.compute(partition, TaskContext(self))
        if any(dep.dep_type == "wide" for dep in rdd.deps):
            self._run_shuffle(rdd)
        return rdd.compute(partition, TaskContext(self))

    def _run_shuffle(self, rdd: RDD) -> None:
        for dep in rdd.deps:
            if dep.dep_type != "wide":
                continue
            parent = dep.parent
            logger.info("Shuffle boundary detected: parent RDD %s -> child RDD %s", parent.id, rdd.id)
            for part in range(parent.num_partitions):
                records = list(self.compute_partition(parent, part))
                buckets: Dict[int, List[KeyValue]] = defaultdict(list)
                for key, value in records:
                    bucket = hash(key) % rdd.num_partitions
                    buckets[bucket].append((key, value))
                for bucket, data in buckets.items():
                    logger.info("Shuffle route: key partition=%s -> reduce partition=%s", part, bucket)
                    self.shuffle_manager.write(parent.id, bucket, data)

    def _save_results(self, results: List[Any], path: str) -> None:
        os.makedirs(path, exist_ok=True)
        output_path = os.path.join(path, "part-00000")
        with open(output_path, "w", encoding="utf-8") as handle:
            for item in results:
                handle.write(f"{item}\n")

    def _collect_stages(self, stage: Stage) -> List[Stage]:
        seen: Dict[int, Stage] = {}

        def visit(node: Stage) -> None:
            if node.stage_id in seen:
                return
            seen[node.stage_id] = node
            for parent in node.parents:
                visit(parent)

        visit(stage)
        return list(seen.values())


class Executor:
    def __init__(self, executor_id: str, scheduler: Scheduler) -> None:
        self.executor_id = executor_id
        self.scheduler = scheduler
        self.cache: Dict[Tuple[int, int], List[Any]] = {}

    def execute(self, task: Task, context: TaskContext) -> List[Any]:
        key = (task.rdd.id, task.partition)
        if key in self.cache:
            logger.info("Executor %s cache hit for RDD %s partition %s", self.executor_id, task.rdd.id, task.partition)
            return self.cache[key]
        if self.scheduler.failed_partitions.pop(key, None):
            logger.warning("Executor %s lost partition %s of RDD %s", self.executor_id, task.partition, task.rdd.id)
            raise RuntimeError("Simulated partition loss")
        result = list(task.rdd.compute(task.partition, context))
        self.cache[key] = result
        return result


class Driver:
    def __init__(self, executors: List[Executor]) -> None:
        self.scheduler = Scheduler(executors)

    def parallelize(self, data: List[Any], num_partitions: int = 2) -> RDD:
        return ParallelizeRDD(data, num_partitions)

    def textFile(self, path: str, num_partitions: int = 2) -> RDD:
        return TextFileRDD(path, num_partitions)

    def run_job(self, rdd: RDD, action: str, action_arg: Optional[Any] = None) -> Any:
        attempt = 0
        while True:
            try:
                return self.scheduler.run(rdd, action, action_arg)
            except RuntimeError as exc:
                attempt += 1
                logger.warning("Job failed (%s). Recomputing via lineage (attempt %s)", exc, attempt)
                if attempt > 2:
                    raise

    def inject_failure(self, rdd: RDD, partition: int) -> None:
        self.scheduler.inject_failure(rdd.id, partition)
