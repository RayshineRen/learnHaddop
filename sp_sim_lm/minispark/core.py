"""MiniSpark core: RDD, dependencies, DAG scheduler, and simple fault recovery."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

Partition = int


@dataclass
class Dependency:
    parent: "RDD"
    dep_type: str  # "narrow" or "wide"


@dataclass
class Task:
    rdd: "RDD"
    partition: Partition


@dataclass
class Stage:
    stage_id: int
    rdd: "RDD"
    parents: List["Stage"]
    is_shuffle: bool


class RDD:
    """Immutable, partitioned dataset with lineage metadata."""

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

    def collect(self, driver: "Driver") -> List[Any]:
        return driver.run_job(self, action="collect")

    def count(self, driver: "Driver") -> int:
        return driver.run_job(self, action="count")


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
        return [self.func(item) for item in context.compute_parent(self.parent, partition)]


class FilterRDD(UnaryRDD):
    def __init__(self, parent: RDD, func: Callable[[Any], bool]) -> None:
        super().__init__(parent, func, name="Filter")

    def compute(self, partition: Partition, context: "TaskContext") -> Iterable[Any]:
        return [item for item in context.compute_parent(self.parent, partition) if self.func(item)]


class TaskContext:
    def __init__(self, scheduler: "Scheduler") -> None:
        self.scheduler = scheduler

    def compute_parent(self, parent: RDD, partition: Partition) -> Iterable[Any]:
        return self.scheduler.compute_partition(parent, partition)


class Scheduler:
    """Builds DAG/stages and dispatches tasks to executors."""

    def __init__(self, executors: List["Executor"]) -> None:
        self.executors = executors
        self.failed_partitions: Dict[Tuple[int, int], bool] = {}

    def inject_failure(self, rdd_id: int, partition: int) -> None:
        self.failed_partitions[(rdd_id, partition)] = True

    def build_stage(self, rdd: RDD) -> Stage:
        parents: List[Stage] = []
        is_shuffle = False
        for dep in rdd.deps:
            parents.append(self.build_stage(dep.parent))
            if dep.dep_type == "wide":
                is_shuffle = True
        return Stage(stage_id=rdd.id, rdd=rdd, parents=parents, is_shuffle=is_shuffle)

    def run(self, rdd: RDD, action: str) -> Any:
        stage = self.build_stage(rdd)
        logger.info("DAG root stage=%s shuffle=%s", stage.stage_id, stage.is_shuffle)
        logger.info("Lineage for RDD %s: %s", rdd.id, rdd.lineage)
        results: List[Any] = []
        for partition in range(rdd.num_partitions):
            results.extend(self.execute_task(Task(rdd=rdd, partition=partition)))
        if action == "collect":
            return results
        if action == "count":
            return len(results)
        raise ValueError(f"Unknown action: {action}")

    def execute_task(self, task: Task) -> List[Any]:
        executor = self.executors[task.partition % len(self.executors)]
        logger.info("Dispatch task partition=%s to executor=%s", task.partition, executor.executor_id)
        context = TaskContext(self)
        return executor.execute(task, context)

    def compute_partition(self, rdd: RDD, partition: Partition) -> Iterable[Any]:
        return rdd.compute(partition, TaskContext(self))


class Executor:
    """Simulated long-lived worker that keeps partition data in memory."""

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
    """Driver builds DAGs and triggers scheduler execution."""

    def __init__(self, executors: List[Executor]) -> None:
        self.scheduler = Scheduler(executors)

    def parallelize(self, data: List[Any], num_partitions: int = 2) -> RDD:
        return ParallelizeRDD(data, num_partitions)

    def run_job(self, rdd: RDD, action: str) -> Any:
        attempt = 0
        while True:
            try:
                return self.scheduler.run(rdd, action)
            except RuntimeError as exc:
                attempt += 1
                logger.warning("Job failed (%s). Recomputing via lineage (attempt %s)", exc, attempt)
                if attempt > 2:
                    raise

    def inject_failure(self, rdd: RDD, partition: int) -> None:
        self.scheduler.inject_failure(rdd.id, partition)
