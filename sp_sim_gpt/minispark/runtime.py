import logging
from typing import List

from .core import Driver, Executor

logger = logging.getLogger(__name__)


class Cluster:
    """Local cluster that owns executors and exposes a driver."""

    def __init__(self, num_executors: int = 2) -> None:
        self.executors: List[Executor] = []
        self.driver: Driver
        self._build(num_executors)

    def _build(self, num_executors: int) -> None:
        scheduler_placeholder = None
        self.executors = [Executor(f"exec-{idx}", scheduler_placeholder) for idx in range(num_executors)]
        self.driver = Driver(self.executors)
        for executor in self.executors:
            executor.scheduler = self.driver.scheduler
        logger.info("Cluster started with %s executors", num_executors)


class MiniSparkContext:
    """User-facing entrypoint similar to SparkContext/SQLContext."""

    def __init__(self, num_executors: int = 2) -> None:
        self.cluster = Cluster(num_executors=num_executors)
        self.driver = self.cluster.driver

    def parallelize(self, data: list, num_partitions: int = 2):
        return self.driver.parallelize(data, num_partitions=num_partitions)

    def textFile(self, path: str, num_partitions: int = 2):
        return self.driver.textFile(path, num_partitions=num_partitions)
