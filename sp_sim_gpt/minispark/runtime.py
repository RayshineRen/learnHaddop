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
