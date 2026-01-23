"""Runnable demo for MiniSpark concepts."""

import logging

from minispark.runtime import MiniSparkContext


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(name)s - %(message)s")


def demo_basic(sc: MiniSparkContext) -> None:
    input_rdd = sc.parallelize([15, 16, 17, 18, 19], num_partitions=2)
    result_rdd = input_rdd.map(lambda x: x * 10).filter(lambda x: x > 25)
    logging.info("Transformations built. Triggering collect...")
    print(result_rdd.collect(sc.driver))


def demo_fault_tolerance(sc: MiniSparkContext) -> None:
    base = sc.parallelize(list(range(6)), num_partitions=2)
    mapped = base.map(lambda x: x + 1)
    sc.driver.inject_failure(mapped, partition=1)
    logging.info("Injected partition loss for fault-tolerance demo.")
    print(mapped.collect(sc.driver))


def main() -> None:
    setup_logging()
    sc = MiniSparkContext(num_executors=2)
    demo_basic(sc)
    demo_fault_tolerance(sc)


if __name__ == "__main__":
    main()
