import logging
import os
from pathlib import Path

from minispark.runtime import Cluster
from minispark.sql import DataFrameReader, Field, Schema


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(name)s - %(message)s",
    )


def demo_lazy_execution(cluster: Cluster) -> None:
    driver = cluster.driver
    data_path = Path("demo_text.txt")
    data_path.write_text("\n".join(["spark demo", "spark is fast", "lazy execution"]), encoding="utf-8")

    rdd = driver.textFile(str(data_path), num_partitions=2)
    transformed = rdd.filter(lambda line: "spark" in line).map(lambda line: line.upper())
    logging.info("RDD transformations defined; no execution yet.")
    result = transformed.take(driver, 2)
    logging.info("Action triggered; result: %s", result)


def demo_shuffle_and_stage(cluster: Cluster) -> None:
    driver = cluster.driver
    data = ["a", "b", "a", "c", "b", "a"]
    rdd = driver.parallelize(data, num_partitions=2)
    pairs = rdd.keyBy(lambda x: x).map(lambda kv: (kv[0], 1))
    counts = pairs.reduceByKey(lambda a, b: a + b)
    output = counts.collect(driver)
    logging.info("Shuffle reduceByKey result: %s", output)


def demo_lineage_recovery(cluster: Cluster) -> None:
    driver = cluster.driver
    base = driver.parallelize(list(range(10)), num_partitions=2)
    mapped = base.map(lambda x: x * 2)
    driver.inject_failure(mapped, partition=0)
    logging.info("Injecting failure for partition 0 to simulate recomputation.")
    result = mapped.collect(driver)
    logging.info("Recovered result after recomputation: %s", result)


def demo_dataframe(cluster: Cluster) -> None:
    driver = cluster.driver
    csv_path = Path("demo_data.csv")
    csv_path.write_text(
        "city,category,amount\n"
        "sh,food,10\n"
        "sh,tech,20\n"
        "bj,food,5\n"
        "bj,tech,7\n",
        encoding="utf-8",
    )
    schema = Schema([Field("city", "str"), Field("category", "str"), Field("amount", "int")])
    df = DataFrameReader(driver).csv(str(csv_path), schema)
    df = df.select("city", "amount").where(lambda row: row["amount"] > 6).groupBy("city").agg("amount")
    df.show(driver, n=10)


def main() -> None:
    setup_logging()
    cluster = Cluster(num_executors=2)
    demo_lazy_execution(cluster)
    demo_shuffle_and_stage(cluster)
    demo_lineage_recovery(cluster)
    demo_dataframe(cluster)
    logging.info("Demo completed. Run with: python demo.py")
    for file_name in ["demo_text.txt", "demo_data.csv"]:
        if os.path.exists(file_name):
            os.remove(file_name)


if __name__ == "__main__":
    main()
