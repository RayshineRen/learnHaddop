import os
import random
import re
import time
from collections import defaultdict
from typing import Callable, Dict, Iterable, List, Sequence, Tuple

from hdfs_sim import DataNode, HDFSCluster

KeyValue = Tuple[str, str]
Mapper = Callable[[str, bytes], Iterable[KeyValue]]
Reducer = Callable[[str, Sequence[str]], Iterable[Tuple[str, object]]]


def default_partitioner(key: str, num_reducers: int) -> int:
    return hash(key) % num_reducers


class MapReduceEngine:
    """A minimal MapReduce engine with shuffle + network simulation."""

    def __init__(
        self,
        hdfs: HDFSCluster,
        num_reducers: int = 2,
        network_latency_per_kb: float = 0.001,
    ) -> None:
        self.hdfs = hdfs
        self.num_reducers = num_reducers
        self.network_latency_per_kb = network_latency_per_kb

    def submit_job(
        self,
        mapper: Mapper,
        reducer: Reducer,
        input_path: str,
    ) -> Dict[str, List[Tuple[str, object]]]:
        blocks = self.hdfs.get_blocks(input_path)
        partitions: Dict[int, List[KeyValue]] = defaultdict(list)

        for block_id, data in blocks:
            for key, value in mapper(block_id, data):
                partition_id = default_partitioner(key, self.num_reducers)
                partitions[partition_id].append((key, value))

        reducer_inputs: Dict[int, Dict[str, List[str]]] = {}
        for partition_id in range(self.num_reducers):
            pairs = partitions.get(partition_id, [])
            self._simulate_network_transfer(pairs)
            grouped: Dict[str, List[str]] = defaultdict(list)
            for key, value in sorted(pairs, key=lambda item: item[0]):
                grouped[key].append(value)
            reducer_inputs[partition_id] = grouped

        outputs: Dict[str, List[Tuple[str, object]]] = {}
        for partition_id, grouped in reducer_inputs.items():
            reduced: List[Tuple[str, object]] = []
            for key, values in grouped.items():
                reduced.extend(list(reducer(key, values)))
            outputs[f"reducer-{partition_id}"] = reduced
        return outputs

    def _simulate_network_transfer(self, pairs: List[KeyValue]) -> None:
        if not pairs:
            return
        payload_bytes = sum(len(key.encode("utf-8")) + len(value.encode("utf-8")) for key, value in pairs)
        payload_kb = payload_bytes / 1024
        time.sleep(payload_kb * self.network_latency_per_kb)


WORD_RE = re.compile(r"\b[\w']+\b")


def wordcount_mapper(_: str, data: bytes) -> Iterable[KeyValue]:
    text = data.decode("utf-8")
    for word in WORD_RE.findall(text.lower()):
        yield word, "1"


def wordcount_reducer(key: str, values: Sequence[str]) -> Iterable[Tuple[str, int]]:
    yield key, sum(int(v) for v in values)


LOG_LINE_RE = re.compile(r"^(?P<url>\S+)\s+(?P<user>\S+)$")


def pv_uv_mapper(_: str, data: bytes) -> Iterable[KeyValue]:
    text = data.decode("utf-8")
    for line in text.splitlines():
        match = LOG_LINE_RE.match(line)
        if not match:
            continue
        yield match.group("url"), match.group("user")


def pv_uv_reducer(key: str, values: Sequence[str]) -> Iterable[Tuple[str, Dict[str, int]]]:
    pv = len(values)
    uv = len(set(values))
    yield key, {"pv": pv, "uv": uv}


def build_sample_text(path: str) -> None:
    if os.path.exists(path):
        return
    lines = [
        "MapReduce makes big data easier.",
        "Hadoop inspired many distributed systems.",
        "MapReduce map map reduce reduce.",
    ]
    with open(path, "w", encoding="utf-8") as handle:
        for line in lines * 200:
            handle.write(line + "\n")


def build_sample_logs(path: str, urls: List[str], users: List[str], lines: int = 5000) -> None:
    if os.path.exists(path):
        return
    with open(path, "w", encoding="utf-8") as handle:
        for _ in range(lines):
            url = random.choice(urls)
            user = random.choice(users)
            handle.write(f"{url} {user}\n")


def main() -> None:
    datanodes = [
        DataNode("dn-1", cache_capacity=8, disk_latency=0.01),
        DataNode("dn-2", cache_capacity=8, disk_latency=0.01),
        DataNode("dn-3", cache_capacity=8, disk_latency=0.01),
    ]
    cluster = HDFSCluster(datanodes, block_size=512, replication=2)

    wordcount_path = "wordcount.txt"
    build_sample_text(wordcount_path)
    cluster.put(wordcount_path)

    logs_path = "access_logs.txt"
    build_sample_logs(
        logs_path,
        urls=["/home", "/product", "/cart", "/search"],
        users=[f"user-{i}" for i in range(100)],
        lines=8000,
    )
    cluster.put(logs_path)

    engine = MapReduceEngine(cluster, num_reducers=2, network_latency_per_kb=0.0005)

    print("WordCount job:")
    wordcount_result = engine.submit_job(wordcount_mapper, wordcount_reducer, wordcount_path)
    for reducer_id, pairs in wordcount_result.items():
        print(reducer_id, sorted(pairs)[:5])

    print("\nPV/UV job (simulated 100TB by scaling log lines):")
    pvuv_result = engine.submit_job(pv_uv_mapper, pv_uv_reducer, logs_path)
    for reducer_id, pairs in pvuv_result.items():
        print(reducer_id, pairs)


if __name__ == "__main__":
    main()
