import os
import random
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class BlockMetadata:
    block_id: str
    size: int


class NameNode:
    """Tracks file-to-block mappings and block locations."""

    def __init__(self) -> None:
        self._file_to_blocks: Dict[str, List[BlockMetadata]] = {}
        self._block_locations: Dict[str, List[str]] = {}

    def register_file(self, file_path: str, blocks: List[BlockMetadata]) -> None:
        self._file_to_blocks[file_path] = blocks

    def register_block_locations(self, block_id: str, datanode_ids: List[str]) -> None:
        self._block_locations[block_id] = datanode_ids

    def get_file_blocks(self, file_path: str) -> List[BlockMetadata]:
        if file_path not in self._file_to_blocks:
            raise FileNotFoundError(f"File not found in NameNode metadata: {file_path}")
        return self._file_to_blocks[file_path]

    def get_block_locations(self, block_id: str) -> List[str]:
        if block_id not in self._block_locations:
            raise KeyError(f"Block metadata missing for block_id={block_id}")
        return self._block_locations[block_id]


class DataNode:
    """Simulates disk and cache storage for HDFS blocks."""

    def __init__(self, node_id: str, cache_capacity: int = 8, disk_latency: float = 0.02) -> None:
        self.node_id = node_id
        self._disk: Dict[str, bytes] = {}
        self._cache: "OrderedDict[str, bytes]" = OrderedDict()
        self.cache_capacity = cache_capacity
        self.disk_latency = disk_latency
        self.cache_hits = 0
        self.cache_misses = 0

    def store_block(self, block_id: str, data: bytes) -> None:
        self._disk[block_id] = data

    def read_block(self, block_id: str) -> bytes:
        if block_id in self._cache:
            self.cache_hits += 1
            self._cache.move_to_end(block_id)
            return self._cache[block_id]

        if block_id not in self._disk:
            raise KeyError(f"Block {block_id} not found on DataNode {self.node_id}")

        self.cache_misses += 1
        time.sleep(self.disk_latency)
        data = self._disk[block_id]
        self._cache[block_id] = data
        if len(self._cache) > self.cache_capacity:
            self._cache.popitem(last=False)
        return data


class HDFSCluster:
    """Facade exposing put/get for the simulated cluster."""

    def __init__(self, datanodes: List[DataNode], block_size: int = 128 * 1024 * 1024, replication: int = 3) -> None:
        if replication > len(datanodes):
            raise ValueError("Replication factor exceeds available DataNodes")
        self.namenode = NameNode()
        self.datanodes = {node.node_id: node for node in datanodes}
        self.block_size = block_size
        self.replication = replication

    def _split_into_blocks(self, data: bytes) -> List[bytes]:
        return [data[i:i + self.block_size] for i in range(0, len(data), self.block_size)]

    def put(self, file_path: str) -> None:
        with open(file_path, "rb") as handle:
            data = handle.read()

        blocks = self._split_into_blocks(data)
        metadata: List[BlockMetadata] = []
        for index, block in enumerate(blocks):
            block_id = f"{os.path.basename(file_path)}_block_{index}"
            metadata.append(BlockMetadata(block_id=block_id, size=len(block)))
            chosen_nodes = random.sample(list(self.datanodes.values()), self.replication)
            for node in chosen_nodes:
                node.store_block(block_id, block)
            self.namenode.register_block_locations(block_id, [node.node_id for node in chosen_nodes])

        self.namenode.register_file(file_path, metadata)

    def get(self, file_path: str) -> Tuple[bytes, float]:
        blocks = self.namenode.get_file_blocks(file_path)
        retrieved: List[bytes] = []
        start_time = time.perf_counter()
        for block in blocks:
            datanode_ids = self.namenode.get_block_locations(block.block_id)
            # Choose a random replica for each block
            chosen_id = random.choice(datanode_ids)
            data = self.datanodes[chosen_id].read_block(block.block_id)
            retrieved.append(data)
        elapsed = time.perf_counter() - start_time
        return b"".join(retrieved), elapsed

    def get_blocks(self, file_path: str) -> List[Tuple[str, bytes]]:
        blocks = self.namenode.get_file_blocks(file_path)
        retrieved: List[Tuple[str, bytes]] = []
        for block in blocks:
            datanode_ids = self.namenode.get_block_locations(block.block_id)
            chosen_id = random.choice(datanode_ids)
            data = self.datanodes[chosen_id].read_block(block.block_id)
            retrieved.append((block.block_id, data))
        return retrieved

    def reset_cache_metrics(self) -> None:
        for node in self.datanodes.values():
            node.cache_hits = 0
            node.cache_misses = 0

    def cache_report(self) -> Dict[str, Tuple[int, int]]:
        return {
            node.node_id: (node.cache_hits, node.cache_misses)
            for node in self.datanodes.values()
        }


def build_test_file(path: str, lines: int = 2000) -> None:
    if os.path.exists(path):
        return
    with open(path, "w", encoding="utf-8") as handle:
        for index in range(lines):
            handle.write(f"Line {index}: Hadoop HDFS simulation cache test.\n")


def main() -> None:
    test_path = "test.txt"
    build_test_file(test_path)

    datanodes = [
        DataNode("dn-1", cache_capacity=4),
        DataNode("dn-2", cache_capacity=4),
        DataNode("dn-3", cache_capacity=4),
        DataNode("dn-4", cache_capacity=4),
    ]
    cluster = HDFSCluster(datanodes, block_size=1024, replication=3)

    print("Uploading test.txt to simulated HDFS...")
    cluster.put(test_path)

    print("\nFirst read (expect cache misses):")
    cluster.reset_cache_metrics()
    _, first_time = cluster.get(test_path)
    print(f"Elapsed: {first_time:.4f}s")
    print(f"Cache metrics: {cluster.cache_report()}")

    print("\nSecond read (expect cache hits):")
    cluster.reset_cache_metrics()
    _, second_time = cluster.get(test_path)
    print(f"Elapsed: {second_time:.4f}s")
    print(f"Cache metrics: {cluster.cache_report()}")


if __name__ == "__main__":
    main()
