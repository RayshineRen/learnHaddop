#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
                    Hadoop 端到端学习案例 - 完整演示脚本
=============================================================================

案例背景：电商平台访问日志分析
目标：计算每个商品页面的 PV（页面访问量）和 UV（独立访客数）

本脚本整合了 HDFS、MapReduce、YARN 三个模块的模拟实现，
以"教学优先、概念映射清晰"为原则，带你完整走一遍 Hadoop 作业的执行流程。

运行方式：
    python end_to_end_demo.py

作者：Hadoop 学习助手
"""

import os
import re
import random
import time
import uuid
from collections import defaultdict, deque, OrderedDict
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

# ============================================================================
#                              工具函数
# ============================================================================

def print_banner(title: str, char: str = "=") -> None:
    """打印分隔横幅"""
    width = 76
    print()
    print(char * width)
    padding = (width - len(title) - 2) // 2
    print(f"{char}{' ' * padding}{title}{' ' * (width - padding - len(title) - 2)}{char}")
    print(char * width)
    print()

def print_section(title: str) -> None:
    """打印小节标题"""
    print(f"\n{'─' * 40}")
    print(f"▶ {title}")
    print(f"{'─' * 40}\n")

def print_step(step_num: int, description: str) -> None:
    """打印步骤"""
    print(f"  【步骤 {step_num}】{description}")

# ============================================================================
#                         第一部分：HDFS 模拟层
# ============================================================================

print_banner("HDFS 模拟层代码", "=")

@dataclass(frozen=True)
class BlockMetadata:
    """
    Block 元数据
    
    【教学说明】
    - block_id: 全局唯一的 Block 标识符
    - size: Block 的实际大小（字节）
    
    在真实 HDFS 中，NameNode 会维护这些元数据，
    包括 Block 所在的 DataNode 列表、创建时间等。
    """
    block_id: str
    size: int


class NameNode:
    """
    NameNode 模拟实现
    
    【核心职责】
    1. 维护文件系统的目录树（文件名 → Block 列表）
    2. 维护 Block 位置信息（Block ID → DataNode 列表）
    3. 不存储任何实际数据！
    
    【真实环境对比】
    - 真实 NameNode 还负责：
      - 处理客户端的读写请求
      - 管理 DataNode 的心跳和状态
      - 处理 Block 的副本管理和再平衡
    """

    def __init__(self) -> None:
        # 文件到 Block 列表的映射
        # 例如: {"/data/logs.txt": [BlockMetadata("blk_001", 128MB), ...]}
        self._file_to_blocks: Dict[str, List[BlockMetadata]] = {}
        
        # Block 到 DataNode 列表的映射
        # 例如: {"blk_001": ["dn-1", "dn-2", "dn-3"]}（3副本）
        self._block_locations: Dict[str, List[str]] = {}

    def register_file(self, file_path: str, blocks: List[BlockMetadata]) -> None:
        """注册文件的 Block 元数据"""
        self._file_to_blocks[file_path] = blocks
        print(f"    NameNode: 注册文件 {file_path}，包含 {len(blocks)} 个 Block")

    def register_block_locations(self, block_id: str, datanode_ids: List[str]) -> None:
        """注册 Block 的存储位置"""
        self._block_locations[block_id] = datanode_ids
        print(f"    NameNode: Block {block_id} 存储在 {datanode_ids}")

    def get_file_blocks(self, file_path: str) -> List[BlockMetadata]:
        """获取文件的 Block 列表"""
        if file_path not in self._file_to_blocks:
            raise FileNotFoundError(f"文件不存在: {file_path}")
        return self._file_to_blocks[file_path]

    def get_block_locations(self, block_id: str) -> List[str]:
        """获取 Block 的存储位置（DataNode 列表）"""
        if block_id not in self._block_locations:
            raise KeyError(f"Block 不存在: {block_id}")
        return self._block_locations[block_id]


class DataNode:
    """
    DataNode 模拟实现
    
    【核心职责】
    1. 存储实际的数据块（Block）
    2. 提供 Block 的读写服务
    3. 模拟本地缓存（提高重复读取性能）
    
    【教学说明】
    - disk: 模拟磁盘存储
    - cache: 模拟内存缓存（LRU 策略）
    - disk_latency: 模拟磁盘 I/O 延迟
    """

    def __init__(self, node_id: str, cache_capacity: int = 8, 
                 disk_latency: float = 0.02) -> None:
        self.node_id = node_id
        self._disk: Dict[str, bytes] = {}       # 磁盘存储
        self._cache: "OrderedDict[str, bytes]" = OrderedDict()  # LRU 缓存
        self.cache_capacity = cache_capacity
        self.disk_latency = disk_latency
        self.cache_hits = 0
        self.cache_misses = 0

    def store_block(self, block_id: str, data: bytes) -> None:
        """存储 Block 到磁盘"""
        self._disk[block_id] = data
        print(f"    DataNode[{self.node_id}]: 存储 Block {block_id} ({len(data)} bytes)")

    def read_block(self, block_id: str) -> bytes:
        """
        读取 Block
        
        【教学说明】
        1. 先检查缓存，命中则直接返回（无延迟）
        2. 缓存未命中，从磁盘读取（有延迟）
        3. 读取后放入缓存，可能触发 LRU 淘汰
        """
        # 检查缓存
        if block_id in self._cache:
            self.cache_hits += 1
            self._cache.move_to_end(block_id)  # LRU: 移到末尾表示最近使用
            return self._cache[block_id]

        # 缓存未命中，从磁盘读取
        if block_id not in self._disk:
            raise KeyError(f"Block {block_id} 不存在于 DataNode {self.node_id}")

        self.cache_misses += 1
        time.sleep(self.disk_latency)  # 模拟磁盘 I/O 延迟
        data = self._disk[block_id]
        
        # 放入缓存
        self._cache[block_id] = data
        if len(self._cache) > self.cache_capacity:
            self._cache.popitem(last=False)  # LRU 淘汰最久未使用的
            
        return data


class HDFSCluster:
    """
    HDFS 集群模拟实现
    
    【核心功能】
    1. put(): 上传文件到 HDFS（切分 Block + 多副本存储）
    2. get(): 从 HDFS 下载文件
    3. get_blocks(): 获取文件的所有 Block（用于 MapReduce）
    """

    def __init__(self, datanodes: List[DataNode], 
                 block_size: int = 128 * 1024 * 1024,  # 默认 128MB
                 replication: int = 3) -> None:
        if replication > len(datanodes):
            raise ValueError("副本数不能超过 DataNode 数量")
        
        self.namenode = NameNode()
        self.datanodes = {node.node_id: node for node in datanodes}
        self.block_size = block_size
        self.replication = replication
        
        print(f"HDFS 集群初始化完成:")
        print(f"  - DataNode 数量: {len(datanodes)}")
        print(f"  - Block 大小: {block_size} bytes")
        print(f"  - 副本因子: {replication}")

    def _split_into_blocks(self, data: bytes) -> List[bytes]:
        """
        将数据切分成 Block
        
        【教学说明】
        这里按字节切分，不考虑行边界。
        真实环境中，TextInputFormat 会在 Map 阶段处理跨 Block 的行。
        """
        blocks = [data[i:i + self.block_size] 
                  for i in range(0, len(data), self.block_size)]
        return blocks

    def put(self, file_path: str) -> None:
        """
        上传文件到 HDFS
        
        【执行步骤】
        1. 读取本地文件
        2. 切分成 Block
        3. 为每个 Block 选择存储节点（考虑副本）
        4. 存储到 DataNode
        5. 注册元数据到 NameNode
        """
        print_section(f"上传文件到 HDFS: {file_path}")
        
        # 读取本地文件
        with open(file_path, "rb") as f:
            data = f.read()
        print(f"  文件大小: {len(data)} bytes")

        # 切分成 Block
        blocks = self._split_into_blocks(data)
        print(f"  切分成 {len(blocks)} 个 Block (block_size={self.block_size})")

        # 存储每个 Block
        metadata: List[BlockMetadata] = []
        for index, block_data in enumerate(blocks):
            block_id = f"{os.path.basename(file_path)}_block_{index}"
            metadata.append(BlockMetadata(block_id=block_id, size=len(block_data)))
            
            # 选择存储节点（随机选择 replication 个）
            chosen_nodes = random.sample(list(self.datanodes.values()), self.replication)
            
            # 存储到选中的 DataNode
            print(f"\n  Block {index} ({len(block_data)} bytes):")
            for node in chosen_nodes:
                node.store_block(block_id, block_data)
            
            # 注册位置到 NameNode
            self.namenode.register_block_locations(
                block_id, 
                [node.node_id for node in chosen_nodes]
            )

        # 注册文件元数据
        self.namenode.register_file(file_path, metadata)
        print(f"\n  ✓ 文件上传完成！")

    def get(self, file_path: str) -> Tuple[bytes, float]:
        """从 HDFS 下载文件"""
        blocks = self.namenode.get_file_blocks(file_path)
        retrieved: List[bytes] = []
        start_time = time.perf_counter()
        
        for block in blocks:
            datanode_ids = self.namenode.get_block_locations(block.block_id)
            # 随机选择一个副本读取
            chosen_id = random.choice(datanode_ids)
            data = self.datanodes[chosen_id].read_block(block.block_id)
            retrieved.append(data)
            
        elapsed = time.perf_counter() - start_time
        return b"".join(retrieved), elapsed

    def get_blocks(self, file_path: str) -> List[Tuple[str, bytes]]:
        """
        获取文件的所有 Block（用于 MapReduce）
        
        【教学说明】
        MapReduce 框架需要直接访问各个 Block，
        这个方法返回每个 Block 的 ID 和数据内容。
        """
        blocks = self.namenode.get_file_blocks(file_path)
        retrieved: List[Tuple[str, bytes]] = []
        
        for block in blocks:
            datanode_ids = self.namenode.get_block_locations(block.block_id)
            chosen_id = random.choice(datanode_ids)
            data = self.datanodes[chosen_id].read_block(block.block_id)
            retrieved.append((block.block_id, data))
            
        return retrieved


# ============================================================================
#                       第二部分：MapReduce 模拟层
# ============================================================================

print_banner("MapReduce 模拟层代码", "=")

# 类型定义
KeyValue = Tuple[str, str]
Mapper = Callable[[str, bytes], Iterable[KeyValue]]
Reducer = Callable[[str, Sequence[str]], Iterable[Tuple[str, object]]]


def default_partitioner(key: str, num_reducers: int) -> int:
    """
    默认分区函数
    
    【教学说明】
    - 使用 key 的哈希值对 Reducer 数量取模
    - 保证相同的 key 一定会发送到同一个 Reducer
    - 这是 MapReduce 正确性的基础！
    
    【潜在问题】
    - 如果某些 key 特别多，会导致数据倾斜（Data Skew）
    - 解决方案：自定义 Partitioner 或预聚合
    """
    return hash(key) % num_reducers


class MapReduceEngine:
    """
    MapReduce 执行引擎
    
    【核心组件】
    1. Map 阶段：并行处理各个 Block，输出 (key, value) 对
    2. Shuffle 阶段：分区、排序、网络传输、分组
    3. Reduce 阶段：聚合相同 key 的所有 values
    """

    def __init__(self, hdfs: HDFSCluster, num_reducers: int = 2,
                 network_latency_per_kb: float = 0.001) -> None:
        self.hdfs = hdfs
        self.num_reducers = num_reducers
        self.network_latency_per_kb = network_latency_per_kb
        
        print(f"MapReduce 引擎初始化:")
        print(f"  - Reducer 数量: {num_reducers}")
        print(f"  - 网络延迟: {network_latency_per_kb} 秒/KB")

    def submit_job(self, mapper: Mapper, reducer: Reducer,
                   input_path: str) -> Dict[str, List[Tuple[str, object]]]:
        """
        提交 MapReduce 作业
        
        【完整执行流程】
        """
        print_banner("MapReduce 作业执行", "-")
        
        # ==================== MAP 阶段 ====================
        print_section("MAP 阶段")
        print("  【目标】将输入数据转换为 (key, value) 键值对")
        print("  【并行性】每个 Block 对应一个 Map Task，可并行执行\n")
        
        # 从 HDFS 获取所有 Block
        blocks = self.hdfs.get_blocks(input_path)
        print(f"  从 HDFS 获取到 {len(blocks)} 个 Block")
        
        # 存储 Map 输出的分区数据
        partitions: Dict[int, List[KeyValue]] = defaultdict(list)
        
        # 执行每个 Map Task
        total_map_outputs = 0
        for block_id, data in blocks:
            print(f"\n  ▶ Map Task [{block_id}]")
            print(f"    输入大小: {len(data)} bytes")
            
            map_output_count = 0
            for key, value in mapper(block_id, data):
                # 分区：决定这条记录发送到哪个 Reducer
                partition_id = default_partitioner(key, self.num_reducers)
                partitions[partition_id].append((key, value))
                map_output_count += 1
                
            print(f"    输出记录数: {map_output_count}")
            total_map_outputs += map_output_count
            
        print(f"\n  ✓ MAP 阶段完成！总输出: {total_map_outputs} 条记录")
        
        # 显示分区统计
        print("\n  分区统计:")
        for pid in range(self.num_reducers):
            count = len(partitions.get(pid, []))
            print(f"    Partition {pid}: {count} 条记录")
        
        # ==================== SHUFFLE 阶段 ====================
        print_section("SHUFFLE 阶段")
        print("  【目标】将 Map 输出按 key 重新组织，发送到对应的 Reducer")
        print("  【步骤】分区 → 排序 → 网络传输 → 分组\n")
        
        reducer_inputs: Dict[int, Dict[str, List[str]]] = {}
        
        for partition_id in range(self.num_reducers):
            pairs = partitions.get(partition_id, [])
            print(f"  ▶ 处理 Partition {partition_id} ({len(pairs)} 条记录)")
            
            # 模拟网络传输
            print(f"    1. 模拟网络传输...")
            self._simulate_network_transfer(pairs)
            
            # 排序
            print(f"    2. 按 Key 排序...")
            sorted_pairs = sorted(pairs, key=lambda item: item[0])
            
            # 分组：相同 key 的 values 聚合在一起
            print(f"    3. 按 Key 分组...")
            grouped: Dict[str, List[str]] = defaultdict(list)
            for key, value in sorted_pairs:
                grouped[key].append(value)
                
            print(f"    → 分组后有 {len(grouped)} 个不同的 Key")
            reducer_inputs[partition_id] = grouped
            
        print(f"\n  ✓ SHUFFLE 阶段完成！")
        
        # ==================== REDUCE 阶段 ====================
        print_section("REDUCE 阶段")
        print("  【目标】对相同 key 的所有 values 进行聚合计算")
        print("  【保证】相同 key 一定在同一个 Reducer 中处理\n")
        
        outputs: Dict[str, List[Tuple[str, object]]] = {}
        
        for partition_id, grouped in reducer_inputs.items():
            print(f"  ▶ Reduce Task {partition_id}")
            print(f"    处理 {len(grouped)} 个 Key")
            
            reduced: List[Tuple[str, object]] = []
            for key, values in grouped.items():
                # 调用用户定义的 Reducer 函数
                for output in reducer(key, values):
                    reduced.append(output)
                    
            outputs[f"reducer-{partition_id}"] = reduced
            print(f"    输出 {len(reduced)} 条结果")
            
        print(f"\n  ✓ REDUCE 阶段完成！")
        
        return outputs

    def _simulate_network_transfer(self, pairs: List[KeyValue]) -> None:
        """模拟 Shuffle 阶段的网络传输延迟"""
        if not pairs:
            return
        payload_bytes = sum(
            len(key.encode("utf-8")) + len(value.encode("utf-8")) 
            for key, value in pairs
        )
        payload_kb = payload_bytes / 1024
        delay = payload_kb * self.network_latency_per_kb
        time.sleep(delay)


# ============================================================================
#                      第三部分：YARN 模拟层
# ============================================================================

print_banner("YARN 模拟层代码", "=")

@dataclass
class Container:
    """
    YARN Container
    
    【教学说明】
    Container 是 YARN 资源分配的最小单位：
    - 每个 Container 包含固定的 CPU 和内存
    - Map/Reduce Task 在 Container 中运行
    - 任务结束后，Container 释放
    """
    container_id: str
    node_id: str
    cores: int
    memory_mb: int
    task_id: str


@dataclass
class Task:
    """MapReduce 任务"""
    task_id: str
    duration_s: float
    remaining_s: float
    task_type: str  # "map" or "reduce"


@dataclass
class JobSpec:
    """作业规格"""
    name: str
    input_path: str
    map_tasks: List[Task]
    reduce_tasks: List[Task]


class NodeManager:
    """
    NodeManager 模拟实现
    
    【核心职责】
    1. 管理本节点的资源（CPU、内存）
    2. 启动和监控 Container
    3. 向 ResourceManager 汇报心跳
    """

    def __init__(self, node_id: str, total_cores: int, total_memory_mb: int) -> None:
        self.node_id = node_id
        self.total_cores = total_cores
        self.total_memory_mb = total_memory_mb
        self.available_cores = total_cores
        self.available_memory_mb = total_memory_mb
        self.running: Dict[str, Container] = {}  # 正在运行的 Container

    def can_allocate(self, cores: int, memory_mb: int) -> bool:
        """检查是否有足够资源"""
        return self.available_cores >= cores and self.available_memory_mb >= memory_mb

    def allocate(self, container: Container) -> None:
        """分配资源并启动 Container"""
        if not self.can_allocate(container.cores, container.memory_mb):
            raise ValueError(f"NodeManager[{self.node_id}] 资源不足")
        self.available_cores -= container.cores
        self.available_memory_mb -= container.memory_mb
        self.running[container.container_id] = container

    def release(self, container_id: str) -> None:
        """释放 Container"""
        if container_id not in self.running:
            return
        container = self.running.pop(container_id)
        self.available_cores += container.cores
        self.available_memory_mb += container.memory_mb

    def heartbeat(self) -> Dict[str, int]:
        """心跳：报告当前资源状态"""
        return {
            "node_id": self.node_id,
            "available_cores": self.available_cores,
            "available_memory_mb": self.available_memory_mb,
            "running_containers": len(self.running),
        }


class ResourceManager:
    """
    ResourceManager 模拟实现
    
    【核心职责】
    1. 集群资源的全局视图和调度
    2. 管理 Application 生命周期
    3. 分配 Container 给 ApplicationMaster
    """

    def __init__(self, scheduler: str = "fifo") -> None:
        if scheduler not in {"fifo", "fair"}:
            raise ValueError("调度策略必须是 'fifo' 或 'fair'")
        self.scheduler = scheduler
        self.nodes: Dict[str, NodeManager] = {}
        self.queue: deque["ApplicationMaster"] = deque()
        
        print(f"ResourceManager 初始化，调度策略: {scheduler}")

    def register_node(self, node: NodeManager) -> None:
        """注册 NodeManager"""
        self.nodes[node.node_id] = node
        print(f"  注册 NodeManager: {node.node_id} "
              f"({node.total_cores} cores, {node.total_memory_mb}MB)")

    def submit_application(self, app_master: "ApplicationMaster") -> None:
        """提交 Application"""
        self.queue.append(app_master)
        print(f"  提交 Application: {app_master.app_id} ({app_master.job.name})")

    def schedule(self) -> None:
        """执行调度"""
        if not self.queue:
            return
        if self.scheduler == "fifo":
            self._schedule_fifo()
        else:
            self._schedule_fair()

    def _schedule_fifo(self) -> None:
        """
        FIFO 调度（先来先服务）
        
        【特点】
        - 简单直接
        - 先提交的作业先获得资源
        - 缺点：大作业会阻塞小作业
        """
        current = self.queue[0]
        if current.is_complete():
            self.queue.popleft()
            return
        self._allocate_for_app(current)
        if current.is_complete():
            self.queue.popleft()

    def _schedule_fair(self) -> None:
        """
        Fair 调度（公平调度）
        
        【特点】
        - 所有作业轮流获得资源
        - 小作业能更快完成
        - 大作业执行时间可能变长
        """
        for _ in range(len(self.queue)):
            app = self.queue.popleft()
            if not app.is_complete():
                self._allocate_for_app(app)
                self.queue.append(app)

    def _allocate_for_app(self, app: "ApplicationMaster") -> None:
        """为应用分配资源"""
        while app.has_pending_tasks():
            allocation = self._find_allocation()
            if allocation is None:
                break
            node_id, cores, memory_mb = allocation
            container = app.request_container(node_id, cores, memory_mb)
            self.nodes[node_id].allocate(container)

    def _find_allocation(self) -> Optional[Tuple[str, int, int]]:
        """查找可用的资源"""
        for node in self.nodes.values():
            if node.can_allocate(1, 256):
                return node.node_id, 1, 256
        return None


class ApplicationMaster:
    """
    ApplicationMaster 模拟实现
    
    【核心职责】
    1. 管理单个作业的生命周期
    2. 向 ResourceManager 申请资源
    3. 跟踪任务进度
    """

    def __init__(self, job: JobSpec, engine: MapReduceEngine) -> None:
        self.job = job
        self.engine = engine
        self.app_id = f"app-{uuid.uuid4().hex[:8]}"
        self.pending_maps: deque[Task] = deque(job.map_tasks)
        self.pending_reduces: deque[Task] = deque(job.reduce_tasks)
        self.running_tasks: Dict[str, Task] = {}
        self.completed_tasks: List[str] = []
        self.output: Optional[Dict[str, List[tuple]]] = None

    def has_pending_tasks(self) -> bool:
        """是否有待执行的任务"""
        return bool(self.pending_maps or 
                   (not self.pending_maps and self.pending_reduces))

    def request_container(self, node_id: str, cores: int, 
                          memory_mb: int) -> Container:
        """申请 Container"""
        if self.pending_maps:
            task = self.pending_maps.popleft()
        else:
            task = self.pending_reduces.popleft()
        self.running_tasks[task.task_id] = task
        container_id = f"container-{uuid.uuid4().hex[:8]}"
        return Container(
            container_id=container_id,
            node_id=node_id,
            cores=cores,
            memory_mb=memory_mb,
            task_id=task.task_id
        )

    def tick(self, delta_s: float, node_managers: Dict[str, NodeManager]) -> None:
        """模拟时间流逝，更新任务状态"""
        finished: List[str] = []
        for task_id, task in self.running_tasks.items():
            task.remaining_s -= delta_s
            if task.remaining_s <= 0:
                finished.append(task_id)
                
        for task_id in finished:
            self.running_tasks.pop(task_id)
            self.completed_tasks.append(task_id)
            # 释放 Container
            for node in node_managers.values():
                for container in list(node.running.values()):
                    if container.task_id == task_id:
                        node.release(container.container_id)

    def is_complete(self) -> bool:
        """作业是否完成"""
        return not (self.pending_maps or self.pending_reduces or self.running_tasks)

    def progress(self) -> float:
        """计算完成进度"""
        total = len(self.job.map_tasks) + len(self.job.reduce_tasks)
        if total == 0:
            return 1.0
        return len(self.completed_tasks) / total


class YarnCluster:
    """YARN 集群"""

    def __init__(self, resource_manager: ResourceManager, 
                 nodes: List[NodeManager]) -> None:
        self.rm = resource_manager
        self.nodes = nodes
        for node in nodes:
            self.rm.register_node(node)
        self.applications: List[ApplicationMaster] = []

    def submit_job(self, app_master: ApplicationMaster) -> None:
        """提交作业"""
        self.applications.append(app_master)
        self.rm.submit_application(app_master)

    def status_report(self) -> List[str]:
        """生成状态报告"""
        reports = []
        for app in self.applications:
            status = "完成" if app.is_complete() else f"进度 {app.progress():.0%}"
            reports.append(f"{app.job.name}: {status}")
        return reports


# ============================================================================
#                      第四部分：业务逻辑实现
# ============================================================================

print_banner("业务逻辑: PV/UV 统计", "=")

# 日志格式: 时间戳 | 用户ID | URL | 来源 | 停留时长
LOG_PATTERN = re.compile(
    r"^(?P<timestamp>[\d\-\s:]+)\s*\|\s*"
    r"(?P<user_id>\S+)\s*\|\s*"
    r"(?P<url>\S+)\s*\|\s*"
    r"(?P<source>\S+)\s*\|\s*"
    r"(?P<duration>\d+)$"
)


def pv_uv_mapper(block_id: str, data: bytes) -> Iterable[KeyValue]:
    """
    Mapper: 从日志行提取 (URL, UserID)
    
    【输入】
    - block_id: Block 标识
    - data: Block 的原始字节数据
    
    【输出】
    - 生成器，产生 (url, user_id) 键值对
    
    【教学说明】
    每条输出记录代表"某用户访问了某页面一次"
    - Key (URL): 用于按页面分组
    - Value (UserID): 用于计算 UV（去重）
    """
    text = data.decode("utf-8")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        match = LOG_PATTERN.match(line)
        if not match:
            continue
        yield match.group("url"), match.group("user_id")


def pv_uv_reducer(url: str, user_ids: Sequence[str]) -> Iterable[Tuple[str, Dict[str, int]]]:
    """
    Reducer: 计算每个 URL 的 PV 和 UV
    
    【输入】
    - url: 商品页面 URL
    - user_ids: 所有访问该 URL 的用户 ID 列表
    
    【输出】
    - (url, {"pv": 访问次数, "uv": 独立访客数})
    
    【教学说明】
    - PV = len(user_ids)，直接统计列表长度
    - UV = len(set(user_ids))，去重后的用户数
    
    Shuffle 保证了同一 URL 的所有记录在同一个 Reducer，
    所以这里的 set() 去重是全局正确的。
    """
    pv = len(user_ids)
    uv = len(set(user_ids))
    yield url, {"pv": pv, "uv": uv}


# ============================================================================
#                      第五部分：完整演示
# ============================================================================

def generate_sample_logs(filepath: str, num_lines: int = 200) -> None:
    """生成示例日志数据"""
    print_section(f"生成示例数据: {filepath}")
    
    urls = [
        "/product/phone_iphone15",
        "/product/laptop_macbook",
        "/product/tablet_ipad",
        "/product/watch_applewatch",
        "/product/earbuds_airpods",
    ]
    users = [f"user_{i:03d}" for i in range(1, 51)]  # 50个用户
    sources = ["search", "direct", "ad", "social", "email"]
    
    with open(filepath, "w", encoding="utf-8") as f:
        for i in range(num_lines):
            timestamp = f"2024-01-20 {10 + i // 60:02d}:{i % 60:02d}:00"
            user = random.choice(users)
            url = random.choice(urls)
            source = random.choice(sources)
            duration = random.randint(10, 300)
            f.write(f"{timestamp} | {user} | {url} | {source} | {duration}\n")
    
    print(f"  生成了 {num_lines} 条日志记录")
    print(f"  商品页面: {len(urls)} 个")
    print(f"  用户数: {len(users)} 个")


def run_complete_demo():
    """运行完整演示"""
    
    print_banner("Hadoop 端到端学习案例演示", "█")
    print("""
    案例背景：电商平台访问日志分析
    业务目标：计算每个商品页面的 PV（页面访问量）和 UV（独立访客数）
    
    本演示将完整展示：
    1. HDFS：数据如何被切分和存储
    2. MapReduce：Map/Shuffle/Reduce 如何协作
    3. YARN：资源如何被调度和管理
    """)
    
    # ========== 准备阶段 ==========
    print_banner("准备阶段", "━")
    
    # 生成示例数据
    log_file = "access_logs_demo.txt"
    generate_sample_logs(log_file, num_lines=300)
    
    # ========== HDFS 阶段 ==========
    print_banner("第一部分: HDFS - 分布式存储", "━")
    
    # 创建 HDFS 集群
    datanodes = [
        DataNode("dn-1", cache_capacity=4, disk_latency=0.01),
        DataNode("dn-2", cache_capacity=4, disk_latency=0.01),
        DataNode("dn-3", cache_capacity=4, disk_latency=0.01),
    ]
    hdfs = HDFSCluster(datanodes, block_size=2048, replication=2)  # 2KB block, 2副本
    
    # 上传文件
    hdfs.put(log_file)
    
    # ========== MapReduce 阶段 ==========
    print_banner("第二部分: MapReduce - 分布式计算", "━")
    
    # 创建 MapReduce 引擎
    mr_engine = MapReduceEngine(hdfs, num_reducers=2, network_latency_per_kb=0.0005)
    
    # 提交作业
    results = mr_engine.submit_job(pv_uv_mapper, pv_uv_reducer, log_file)
    
    # ========== YARN 调度模拟 ==========
    print_banner("第三部分: YARN - 资源调度（模拟）", "━")
    
    print("""
    【说明】以下模拟 YARN 的调度过程
    在真实环境中，YARN 会：
    1. ResourceManager 接收作业请求
    2. 为作业启动 ApplicationMaster
    3. AM 向 RM 申请 Container
    4. NodeManager 在 Container 中运行任务
    """)
    
    # 创建 YARN 集群
    rm = ResourceManager(scheduler="fair")
    node_managers = [
        NodeManager("nm-1", total_cores=4, total_memory_mb=4096),
        NodeManager("nm-2", total_cores=4, total_memory_mb=4096),
    ]
    yarn = YarnCluster(rm, node_managers)
    
    # 创建模拟作业
    map_count = 3
    job = JobSpec(
        name="PV/UV 统计作业",
        input_path=log_file,
        map_tasks=[Task(f"map-{i}", 0.3, 0.3, "map") for i in range(map_count)],
        reduce_tasks=[Task(f"reduce-{i}", 0.4, 0.4, "reduce") for i in range(2)]
    )
    
    # 提交作业到 YARN
    print_section("提交作业到 YARN")
    am = ApplicationMaster(job, mr_engine)
    yarn.submit_job(am)
    
    # 模拟调度过程
    print_section("模拟调度过程")
    tick = 0
    while not all(app.is_complete() for app in yarn.applications):
        yarn.rm.schedule()
        for app in yarn.applications:
            app.tick(0.2, yarn.rm.nodes)
        tick += 1
        if tick % 5 == 0:
            print(f"  Tick {tick}: " + " | ".join(yarn.status_report()))
    
    print(f"\n  ✓ 所有作业完成！共 {tick} 个调度周期")
    
    # ========== 结果展示 ==========
    print_banner("第四部分: 结果展示", "━")
    
    print_section("MapReduce 输出结果")
    
    # 合并所有 Reducer 的输出
    all_results = []
    for reducer_id, outputs in sorted(results.items()):
        print(f"\n  {reducer_id}:")
        for url, stats in sorted(outputs):
            print(f"    {url}")
            print(f"      PV (页面访问量): {stats['pv']}")
            print(f"      UV (独立访客数): {stats['uv']}")
            all_results.append((url, stats))
    
    # 汇总统计
    print_section("汇总统计")
    total_pv = sum(s['pv'] for _, s in all_results)
    total_uv = sum(s['uv'] for _, s in all_results)
    print(f"  总页面访问量 (Total PV): {total_pv}")
    print(f"  总独立访客数 (Total UV): {total_uv}")
    print(f"  平均每用户访问次数: {total_pv / total_uv:.2f}")
    
    # ========== 学习总结 ==========
    print_banner("学习总结", "█")
    
    print("""
    通过这个案例，你应该理解了：

    【HDFS 层面】
    ✓ 文件如何被切分成 Block
    ✓ Block 如何分布存储到多个 DataNode
    ✓ 副本策略如何保证数据可靠性
    
    【MapReduce 层面】
    ✓ Map 阶段：每个 Block 对应一个 Map Task
    ✓ Shuffle 阶段：分区→排序→传输→分组
    ✓ Reduce 阶段：相同 Key 的 Values 被聚合处理
    ✓ 为什么 UV 去重能正确工作（Shuffle 保证）
    
    【YARN 层面】
    ✓ ResourceManager：全局资源调度
    ✓ NodeManager：节点资源管理
    ✓ ApplicationMaster：作业生命周期管理
    ✓ Container：资源分配的最小单位
    
    【关键概念映射】
    ┌─────────────┬──────────────────────────────────┐
    │ 概念        │ 在本案例中的体现                  │
    ├─────────────┼──────────────────────────────────┤
    │ Block       │ 日志文件被切成多个 2KB 块        │
    │ InputSplit  │ 每个 Block 对应一个 Map Task     │
    │ Partitioner │ hash(url) 决定去哪个 Reducer     │
    │ Combiner    │ （本例未使用，可在 Map 端预聚合）│
    │ 数据本地性   │ Map Task 优先在数据所在节点运行  │
    └─────────────┴──────────────────────────────────┘
    
    后续学习建议：
    1. 尝试修改 Reducer 数量，观察输出文件变化
    2. 添加 Combiner 减少 Shuffle 数据量
    3. 研究数据倾斜问题和解决方案
    4. 学习 Spark/Flink 如何优化这些流程
    """)
    
    # 清理
    if os.path.exists(log_file):
        os.remove(log_file)
    
    print("\n演示完成！\n")


if __name__ == "__main__":
    run_complete_demo()
