"""
MiniSpark RDD 实现
==================
实现 RDD 的核心抽象，包括 transformations 和 actions。
"""

import os
from dataclasses import dataclass, field
from typing import (
    TypeVar, Generic, Callable, List, Optional, Dict, Any, 
    Tuple, Iterator, Union
)
from enum import Enum
import threading

from .logger import get_logger, EventType


T = TypeVar('T')
U = TypeVar('U')
K = TypeVar('K')
V = TypeVar('V')


class DependencyType(Enum):
    """依赖类型"""
    NARROW = "narrow"   # 窄依赖：一对一映射
    WIDE = "wide"       # 宽依赖：需要 shuffle


@dataclass
class Dependency:
    """RDD 依赖关系"""
    parent_rdd: 'RDD'
    dependency_type: DependencyType
    # 对于 shuffle 依赖，记录 partitioner 信息
    partitioner: Optional[Callable] = None
    shuffle_id: Optional[int] = None


class RDD(Generic[T]):
    """
    RDD (Resilient Distributed Dataset) - 弹性分布式数据集
    
    RDD 是 Spark 的核心抽象，表示一个不可变的、分区的数据集合。
    
    关键特性：
    1. 不可变性：RDD 一旦创建就不能修改
    2. 分区：数据被划分为多个分区，支持并行计算
    3. 惰性求值：Transformation 不会立即执行，直到遇到 Action
    4. 血统（Lineage）：记录数据的来源，支持容错重算
    """
    
    _id_counter = 0
    _id_lock = threading.Lock()
    
    @classmethod
    def _next_id(cls) -> int:
        with cls._id_lock:
            cls._id_counter += 1
            return cls._id_counter
    
    def __init__(
        self,
        context: 'SparkContext',
        name: str = "",
        num_partitions: int = 4,
        dependencies: List[Dependency] = None,
        compute_func: Callable[[int], Iterator[T]] = None
    ):
        self.id = self._next_id()
        self.context = context
        self.name = name or f"RDD-{self.id}"
        self.num_partitions = num_partitions
        self.dependencies = dependencies or []
        self._compute_func = compute_func
        
        # 缓存相关
        self._cached = False
        self._cache_blocks: Dict[int, List[T]] = {}
        self._cache_lock = threading.Lock()
        
        # Shuffle 相关
        self._shuffle_id: Optional[int] = None
        
    @property
    def parents(self) -> List['RDD']:
        """获取父 RDD 列表"""
        return [dep.parent_rdd for dep in self.dependencies]
    
    @property
    def dependency_type(self) -> Optional[DependencyType]:
        """获取主要依赖类型（用于显示）"""
        if not self.dependencies:
            return None
        # 如果有任何宽依赖，返回 WIDE
        for dep in self.dependencies:
            if dep.dependency_type == DependencyType.WIDE:
                return DependencyType.WIDE
        return DependencyType.NARROW
    
    def compute(self, partition_id: int) -> Iterator[T]:
        """
        计算指定分区的数据
        
        这是 RDD 的核心方法，由子类或 compute_func 实现具体计算逻辑。
        """
        if self._compute_func:
            return self._compute_func(partition_id)
        raise NotImplementedError("RDD must implement compute()")
    
    def get_cached(self, partition_id: int) -> Optional[List[T]]:
        """获取缓存的分区数据"""
        with self._cache_lock:
            return self._cache_blocks.get(partition_id)
    
    def put_cache(self, partition_id: int, data: List[T]):
        """缓存分区数据"""
        with self._cache_lock:
            self._cache_blocks[partition_id] = data
            get_logger().log(
                EventType.CACHE_PUT,
                rdd_id=self.id,
                partition_id=partition_id,
                output_records=len(data),
                note=f"缓存 RDD[{self.id}] 分区 {partition_id}, {len(data)} 条记录"
            )
    
    # ==================== Transformations ====================
    # Transformations 是惰性的，只构建 DAG，不触发计算
    
    def map(self, f: Callable[[T], U]) -> 'RDD[U]':
        """
        对每个元素应用函数 f
        
        这是窄依赖：父 RDD 的每个分区只被一个子 RDD 分区依赖。
        """
        parent = self
        
        def compute_func(partition_id: int) -> Iterator[U]:
            for item in parent._compute_partition(partition_id):
                yield f(item)
        
        return RDD(
            context=self.context,
            name=f"map({self.name})",
            num_partitions=self.num_partitions,
            dependencies=[Dependency(self, DependencyType.NARROW)],
            compute_func=compute_func
        )
    
    def filter(self, f: Callable[[T], bool]) -> 'RDD[T]':
        """
        过滤元素，保留满足条件的元素
        
        窄依赖。
        """
        parent = self
        
        def compute_func(partition_id: int) -> Iterator[T]:
            for item in parent._compute_partition(partition_id):
                if f(item):
                    yield item
        
        return RDD(
            context=self.context,
            name=f"filter({self.name})",
            num_partitions=self.num_partitions,
            dependencies=[Dependency(self, DependencyType.NARROW)],
            compute_func=compute_func
        )
    
    def flatMap(self, f: Callable[[T], Iterator[U]]) -> 'RDD[U]':
        """
        对每个元素应用函数 f，f 返回一个迭代器，然后展平结果
        
        窄依赖。
        """
        parent = self
        
        def compute_func(partition_id: int) -> Iterator[U]:
            for item in parent._compute_partition(partition_id):
                for result in f(item):
                    yield result
        
        return RDD(
            context=self.context,
            name=f"flatMap({self.name})",
            num_partitions=self.num_partitions,
            dependencies=[Dependency(self, DependencyType.NARROW)],
            compute_func=compute_func
        )
    
    def mapPartitions(self, f: Callable[[Iterator[T]], Iterator[U]]) -> 'RDD[U]':
        """
        对每个分区应用函数 f
        
        窄依赖。比 map 更高效，因为可以在分区级别批量处理。
        """
        parent = self
        
        def compute_func(partition_id: int) -> Iterator[U]:
            partition_iter = parent._compute_partition(partition_id)
            return f(partition_iter)
        
        return RDD(
            context=self.context,
            name=f"mapPartitions({self.name})",
            num_partitions=self.num_partitions,
            dependencies=[Dependency(self, DependencyType.NARROW)],
            compute_func=compute_func
        )
    
    def union(self, other: 'RDD[T]') -> 'RDD[T]':
        """
        合并两个 RDD
        
        窄依赖（每个父 RDD 的分区独立映射到子 RDD 的分区）。
        """
        parent1 = self
        parent2 = other
        total_partitions = self.num_partitions + other.num_partitions
        
        def compute_func(partition_id: int) -> Iterator[T]:
            if partition_id < parent1.num_partitions:
                return parent1._compute_partition(partition_id)
            else:
                return parent2._compute_partition(partition_id - parent1.num_partitions)
        
        return RDD(
            context=self.context,
            name=f"union({self.name}, {other.name})",
            num_partitions=total_partitions,
            dependencies=[
                Dependency(self, DependencyType.NARROW),
                Dependency(other, DependencyType.NARROW)
            ],
            compute_func=compute_func
        )
    
    def keyBy(self, f: Callable[[T], K]) -> 'RDD[Tuple[K, T]]':
        """
        为每个元素生成 key，转换为 (key, value) 对
        
        窄依赖。
        """
        return self.map(lambda x: (f(x), x))
    
    def reduceByKey(
        self, 
        f: Callable[[V, V], V], 
        num_partitions: int = None
    ) -> 'RDD[Tuple[K, V]]':
        """
        按 key 聚合 value
        
        这是宽依赖！需要 shuffle：相同 key 的数据可能分布在不同分区，
        需要重新分区使相同 key 的数据聚集到同一分区。
        
        执行流程：
        1. Map 端：在每个分区内先做本地 reduce（combiner）
        2. Shuffle：按 key 的 hash 值重新分区
        3. Reduce 端：对每个分区内的数据做最终 reduce
        """
        from .shuffle import get_shuffle_manager
        
        parent = self
        num_partitions = num_partitions or self.num_partitions
        shuffle_manager = get_shuffle_manager()
        shuffle_id = shuffle_manager.new_shuffle_id()
        
        def compute_func(partition_id: int) -> Iterator[Tuple[K, V]]:
            # 从 shuffle 读取数据
            records, _ = shuffle_manager.read_shuffle(shuffle_id, partition_id)
            
            # 按 key 聚合
            aggregated: Dict[K, V] = {}
            for key, value in records:
                if key in aggregated:
                    aggregated[key] = f(aggregated[key], value)
                else:
                    aggregated[key] = value
            
            for key, value in aggregated.items():
                yield (key, value)
        
        rdd = RDD(
            context=self.context,
            name=f"reduceByKey({self.name})",
            num_partitions=num_partitions,
            dependencies=[Dependency(
                self, 
                DependencyType.WIDE,
                shuffle_id=shuffle_id
            )],
            compute_func=compute_func
        )
        rdd._shuffle_id = shuffle_id
        rdd._shuffle_parent = parent
        rdd._reduce_func = f
        rdd._is_shuffle_rdd = True
        
        return rdd
    
    def groupByKey(self, num_partitions: int = None) -> 'RDD[Tuple[K, List[V]]]':
        """
        按 key 分组
        
        宽依赖，需要 shuffle。
        注意：groupByKey 比 reduceByKey 效率低，因为不能在 map 端预聚合。
        """
        from .shuffle import get_shuffle_manager
        
        parent = self
        num_partitions = num_partitions or self.num_partitions
        shuffle_manager = get_shuffle_manager()
        shuffle_id = shuffle_manager.new_shuffle_id()
        
        def compute_func(partition_id: int) -> Iterator[Tuple[K, List[V]]]:
            # 从 shuffle 读取数据
            records, _ = shuffle_manager.read_shuffle(shuffle_id, partition_id)
            
            # 按 key 分组
            grouped: Dict[K, List[V]] = {}
            for key, value in records:
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(value)
            
            for key, values in grouped.items():
                yield (key, values)
        
        rdd = RDD(
            context=self.context,
            name=f"groupByKey({self.name})",
            num_partitions=num_partitions,
            dependencies=[Dependency(
                self, 
                DependencyType.WIDE,
                shuffle_id=shuffle_id
            )],
            compute_func=compute_func
        )
        rdd._shuffle_id = shuffle_id
        rdd._shuffle_parent = parent
        rdd._is_shuffle_rdd = True
        
        return rdd
    
    def join(
        self, 
        other: 'RDD[Tuple[K, V]]', 
        num_partitions: int = None
    ) -> 'RDD[Tuple[K, Tuple[V, V]]]':
        """
        按 key 连接两个 RDD
        
        宽依赖，需要 shuffle 两个 RDD。
        """
        # 简化实现：先 cogroup 再展开
        from .shuffle import get_shuffle_manager
        
        num_partitions = num_partitions or max(self.num_partitions, other.num_partitions)
        shuffle_manager = get_shuffle_manager()
        shuffle_id_left = shuffle_manager.new_shuffle_id()
        shuffle_id_right = shuffle_manager.new_shuffle_id()
        
        parent_left = self
        parent_right = other
        
        def compute_func(partition_id: int) -> Iterator[Tuple[K, Tuple[V, V]]]:
            # 读取两个 shuffle 的数据
            left_records, _ = shuffle_manager.read_shuffle(shuffle_id_left, partition_id)
            right_records, _ = shuffle_manager.read_shuffle(shuffle_id_right, partition_id)
            
            # 按 key 分组
            left_grouped: Dict[K, List[V]] = {}
            for key, value in left_records:
                if key not in left_grouped:
                    left_grouped[key] = []
                left_grouped[key].append(value)
            
            right_grouped: Dict[K, List[V]] = {}
            for key, value in right_records:
                if key not in right_grouped:
                    right_grouped[key] = []
                right_grouped[key].append(value)
            
            # 内连接
            for key in left_grouped:
                if key in right_grouped:
                    for lv in left_grouped[key]:
                        for rv in right_grouped[key]:
                            yield (key, (lv, rv))
        
        rdd = RDD(
            context=self.context,
            name=f"join({self.name}, {other.name})",
            num_partitions=num_partitions,
            dependencies=[
                Dependency(self, DependencyType.WIDE, shuffle_id=shuffle_id_left),
                Dependency(other, DependencyType.WIDE, shuffle_id=shuffle_id_right)
            ],
            compute_func=compute_func
        )
        rdd._shuffle_id = (shuffle_id_left, shuffle_id_right)
        rdd._shuffle_parents = (parent_left, parent_right)
        rdd._is_shuffle_rdd = True
        rdd._is_join = True
        
        return rdd
    
    # ==================== Actions ====================
    # Actions 触发实际计算，启动 Job
    
    def collect(self) -> List[T]:
        """
        收集所有分区的数据到 Driver
        
        这是一个 Action，会触发 Job 执行。
        注意：collect 会将所有数据拉到 Driver，数据量大时可能 OOM。
        """
        return self.context._run_action(self, "collect")
    
    def count(self) -> int:
        """
        计算元素总数
        
        Action，触发 Job 执行。
        """
        return self.context._run_action(self, "count")
    
    def take(self, n: int) -> List[T]:
        """
        获取前 n 个元素
        
        Action，触发 Job 执行。
        """
        return self.context._run_action(self, "take", n=n)
    
    def saveAsTextFile(self, path: str):
        """
        将 RDD 保存为文本文件
        
        Action，触发 Job 执行。
        每个分区保存为一个文件。
        """
        return self.context._run_action(self, "saveAsTextFile", path=path)
    
    def first(self) -> T:
        """获取第一个元素"""
        result = self.take(1)
        if result:
            return result[0]
        raise ValueError("RDD is empty")
    
    def reduce(self, f: Callable[[T, T], T]) -> T:
        """
        使用函数 f 聚合所有元素
        
        Action，触发 Job 执行。
        """
        return self.context._run_action(self, "reduce", reduce_func=f)
    
    # ==================== Persistence ====================
    
    def persist(self) -> 'RDD[T]':
        """
        标记 RDD 为持久化
        
        下次计算时会缓存结果，后续使用时可以直接从缓存读取。
        """
        self._cached = True
        return self
    
    def cache(self) -> 'RDD[T]':
        """
        cache() 是 persist() 的简写，使用内存存储
        """
        return self.persist()
    
    def unpersist(self) -> 'RDD[T]':
        """
        取消持久化，释放缓存
        """
        self._cached = False
        with self._cache_lock:
            self._cache_blocks.clear()
        return self
    
    def is_cached(self) -> bool:
        """检查是否已缓存"""
        return self._cached
    
    # ==================== Internal Methods ====================
    
    def _compute_partition(self, partition_id: int) -> Iterator[T]:
        """
        计算分区数据（内部使用）
        
        这个方法处理缓存逻辑：
        1. 如果已缓存，直接返回缓存数据
        2. 否则计算并可能缓存结果
        """
        # 检查缓存
        if self._cached:
            cached = self.get_cached(partition_id)
            if cached is not None:
                get_logger().log(
                    EventType.CACHE_HIT,
                    rdd_id=self.id,
                    partition_id=partition_id,
                    input_records=len(cached),
                    note=f"缓存命中: RDD[{self.id}] 分区 {partition_id}, {len(cached)} 条记录"
                )
                return iter(cached)
        
        # 计算分区
        result = list(self.compute(partition_id))
        
        # 如果需要缓存，保存结果
        if self._cached and self.get_cached(partition_id) is None:
            self.put_cache(partition_id, result)
        
        return iter(result)
    
    def _get_lineage(self) -> List['RDD']:
        """
        获取完整的血统（lineage）
        
        从当前 RDD 向上遍历所有祖先 RDD。
        """
        lineage = [self]
        visited = {self.id}
        queue = list(self.parents)
        
        while queue:
            rdd = queue.pop(0)
            if rdd.id not in visited:
                visited.add(rdd.id)
                lineage.append(rdd)
                queue.extend(rdd.parents)
        
        return lineage
    
    def __repr__(self) -> str:
        return f"RDD[{self.id}] {self.name} (partitions={self.num_partitions})"


class ParallelCollectionRDD(RDD[T]):
    """
    从内存集合创建的 RDD
    
    这是最基本的 RDD 类型，数据直接来自 Driver 的内存。
    """
    
    def __init__(
        self, 
        context: 'SparkContext', 
        data: List[T], 
        num_partitions: int = None
    ):
        self._data = data
        num_partitions = num_partitions or min(len(data), 4)
        
        super().__init__(
            context=context,
            name="parallelize",
            num_partitions=num_partitions
        )
        
        # 将数据分配到各个分区
        self._partitions = self._partition_data(data, num_partitions)
    
    def _partition_data(self, data: List[T], num_partitions: int) -> List[List[T]]:
        """将数据均匀分配到分区"""
        partitions = [[] for _ in range(num_partitions)]
        for i, item in enumerate(data):
            partitions[i % num_partitions].append(item)
        return partitions
    
    def compute(self, partition_id: int) -> Iterator[T]:
        return iter(self._partitions[partition_id])


class TextFileRDD(RDD[str]):
    """
    从文本文件创建的 RDD
    
    每行作为一个元素。
    """
    
    def __init__(
        self, 
        context: 'SparkContext', 
        path: str, 
        num_partitions: int = None
    ):
        self._path = path
        
        # 读取文件内容
        with open(path, 'r', encoding='utf-8') as f:
            self._lines = f.readlines()
        
        num_partitions = num_partitions or min(len(self._lines) // 10 + 1, 4)
        
        super().__init__(
            context=context,
            name=f"textFile({os.path.basename(path)})",
            num_partitions=num_partitions
        )
        
        # 将行分配到各个分区
        self._partitions = self._partition_lines(self._lines, num_partitions)
    
    def _partition_lines(self, lines: List[str], num_partitions: int) -> List[List[str]]:
        """将行均匀分配到分区"""
        partitions = [[] for _ in range(num_partitions)]
        for i, line in enumerate(lines):
            partitions[i % num_partitions].append(line.rstrip('\n'))
        return partitions
    
    def compute(self, partition_id: int) -> Iterator[str]:
        return iter(self._partitions[partition_id])
