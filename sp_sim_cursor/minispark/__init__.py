"""
MiniSpark - 教学用 Spark 模拟框架
================================

MiniSpark 是一个纯 Python 实现的 Spark 核心抽象模拟器，
用于帮助理解 Spark 的内部工作原理。

主要特性：
- RDD 抽象（支持 transformations 和 actions）
- 惰性求值
- DAG 调度（Job/Stage/Task 切分）
- Shuffle 机制
- Cache/Persist
- Lineage 容错重算
- Driver/Executor 架构模拟

示例用法：
    from minispark import SparkContext
    
    sc = SparkContext("MyApp", num_executors=4)
    
    # 创建 RDD
    rdd = sc.parallelize([1, 2, 3, 4, 5])
    
    # Transformations（惰性）
    result = rdd.map(lambda x: x * 2).filter(lambda x: x > 5)
    
    # Action（触发执行）
    print(result.collect())
    
    sc.stop()
"""

from .context import SparkContext, create_context
from .rdd import RDD, ParallelCollectionRDD, TextFileRDD, DependencyType
from .scheduler import DAGScheduler, Stage, Job, StageType
from .cluster import ClusterManager, Executor, Task, TaskResult, TaskMetrics
from .shuffle import ShuffleManager
from .logger import MiniSparkLogger, EventType, get_logger, set_logger
from .dag import DAGVisualizer, print_rdd_dag, print_lineage


__version__ = "1.0.0"
__author__ = "MiniSpark Team"


__all__ = [
    # Context
    "SparkContext",
    "create_context",
    
    # RDD
    "RDD",
    "ParallelCollectionRDD", 
    "TextFileRDD",
    "DependencyType",
    
    # Scheduler
    "DAGScheduler",
    "Stage",
    "Job",
    "StageType",
    
    # Cluster
    "ClusterManager",
    "Executor",
    "Task",
    "TaskResult",
    "TaskMetrics",
    
    # Shuffle
    "ShuffleManager",
    
    # Logger
    "MiniSparkLogger",
    "EventType",
    "get_logger",
    "set_logger",
    
    # DAG
    "DAGVisualizer",
    "print_rdd_dag",
    "print_lineage",
]
