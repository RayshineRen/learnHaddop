"""
MiniSpark Context
=================
SparkContext 是用户与 MiniSpark 交互的入口点。
"""

import os
from typing import List, TypeVar, Any

from .rdd import RDD, ParallelCollectionRDD, TextFileRDD
from .scheduler import DAGScheduler
from .cluster import ClusterManager, set_cluster_manager, get_cluster_manager
from .shuffle import ShuffleManager, set_shuffle_manager, get_shuffle_manager
from .logger import MiniSparkLogger, set_logger, get_logger
from .dag import DAGVisualizer, print_rdd_dag


T = TypeVar('T')


class SparkContext:
    """
    SparkContext - MiniSpark 的入口点
    
    在真实 Spark 中，SparkContext 是 Driver 程序与集群交互的入口。
    它负责：
    1. 初始化执行环境
    2. 创建 RDD
    3. 调度任务执行
    """
    
    _active_context = None
    
    def __init__(
        self,
        app_name: str = "MiniSpark",
        num_executors: int = 4,
        failure_rate: float = 0.0,
        verbose: bool = True,
        log_format: str = "kv"
    ):
        """
        初始化 SparkContext
        
        Args:
            app_name: 应用名称
            num_executors: Executor 数量
            failure_rate: 模拟故障率（用于测试容错）
            verbose: 是否输出详细日志
            log_format: 日志格式 ("json" 或 "kv")
        """
        self.app_name = app_name
        self.num_executors = num_executors
        self.failure_rate = failure_rate
        
        # 初始化日志
        self._logger = MiniSparkLogger(verbose=verbose, format=log_format)
        set_logger(self._logger)
        
        # 初始化 ShuffleManager
        self._shuffle_manager = ShuffleManager()
        set_shuffle_manager(self._shuffle_manager)
        
        # 初始化 ClusterManager
        self._cluster_manager = ClusterManager(
            num_executors=num_executors,
            failure_rate=failure_rate
        )
        set_cluster_manager(self._cluster_manager)
        
        # 初始化调度器
        self._scheduler = DAGScheduler(self._cluster_manager)
        
        # DAG 可视化器
        self._dag_visualizer = DAGVisualizer()
        
        # 启动集群
        self._cluster_manager.start()
        
        # 设置为活跃 context
        SparkContext._active_context = self
        
        print(f"\n{'='*60}")
        print(f"  MiniSpark 初始化完成")
        print(f"  应用名称: {app_name}")
        print(f"  Executor 数量: {num_executors}")
        print(f"  故障模拟率: {failure_rate*100:.1f}%")
        print(f"{'='*60}\n")
    
    @classmethod
    def get_or_create(cls, **kwargs) -> 'SparkContext':
        """获取或创建 SparkContext"""
        if cls._active_context is None:
            return cls(**kwargs)
        return cls._active_context
    
    def parallelize(self, data: List[T], num_partitions: int = None) -> RDD[T]:
        """
        从内存集合创建 RDD
        
        这是创建 RDD 的基本方式之一。
        
        Args:
            data: Python 列表
            num_partitions: 分区数（默认根据数据量自动决定）
        
        Returns:
            ParallelCollectionRDD
        """
        return ParallelCollectionRDD(
            context=self,
            data=data,
            num_partitions=num_partitions
        )
    
    def textFile(self, path: str, num_partitions: int = None) -> RDD[str]:
        """
        从文本文件创建 RDD
        
        每行作为一个元素。
        
        Args:
            path: 文件路径
            num_partitions: 分区数
        
        Returns:
            TextFileRDD
        """
        return TextFileRDD(
            context=self,
            path=path,
            num_partitions=num_partitions
        )
    
    def _run_action(self, rdd: RDD, action_type: str, **params) -> Any:
        """
        执行 Action
        
        这是内部方法，由 RDD 的 Action 方法调用。
        """
        # 打印逻辑 DAG
        self._logger.section(f"Action: {action_type} on RDD[{rdd.id}]")
        print_rdd_dag(rdd, f"逻辑 DAG (执行 {action_type} 前)")
        
        # 提交 Job 到调度器
        result, job = self._scheduler.submit_job(rdd, action_type, **params)
        
        return result
    
    def stop(self):
        """停止 SparkContext"""
        # 关闭集群
        self._cluster_manager.shutdown()
        
        # 清理 shuffle 数据
        self._shuffle_manager.cleanup_all()
        
        # 清除活跃 context
        if SparkContext._active_context == self:
            SparkContext._active_context = None
        
        print(f"\nSparkContext 已停止")
    
    def set_failure_rate(self, rate: float):
        """设置故障率（用于测试容错）"""
        self.failure_rate = rate
        for executor in self._cluster_manager.executors.values():
            executor.failure_rate = rate
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False


# 便捷函数
def create_context(**kwargs) -> SparkContext:
    """创建 SparkContext 的便捷函数"""
    return SparkContext(**kwargs)
