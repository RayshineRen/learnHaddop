"""
MiniSpark 集群管理器和 Executor
==============================
模拟 Spark 的 Driver/Executor 架构。
"""

import time
import uuid
import random
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any, Tuple
from queue import Queue
from enum import Enum

from .logger import get_logger, EventType


class TaskState(Enum):
    """任务状态"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class TaskMetrics:
    """任务执行指标"""
    task_id: int
    executor_id: str
    start_time: float = 0
    end_time: float = 0
    input_records: int = 0
    output_records: int = 0
    shuffle_write_records: int = 0
    shuffle_read_records: int = 0
    cache_hit: bool = False
    cache_put: bool = False
    duration_ms: float = 0
    error: Optional[str] = None
    
    def finalize(self):
        """完成指标计算"""
        self.duration_ms = (self.end_time - self.start_time) * 1000


@dataclass
class Task:
    """任务定义"""
    task_id: int
    stage_id: int
    job_id: int
    partition_id: int
    rdd_id: int
    compute_func: Callable
    state: TaskState = TaskState.PENDING
    metrics: Optional[TaskMetrics] = None
    attempt: int = 0
    max_attempts: int = 3


@dataclass
class TaskResult:
    """任务执行结果"""
    task_id: int
    success: bool
    result: Any = None
    error: Optional[str] = None
    metrics: Optional[TaskMetrics] = None


class Executor:
    """
    Executor 执行器
    
    在真实 Spark 中，Executor 运行在集群的工作节点上。
    这里用线程模拟，每个 Executor 有自己的 ID。
    """
    
    def __init__(self, executor_id: str, failure_rate: float = 0.0):
        self.executor_id = executor_id
        self.failure_rate = failure_rate  # 模拟故障的概率
        self._running = True
        self._current_task: Optional[Task] = None
        
        get_logger().log(
            EventType.EXECUTOR_START,
            executor_id=executor_id,
            note=f"Executor {executor_id} 启动"
        )
    
    def execute_task(self, task: Task) -> TaskResult:
        """
        执行任务
        
        这是 Executor 的核心方法，执行 Driver 分配的任务。
        """
        logger = get_logger()
        
        self._current_task = task
        task.state = TaskState.RUNNING
        
        # 创建指标
        metrics = TaskMetrics(
            task_id=task.task_id,
            executor_id=self.executor_id
        )
        metrics.start_time = time.time()
        
        logger.log(
            EventType.EXECUTOR_TASK_RECEIVED,
            executor_id=self.executor_id,
            task_id=task.task_id,
            stage_id=task.stage_id,
            job_id=task.job_id,
            partition_id=task.partition_id,
            rdd_id=task.rdd_id,
            note=f"Executor {self.executor_id} 接收任务 T{task.task_id}[p{task.partition_id}]"
        )
        
        logger.log(
            EventType.TASK_START,
            executor_id=self.executor_id,
            task_id=task.task_id,
            stage_id=task.stage_id,
            job_id=task.job_id,
            partition_id=task.partition_id,
            rdd_id=task.rdd_id,
            note=f"任务 T{task.task_id} 在 Executor {self.executor_id} 上开始执行"
        )
        
        try:
            # 模拟随机故障
            if self.failure_rate > 0 and random.random() < self.failure_rate:
                raise RuntimeError(f"模拟 Executor {self.executor_id} 故障")
            
            # 执行计算
            result, task_metrics = task.compute_func(task.partition_id, self.executor_id)
            
            # 更新指标
            metrics.end_time = time.time()
            metrics.finalize()
            if task_metrics:
                metrics.input_records = task_metrics.get('input_records', 0)
                metrics.output_records = task_metrics.get('output_records', 0)
                metrics.shuffle_write_records = task_metrics.get('shuffle_write_records', 0)
                metrics.shuffle_read_records = task_metrics.get('shuffle_read_records', 0)
                metrics.cache_hit = task_metrics.get('cache_hit', False)
                metrics.cache_put = task_metrics.get('cache_put', False)
            
            task.state = TaskState.COMPLETED
            task.metrics = metrics
            
            logger.log(
                EventType.TASK_END,
                executor_id=self.executor_id,
                task_id=task.task_id,
                stage_id=task.stage_id,
                job_id=task.job_id,
                partition_id=task.partition_id,
                rdd_id=task.rdd_id,
                input_records=metrics.input_records,
                output_records=metrics.output_records,
                duration_ms=round(metrics.duration_ms, 2),
                cache_hit=metrics.cache_hit,
                note=f"任务 T{task.task_id} 完成: 输入 {metrics.input_records} 条, 输出 {metrics.output_records} 条, 耗时 {metrics.duration_ms:.2f}ms"
            )
            
            return TaskResult(
                task_id=task.task_id,
                success=True,
                result=result,
                metrics=metrics
            )
            
        except Exception as e:
            metrics.end_time = time.time()
            metrics.finalize()
            metrics.error = str(e)
            
            task.state = TaskState.FAILED
            task.metrics = metrics
            
            logger.log(
                EventType.TASK_FAILED,
                executor_id=self.executor_id,
                task_id=task.task_id,
                stage_id=task.stage_id,
                job_id=task.job_id,
                partition_id=task.partition_id,
                rdd_id=task.rdd_id,
                duration_ms=round(metrics.duration_ms, 2),
                note=f"任务 T{task.task_id} 失败: {str(e)}"
            )
            
            return TaskResult(
                task_id=task.task_id,
                success=False,
                error=str(e),
                metrics=metrics
            )
        finally:
            self._current_task = None
    
    def shutdown(self):
        """关闭 Executor"""
        self._running = False


class ClusterManager:
    """
    集群管理器
    
    管理 Executor 的生命周期，分配任务给 Executor 执行。
    在真实 Spark 中，这对应 YARN/Mesos/Kubernetes 或 Standalone 模式的 Master。
    """
    
    def __init__(self, num_executors: int = 4, failure_rate: float = 0.0):
        self.num_executors = num_executors
        self.failure_rate = failure_rate
        self.executors: Dict[str, Executor] = {}
        self._thread_pool: Optional[ThreadPoolExecutor] = None
        self._executor_futures: Dict[str, Future] = {}
        
    def start(self):
        """启动集群"""
        get_logger().section("集群启动")
        
        # 创建线程池模拟多个 Executor
        self._thread_pool = ThreadPoolExecutor(max_workers=self.num_executors)
        
        # 创建 Executor 实例
        for i in range(self.num_executors):
            executor_id = f"executor-{i}"
            self.executors[executor_id] = Executor(
                executor_id=executor_id,
                failure_rate=self.failure_rate
            )
        
        print(f"\n集群已启动: {self.num_executors} 个 Executor 就绪")
    
    def submit_task(self, task: Task) -> Future:
        """
        提交任务到集群执行
        
        选择一个 Executor 执行任务（这里简单地轮询选择）。
        """
        # 简单的轮询调度
        executor_id = f"executor-{task.task_id % self.num_executors}"
        executor = self.executors[executor_id]
        
        # 提交到线程池执行
        future = self._thread_pool.submit(executor.execute_task, task)
        return future
    
    def submit_tasks(self, tasks: List[Task]) -> List[TaskResult]:
        """
        批量提交任务并等待所有任务完成
        
        这模拟了 Stage 内的并行任务执行。
        """
        futures = []
        for task in tasks:
            future = self.submit_task(task)
            futures.append((task, future))
        
        # 等待所有任务完成并收集结果
        results = []
        for task, future in futures:
            try:
                result = future.result(timeout=60)
                results.append(result)
            except Exception as e:
                results.append(TaskResult(
                    task_id=task.task_id,
                    success=False,
                    error=str(e)
                ))
        
        return results
    
    def get_executor_count(self) -> int:
        """获取 Executor 数量"""
        return len(self.executors)
    
    def shutdown(self):
        """关闭集群"""
        for executor in self.executors.values():
            executor.shutdown()
        
        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)
        
        print("\n集群已关闭")


# 全局 ClusterManager 实例
_cluster_manager: Optional[ClusterManager] = None


def get_cluster_manager() -> ClusterManager:
    """获取全局 ClusterManager 实例"""
    global _cluster_manager
    if _cluster_manager is None:
        _cluster_manager = ClusterManager()
    return _cluster_manager


def set_cluster_manager(manager: ClusterManager):
    """设置全局 ClusterManager 实例"""
    global _cluster_manager
    _cluster_manager = manager
