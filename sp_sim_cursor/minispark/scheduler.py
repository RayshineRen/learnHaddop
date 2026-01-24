"""
MiniSpark 调度器
================
实现 DAG 调度，包括 Job/Stage/Task 的切分和执行。
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set, Tuple, Callable
from enum import Enum
import time

from .rdd import RDD, DependencyType
from .cluster import Task, TaskState, ClusterManager, get_cluster_manager, TaskResult
from .shuffle import get_shuffle_manager
from .logger import get_logger, EventType


class StageType(Enum):
    """Stage 类型"""
    SHUFFLE_MAP = "ShuffleMapStage"   # 产生 shuffle 输出的 stage
    RESULT = "ResultStage"             # 产生最终结果的 stage


@dataclass
class Stage:
    """
    Stage 定义
    
    Stage 是一组可以并行执行的任务集合。
    Stage 的边界由 shuffle 依赖决定：
    - 窄依赖可以在同一个 Stage 内流水线执行
    - 宽依赖（shuffle）需要切分 Stage
    """
    stage_id: int
    rdd: RDD
    stage_type: StageType
    parent_stages: List['Stage'] = field(default_factory=list)
    shuffle_dep: Optional['Dependency'] = None
    num_partitions: int = 0
    is_shuffle_boundary: bool = False
    
    def __post_init__(self):
        self.num_partitions = self.rdd.num_partitions


@dataclass
class Job:
    """
    Job 定义
    
    一个 Action 触发一个 Job。
    Job 包含一个或多个 Stage。
    """
    job_id: int
    final_rdd: RDD
    action_type: str
    action_params: Dict[str, Any] = field(default_factory=dict)
    stages: List[Stage] = field(default_factory=list)
    start_time: float = 0
    end_time: float = 0


class DAGScheduler:
    """
    DAG 调度器
    
    负责：
    1. 将 RDD DAG 转换为 Stage DAG
    2. 按依赖顺序调度 Stage
    3. 为每个 Stage 生成 TaskSet
    """
    
    def __init__(self, cluster_manager: ClusterManager = None):
        self.cluster_manager = cluster_manager or get_cluster_manager()
        self._job_counter = 0
        self._stage_counter = 0
        self._task_counter = 0
        self._active_jobs: Dict[int, Job] = {}
        
    def submit_job(
        self, 
        rdd: RDD, 
        action_type: str, 
        **action_params
    ) -> Tuple[Any, Job]:
        """
        提交 Job
        
        这是调度器的入口点，当用户调用 Action 时被调用。
        """
        logger = get_logger()
        
        # 创建 Job
        self._job_counter += 1
        job = Job(
            job_id=self._job_counter,
            final_rdd=rdd,
            action_type=action_type,
            action_params=action_params
        )
        job.start_time = time.time()
        
        logger.log(
            EventType.ACTION_CALLED,
            job_id=job.job_id,
            rdd_id=rdd.id,
            note=f"Action '{action_type}' 被调用，触发 Job {job.job_id}"
        )
        
        # 构建 Stage DAG
        final_stage = self._build_stages(rdd, job)
        job.stages = self._get_stage_order(final_stage)
        
        logger.log(
            EventType.JOB_SUBMITTED,
            job_id=job.job_id,
            note=f"Job {job.job_id} 已提交，包含 {len(job.stages)} 个 Stage"
        )
        
        # 打印物理执行计划
        self._print_physical_plan(job)
        
        # 执行 Job
        self._active_jobs[job.job_id] = job
        result = self._run_job(job)
        
        job.end_time = time.time()
        del self._active_jobs[job.job_id]
        
        # 打印执行摘要
        logger.print_summary(job.job_id)
        
        return result, job
    
    def _build_stages(self, rdd: RDD, job: Job) -> Stage:
        """
        构建 Stage DAG
        
        从最终 RDD 反向遍历，遇到 shuffle 依赖就切分 Stage。
        
        关键思想：
        - 窄依赖形成流水线，可以在同一个 Stage 内执行
        - 宽依赖需要先完成父 Stage（shuffle map），才能执行子 Stage（shuffle read）
        """
        logger = get_logger()
        
        # 缓存已创建的 Stage，避免重复创建
        shuffle_to_stage: Dict[int, Stage] = {}
        
        def get_or_create_parent_stages(rdd: RDD) -> List[Stage]:
            """获取或创建父 Stage"""
            parent_stages = []
            
            for dep in rdd.dependencies:
                if dep.dependency_type == DependencyType.WIDE:
                    # 宽依赖：需要创建新的 ShuffleMapStage
                    shuffle_id = dep.shuffle_id
                    
                    if shuffle_id not in shuffle_to_stage:
                        self._stage_counter += 1
                        parent_stage = Stage(
                            stage_id=self._stage_counter,
                            rdd=dep.parent_rdd,
                            stage_type=StageType.SHUFFLE_MAP,
                            parent_stages=get_or_create_parent_stages(dep.parent_rdd),
                            shuffle_dep=dep,
                            is_shuffle_boundary=True
                        )
                        shuffle_to_stage[shuffle_id] = parent_stage
                        
                        logger.log(
                            EventType.STAGE_PLANNED,
                            job_id=job.job_id,
                            stage_id=parent_stage.stage_id,
                            rdd_id=dep.parent_rdd.id,
                            dependency="wide",
                            note=f"Stage {parent_stage.stage_id} (ShuffleMap): RDD[{dep.parent_rdd.id}] -> shuffle_{shuffle_id}"
                        )
                    
                    parent_stages.append(shuffle_to_stage[shuffle_id])
                else:
                    # 窄依赖：递归处理父 RDD 的依赖
                    parent_stages.extend(get_or_create_parent_stages(dep.parent_rdd))
            
            return parent_stages
        
        # 创建最终的 ResultStage
        self._stage_counter += 1
        final_stage = Stage(
            stage_id=self._stage_counter,
            rdd=rdd,
            stage_type=StageType.RESULT,
            parent_stages=get_or_create_parent_stages(rdd)
        )
        
        logger.log(
            EventType.STAGE_PLANNED,
            job_id=job.job_id,
            stage_id=final_stage.stage_id,
            rdd_id=rdd.id,
            dependency="narrow" if not rdd.dependencies or rdd.dependencies[0].dependency_type == DependencyType.NARROW else "wide",
            note=f"Stage {final_stage.stage_id} (Result): 最终结果 Stage"
        )
        
        return final_stage
    
    def _get_stage_order(self, final_stage: Stage) -> List[Stage]:
        """
        获取 Stage 的执行顺序（拓扑排序）
        
        父 Stage 必须在子 Stage 之前执行。
        """
        stages = []
        visited = set()
        
        def visit(stage: Stage):
            if stage.stage_id in visited:
                return
            visited.add(stage.stage_id)
            
            for parent in stage.parent_stages:
                visit(parent)
            
            stages.append(stage)
        
        visit(final_stage)
        return stages
    
    def _run_job(self, job: Job) -> Any:
        """
        执行 Job
        
        按顺序执行每个 Stage，收集最终结果。
        """
        logger = get_logger()
        
        logger.section(f"开始执行 Job {job.job_id} (action={job.action_type})")
        
        for i, stage in enumerate(job.stages):
            logger.subsection(f"Stage {stage.stage_id} ({stage.stage_type.value})")
            
            logger.log(
                EventType.STAGE_START,
                job_id=job.job_id,
                stage_id=stage.stage_id,
                rdd_id=stage.rdd.id,
                note=f"Stage {stage.stage_id} 开始执行，{stage.num_partitions} 个分区"
            )
            
            if stage.stage_type == StageType.SHUFFLE_MAP:
                self._run_shuffle_map_stage(job, stage)
            else:
                result = self._run_result_stage(job, stage)
            
            logger.log(
                EventType.STAGE_END,
                job_id=job.job_id,
                stage_id=stage.stage_id,
                note=f"Stage {stage.stage_id} 执行完成"
            )
        
        return result
    
    def _run_shuffle_map_stage(self, job: Job, stage: Stage):
        """
        执行 ShuffleMap Stage
        
        计算每个分区的数据，并写入 shuffle 文件。
        """
        shuffle_manager = get_shuffle_manager()
        shuffle_id = stage.shuffle_dep.shuffle_id
        
        # 获取下游 RDD 的分区数（reduce 端分区数）
        num_reduce_partitions = stage.rdd.num_partitions
        
        # 为每个分区创建任务
        tasks = []
        for partition_id in range(stage.num_partitions):
            self._task_counter += 1
            
            # 创建计算函数
            compute_func = self._create_shuffle_map_task_func(
                stage.rdd, 
                shuffle_id, 
                num_reduce_partitions
            )
            
            task = Task(
                task_id=self._task_counter,
                stage_id=stage.stage_id,
                job_id=job.job_id,
                partition_id=partition_id,
                rdd_id=stage.rdd.id,
                compute_func=compute_func
            )
            tasks.append(task)
        
        # 并行执行所有任务
        results = self._execute_tasks(tasks, job, stage)
    
    def _run_result_stage(self, job: Job, stage: Stage) -> Any:
        """
        执行 Result Stage
        
        计算每个分区的数据，并根据 action 类型聚合结果。
        """
        # 为每个分区创建任务
        tasks = []
        for partition_id in range(stage.num_partitions):
            self._task_counter += 1
            
            # 创建计算函数
            compute_func = self._create_result_task_func(stage.rdd, job.action_type)
            
            task = Task(
                task_id=self._task_counter,
                stage_id=stage.stage_id,
                job_id=job.job_id,
                partition_id=partition_id,
                rdd_id=stage.rdd.id,
                compute_func=compute_func
            )
            tasks.append(task)
        
        # 并行执行所有任务
        results = self._execute_tasks(tasks, job, stage)
        
        # 根据 action 类型聚合结果
        return self._aggregate_results(job, results)
    
    def _create_shuffle_map_task_func(
        self, 
        rdd: RDD, 
        shuffle_id: int,
        num_reduce_partitions: int
    ) -> Callable:
        """创建 ShuffleMap 任务的计算函数"""
        shuffle_manager = get_shuffle_manager()
        
        def compute(partition_id: int, executor_id: str):
            metrics = {
                'input_records': 0,
                'output_records': 0,
                'shuffle_write_records': 0,
                'cache_hit': False,
                'cache_put': False
            }
            
            # 计算分区数据
            records = []
            for item in rdd._compute_partition(partition_id):
                records.append(item)
                metrics['input_records'] += 1
            
            # 检查是否有缓存命中
            if rdd._cached and rdd.get_cached(partition_id) is not None:
                metrics['cache_hit'] = True
            
            # 写入 shuffle
            shuffle_manager.write_shuffle(
                shuffle_id=shuffle_id,
                map_task_id=partition_id,
                map_partition=partition_id,
                records=records,
                num_reduce_partitions=num_reduce_partitions
            )
            
            metrics['shuffle_write_records'] = len(records)
            metrics['output_records'] = len(records)
            
            return None, metrics
        
        return compute
    
    def _create_result_task_func(self, rdd: RDD, action_type: str) -> Callable:
        """创建 Result 任务的计算函数"""
        
        def compute(partition_id: int, executor_id: str):
            metrics = {
                'input_records': 0,
                'output_records': 0,
                'cache_hit': False,
                'cache_put': False
            }
            
            # 检查是否有缓存命中（在计算前）
            if rdd._cached and rdd.get_cached(partition_id) is not None:
                metrics['cache_hit'] = True
            
            # 计算分区数据
            records = []
            for item in rdd._compute_partition(partition_id):
                records.append(item)
                metrics['input_records'] += 1
            
            metrics['output_records'] = len(records)
            
            # 检查是否写入缓存（计算后）
            if rdd._cached and not metrics['cache_hit']:
                metrics['cache_put'] = True
            
            return records, metrics
        
        return compute
    
    def _execute_tasks(
        self, 
        tasks: List[Task], 
        job: Job, 
        stage: Stage
    ) -> List[TaskResult]:
        """
        执行任务集
        
        支持失败重试和 lineage 重算。
        """
        logger = get_logger()
        
        results = []
        pending_tasks = list(tasks)
        
        while pending_tasks:
            # 提交所有待执行任务
            task_results = self.cluster_manager.submit_tasks(pending_tasks)
            
            # 处理结果
            failed_tasks = []
            for i, result in enumerate(task_results):
                task = pending_tasks[i]
                
                if result.success:
                    results.append(result)
                else:
                    # 任务失败，检查是否可以重试
                    task.attempt += 1
                    
                    if task.attempt < task.max_attempts:
                        # 触发 lineage 重算
                        logger.log(
                            EventType.LINEAGE_RECOMPUTE,
                            job_id=job.job_id,
                            stage_id=stage.stage_id,
                            task_id=task.task_id,
                            partition_id=task.partition_id,
                            rdd_id=task.rdd_id,
                            note=f"任务 T{task.task_id} 失败，通过 lineage 重算分区 {task.partition_id} (尝试 {task.attempt + 1}/{task.max_attempts})"
                        )
                        
                        logger.log(
                            EventType.TASK_RETRY,
                            job_id=job.job_id,
                            stage_id=stage.stage_id,
                            task_id=task.task_id,
                            partition_id=task.partition_id,
                            note=f"重试任务 T{task.task_id}"
                        )
                        
                        failed_tasks.append(task)
                    else:
                        raise RuntimeError(
                            f"任务 T{task.task_id} 达到最大重试次数 {task.max_attempts}"
                        )
            
            pending_tasks = failed_tasks
        
        return results
    
    def _aggregate_results(self, job: Job, results: List[TaskResult]) -> Any:
        """根据 action 类型聚合结果"""
        action_type = job.action_type
        
        if action_type == "collect":
            # 收集所有数据
            all_data = []
            for result in results:
                if result.result:
                    all_data.extend(result.result)
            return all_data
        
        elif action_type == "count":
            # 计算总数
            total = sum(len(r.result) if r.result else 0 for r in results)
            return total
        
        elif action_type == "take":
            # 获取前 n 个
            n = job.action_params.get('n', 10)
            all_data = []
            for result in results:
                if result.result:
                    all_data.extend(result.result)
                    if len(all_data) >= n:
                        break
            return all_data[:n]
        
        elif action_type == "reduce":
            # 使用 reduce 函数聚合
            reduce_func = job.action_params.get('reduce_func')
            all_data = []
            for result in results:
                if result.result:
                    all_data.extend(result.result)
            
            if not all_data:
                raise ValueError("Cannot reduce empty RDD")
            
            result = all_data[0]
            for item in all_data[1:]:
                result = reduce_func(result, item)
            return result
        
        elif action_type == "saveAsTextFile":
            # 保存到文件
            import os
            path = job.action_params.get('path')
            os.makedirs(path, exist_ok=True)
            
            for i, result in enumerate(results):
                file_path = os.path.join(path, f"part-{i:05d}")
                with open(file_path, 'w') as f:
                    if result.result:
                        for item in result.result:
                            f.write(str(item) + '\n')
            
            return None
        
        else:
            raise ValueError(f"Unknown action type: {action_type}")
    
    def _print_physical_plan(self, job: Job):
        """打印物理执行计划（ASCII 图）"""
        logger = get_logger()
        
        print(f"\n{'='*60}")
        print(f"  物理执行计划 - Job {job.job_id} (action={job.action_type})")
        print(f"{'='*60}")
        
        for i, stage in enumerate(job.stages):
            stage_type = "ShuffleMap" if stage.stage_type == StageType.SHUFFLE_MAP else "Result"
            
            # 任务信息
            tasks_str = "  ".join([f"T{self._task_counter + j + 1}[p{j}]" 
                                   for j in range(stage.num_partitions)])
            
            print(f"\n  STAGE {stage.stage_id} ({stage_type}, RDD[{stage.rdd.id}])")
            print(f"  {'-'*50}")
            print(f"    任务: {tasks_str}")
            print(f"    分区数: {stage.num_partitions}")
            
            if stage.is_shuffle_boundary and stage.shuffle_dep:
                print(f"    Shuffle: shuffle_{stage.shuffle_dep.shuffle_id}")
                print(f"         \\")
                print(f"          --> SHUFFLE WRITE")
            
            if i < len(job.stages) - 1:
                next_stage = job.stages[i + 1]
                if next_stage.parent_stages and stage in next_stage.parent_stages:
                    print(f"         |")
                    print(f"    SHUFFLE READ")
                    print(f"         |")
                    print(f"         v")
        
        print(f"\n  RESULT -> Driver")
        print(f"{'='*60}\n")
