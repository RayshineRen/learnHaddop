"""
MiniSpark 结构化日志系统
========================
提供可观测的结构化日志，帮助理解 Spark 执行过程。
"""

import json
import time
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from enum import Enum


class EventType(Enum):
    """日志事件类型"""
    DAG_BUILT = "DAG_BUILT"
    ACTION_CALLED = "ACTION_CALLED"
    JOB_SUBMITTED = "JOB_SUBMITTED"
    STAGE_PLANNED = "STAGE_PLANNED"
    STAGE_START = "STAGE_START"
    STAGE_END = "STAGE_END"
    TASK_START = "TASK_START"
    TASK_END = "TASK_END"
    SHUFFLE_WRITE = "SHUFFLE_WRITE"
    SHUFFLE_READ = "SHUFFLE_READ"
    CACHE_PUT = "CACHE_PUT"
    CACHE_HIT = "CACHE_HIT"
    LINEAGE_RECOMPUTE = "LINEAGE_RECOMPUTE"
    TASK_FAILED = "TASK_FAILED"
    TASK_RETRY = "TASK_RETRY"
    EXECUTOR_START = "EXECUTOR_START"
    EXECUTOR_TASK_RECEIVED = "EXECUTOR_TASK_RECEIVED"


@dataclass
class LogEntry:
    """结构化日志条目"""
    event: str
    timestamp: float = field(default_factory=time.time)
    job_id: Optional[int] = None
    stage_id: Optional[int] = None
    task_id: Optional[int] = None
    rdd_id: Optional[int] = None
    partition_id: Optional[int] = None
    executor_id: Optional[str] = None
    input_records: Optional[int] = None
    output_records: Optional[int] = None
    duration_ms: Optional[float] = None
    dependency: Optional[str] = None  # narrow/wide
    shuffle_id: Optional[int] = None
    cache_hit: Optional[bool] = None
    note: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None
    
    def to_json(self) -> str:
        """转换为 JSON 格式"""
        data = {k: v for k, v in asdict(self).items() if v is not None}
        return json.dumps(data, ensure_ascii=False)
    
    def to_kv(self) -> str:
        """转换为 key=value 格式"""
        data = {k: v for k, v in asdict(self).items() if v is not None}
        return " | ".join(f"{k}={v}" for k, v in data.items())


class MiniSparkLogger:
    """MiniSpark 日志管理器"""
    
    def __init__(self, verbose: bool = True, format: str = "kv"):
        self.verbose = verbose
        self.format = format  # "json" or "kv"
        self.entries: List[LogEntry] = []
        self._indent = 0
        
    def log(self, event: EventType, **kwargs) -> LogEntry:
        """记录日志事件"""
        entry = LogEntry(event=event.value, **kwargs)
        self.entries.append(entry)
        
        if self.verbose:
            self._print_entry(entry)
        
        return entry
    
    def _print_entry(self, entry: LogEntry):
        """打印日志条目"""
        indent = "  " * self._indent
        
        # 根据事件类型选择颜色标记
        event_markers = {
            "DAG_BUILT": "📊",
            "ACTION_CALLED": "🎯",
            "JOB_SUBMITTED": "📋",
            "STAGE_PLANNED": "📝",
            "STAGE_START": "▶️ ",
            "STAGE_END": "✅",
            "TASK_START": "⚙️ ",
            "TASK_END": "✔️ ",
            "SHUFFLE_WRITE": "📤",
            "SHUFFLE_READ": "📥",
            "CACHE_PUT": "💾",
            "CACHE_HIT": "🎯",
            "LINEAGE_RECOMPUTE": "🔄",
            "TASK_FAILED": "❌",
            "TASK_RETRY": "🔁",
            "EXECUTOR_START": "🖥️ ",
            "EXECUTOR_TASK_RECEIVED": "📨",
        }
        
        marker = event_markers.get(entry.event, "📌")
        
        if self.format == "json":
            print(f"{indent}{marker} {entry.to_json()}")
        else:
            print(f"{indent}{marker} {entry.to_kv()}")
    
    def indent(self):
        """增加缩进"""
        self._indent += 1
        
    def dedent(self):
        """减少缩进"""
        self._indent = max(0, self._indent - 1)
    
    def section(self, title: str):
        """打印分隔线"""
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"  {title}")
            print(f"{'='*60}")
    
    def subsection(self, title: str):
        """打印子分隔线"""
        if self.verbose:
            print(f"\n{'-'*40}")
            print(f"  {title}")
            print(f"{'-'*40}")
    
    def print_summary(self, job_id: int):
        """打印作业执行摘要"""
        job_entries = [e for e in self.entries if e.job_id == job_id]
        
        stages = set(e.stage_id for e in job_entries if e.stage_id is not None)
        tasks = [e for e in job_entries if e.event == "TASK_END"]
        shuffles = [e for e in job_entries if e.event in ("SHUFFLE_WRITE", "SHUFFLE_READ")]
        cache_hits = [e for e in job_entries if e.event == "CACHE_HIT"]
        cache_puts = [e for e in job_entries if e.event == "CACHE_PUT"]
        recomputes = [e for e in job_entries if e.event == "LINEAGE_RECOMPUTE"]
        failures = [e for e in job_entries if e.event == "TASK_FAILED"]
        
        total_duration = sum(e.duration_ms or 0 for e in tasks)
        total_input = sum(e.input_records or 0 for e in tasks)
        total_output = sum(e.output_records or 0 for e in tasks)
        
        print(f"\n{'='*60}")
        print(f"  JOB {job_id} 执行摘要")
        print(f"{'='*60}")
        print(f"  Stage 数量:        {len(stages)}")
        print(f"  Task 总数:         {len(tasks)}")
        print(f"  Shuffle 操作:      {len(shuffles)} (写: {len([s for s in shuffles if s.event == 'SHUFFLE_WRITE'])}, 读: {len([s for s in shuffles if s.event == 'SHUFFLE_READ'])})")
        print(f"  Cache 命中:        {len(cache_hits)}")
        print(f"  Cache 写入:        {len(cache_puts)}")
        print(f"  Lineage 重算:      {len(recomputes)}")
        print(f"  Task 失败/重试:    {len(failures)}")
        print(f"  总输入记录:        {total_input}")
        print(f"  总输出记录:        {total_output}")
        print(f"  总执行时间:        {total_duration:.2f} ms")
        print(f"{'='*60}\n")
    
    def clear(self):
        """清空日志"""
        self.entries.clear()


# 全局日志实例
_logger: Optional[MiniSparkLogger] = None


def get_logger() -> MiniSparkLogger:
    """获取全局日志实例"""
    global _logger
    if _logger is None:
        _logger = MiniSparkLogger()
    return _logger


def set_logger(logger: MiniSparkLogger):
    """设置全局日志实例"""
    global _logger
    _logger = logger
