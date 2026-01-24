"""
MiniSpark Shuffle 管理器
========================
管理 shuffle 数据的写入和读取，模拟 Spark 的 shuffle 过程。
"""

import os
import json
import shutil
from dataclasses import dataclass, field
from typing import Dict, List, Any, Tuple, Iterator
from collections import defaultdict
import threading

from .logger import get_logger, EventType


@dataclass
class ShuffleWriteResult:
    """Shuffle 写入结果"""
    shuffle_id: int
    map_task_id: int
    partition_id: int
    records_written: int
    bytes_written: int
    file_path: str


@dataclass 
class ShuffleReadResult:
    """Shuffle 读取结果"""
    shuffle_id: int
    reduce_partition: int
    records_read: int
    source_partitions: List[int]


class ShuffleManager:
    """
    Shuffle 管理器
    
    负责 shuffle 数据的写入和读取。
    在真实 Spark 中，shuffle 数据会写入本地磁盘，然后通过网络传输给 reducer。
    这里我们用本地文件系统模拟。
    """
    
    def __init__(self, base_dir: str = "./_shuffle"):
        self.base_dir = base_dir
        self._shuffle_counter = 0
        self._lock = threading.Lock()
        # 内存中也保存一份，方便调试
        self._shuffle_data: Dict[int, Dict[int, List[Tuple[Any, Any]]]] = defaultdict(
            lambda: defaultdict(list)
        )
        
    def new_shuffle_id(self) -> int:
        """生成新的 shuffle ID"""
        with self._lock:
            self._shuffle_counter += 1
            return self._shuffle_counter
    
    def get_shuffle_dir(self, shuffle_id: int) -> str:
        """获取 shuffle 数据目录"""
        return os.path.join(self.base_dir, f"shuffle_{shuffle_id}")
    
    def write_shuffle(
        self,
        shuffle_id: int,
        map_task_id: int,
        map_partition: int,
        records: List[Tuple[Any, Any]],
        num_reduce_partitions: int
    ) -> ShuffleWriteResult:
        """
        写入 shuffle 数据
        
        将 map 任务的输出按 key 的 hash 分区写入到对应的 reduce 分区文件。
        
        Args:
            shuffle_id: Shuffle ID
            map_task_id: Map 任务 ID
            map_partition: Map 分区 ID
            records: (key, value) 对的列表
            num_reduce_partitions: Reduce 分区数
        """
        logger = get_logger()
        
        shuffle_dir = self.get_shuffle_dir(shuffle_id)
        os.makedirs(shuffle_dir, exist_ok=True)
        
        # 按 reduce 分区组织数据
        partitioned_data: Dict[int, List[Tuple[Any, Any]]] = defaultdict(list)
        for key, value in records:
            reduce_partition = hash(key) % num_reduce_partitions
            partitioned_data[reduce_partition].append((key, value))
        
        total_bytes = 0
        total_records = 0
        
        # 写入每个 reduce 分区的文件
        for reduce_partition, partition_records in partitioned_data.items():
            file_path = os.path.join(
                shuffle_dir, 
                f"map_{map_partition}_reduce_{reduce_partition}.jsonl"
            )
            
            with open(file_path, 'a') as f:
                for key, value in partition_records:
                    line = json.dumps({"key": key, "value": value}, ensure_ascii=False)
                    f.write(line + "\n")
                    total_bytes += len(line) + 1
                    total_records += 1
            
            # 同时保存到内存
            with self._lock:
                self._shuffle_data[shuffle_id][reduce_partition].extend(partition_records)
        
        result = ShuffleWriteResult(
            shuffle_id=shuffle_id,
            map_task_id=map_task_id,
            partition_id=map_partition,
            records_written=total_records,
            bytes_written=total_bytes,
            file_path=shuffle_dir
        )
        
        logger.log(
            EventType.SHUFFLE_WRITE,
            shuffle_id=shuffle_id,
            task_id=map_task_id,
            partition_id=map_partition,
            output_records=total_records,
            note=f"Map 任务 {map_task_id} 将 {total_records} 条记录写入 shuffle_{shuffle_id}"
        )
        
        return result
    
    def read_shuffle(
        self,
        shuffle_id: int,
        reduce_partition: int
    ) -> Tuple[List[Tuple[Any, Any]], ShuffleReadResult]:
        """
        读取 shuffle 数据
        
        读取所有 map 任务为指定 reduce 分区写入的数据。
        
        Args:
            shuffle_id: Shuffle ID
            reduce_partition: Reduce 分区 ID
            
        Returns:
            (records, result) 元组
        """
        logger = get_logger()
        
        # 优先从内存读取
        with self._lock:
            if shuffle_id in self._shuffle_data:
                records = list(self._shuffle_data[shuffle_id][reduce_partition])
                
                result = ShuffleReadResult(
                    shuffle_id=shuffle_id,
                    reduce_partition=reduce_partition,
                    records_read=len(records),
                    source_partitions=[]
                )
                
                logger.log(
                    EventType.SHUFFLE_READ,
                    shuffle_id=shuffle_id,
                    partition_id=reduce_partition,
                    input_records=len(records),
                    note=f"Reduce 任务从 shuffle_{shuffle_id} 读取 {len(records)} 条记录到分区 {reduce_partition}"
                )
                
                return records, result
        
        # 从文件读取
        shuffle_dir = self.get_shuffle_dir(shuffle_id)
        records = []
        source_partitions = []
        
        if os.path.exists(shuffle_dir):
            for filename in os.listdir(shuffle_dir):
                if f"_reduce_{reduce_partition}.jsonl" in filename:
                    file_path = os.path.join(shuffle_dir, filename)
                    # 提取 map 分区 ID
                    map_partition = int(filename.split("_")[1])
                    source_partitions.append(map_partition)
                    
                    with open(file_path, 'r') as f:
                        for line in f:
                            data = json.loads(line.strip())
                            records.append((data["key"], data["value"]))
        
        result = ShuffleReadResult(
            shuffle_id=shuffle_id,
            reduce_partition=reduce_partition,
            records_read=len(records),
            source_partitions=source_partitions
        )
        
        logger.log(
            EventType.SHUFFLE_READ,
            shuffle_id=shuffle_id,
            partition_id=reduce_partition,
            input_records=len(records),
            note=f"Reduce 任务从 shuffle_{shuffle_id} 读取 {len(records)} 条记录到分区 {reduce_partition}"
        )
        
        return records, result
    
    def cleanup(self, shuffle_id: int = None):
        """清理 shuffle 数据"""
        if shuffle_id is not None:
            shuffle_dir = self.get_shuffle_dir(shuffle_id)
            if os.path.exists(shuffle_dir):
                shutil.rmtree(shuffle_dir)
            with self._lock:
                if shuffle_id in self._shuffle_data:
                    del self._shuffle_data[shuffle_id]
        else:
            if os.path.exists(self.base_dir):
                shutil.rmtree(self.base_dir)
            with self._lock:
                self._shuffle_data.clear()
    
    def cleanup_all(self):
        """清理所有 shuffle 数据"""
        self.cleanup()


# 全局 ShuffleManager 实例
_shuffle_manager: ShuffleManager = None


def get_shuffle_manager() -> ShuffleManager:
    """获取全局 ShuffleManager 实例"""
    global _shuffle_manager
    if _shuffle_manager is None:
        _shuffle_manager = ShuffleManager()
    return _shuffle_manager


def set_shuffle_manager(manager: ShuffleManager):
    """设置全局 ShuffleManager 实例"""
    global _shuffle_manager
    _shuffle_manager = manager
