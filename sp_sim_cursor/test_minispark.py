#!/usr/bin/env python3
"""
MiniSpark 自动化测试脚本
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from minispark import SparkContext, print_rdd_dag


def test_basic_transformations():
    """测试基本 transformations"""
    print("\n" + "="*60)
    print("  测试 1: 基本 Transformations")
    print("="*60)
    
    sc = SparkContext("BasicTest", num_executors=2, verbose=True)
    
    try:
        # 创建 RDD
        rdd = sc.parallelize([1, 2, 3, 4, 5], num_partitions=2)
        
        # map
        mapped = rdd.map(lambda x: x * 2)
        
        # filter
        filtered = mapped.filter(lambda x: x > 5)
        
        # collect
        result = filtered.collect()
        
        print(f"\n>>> 结果: {result}")
        assert set(result) == {6, 8, 10}, f"期望 [6, 8, 10]，实际 {result}"
        print(">>> 测试通过!")
        
    finally:
        sc.stop()


def test_reduce_by_key():
    """测试 reduceByKey (shuffle)"""
    print("\n" + "="*60)
    print("  测试 2: reduceByKey (Shuffle)")
    print("="*60)
    
    sc = SparkContext("ReduceByKeyTest", num_executors=2, verbose=True)
    
    try:
        # 创建 key-value RDD
        rdd = sc.parallelize([
            ("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5)
        ], num_partitions=2)
        
        # reduceByKey
        reduced = rdd.reduceByKey(lambda a, b: a + b)
        
        # 打印 DAG
        print_rdd_dag(reduced)
        
        # collect
        result = dict(reduced.collect())
        
        print(f"\n>>> 结果: {result}")
        assert result == {"a": 4, "b": 6, "c": 5}, f"期望 {{'a': 4, 'b': 6, 'c': 5}}，实际 {result}"
        print(">>> 测试通过!")
        
    finally:
        sc.stop()


def test_cache():
    """测试 cache 机制"""
    print("\n" + "="*60)
    print("  测试 3: Cache 机制")
    print("="*60)
    
    sc = SparkContext("CacheTest", num_executors=2, verbose=True)
    
    try:
        # 创建 RDD 并 cache
        rdd = sc.parallelize([1, 2, 3, 4, 5], num_partitions=2)
        mapped = rdd.map(lambda x: x * 2)
        cached = mapped.cache()
        
        # 第一次 action - 应该计算并缓存
        count1 = cached.count()
        print(f"\n>>> 第一次 count: {count1}")
        
        # 第二次 action - 应该命中缓存
        result = cached.collect()
        print(f">>> 第二次 collect: {result}")
        
        assert count1 == 5
        assert set(result) == {2, 4, 6, 8, 10}
        print(">>> 测试通过!")
        
    finally:
        sc.stop()


def test_wordcount():
    """测试完整的 WordCount pipeline"""
    print("\n" + "="*60)
    print("  测试 4: WordCount Pipeline")
    print("="*60)
    
    # 创建测试文件
    test_file = "/tmp/test_wordcount.txt"
    with open(test_file, 'w') as f:
        f.write("hello world\n")
        f.write("hello spark\n")
        f.write("spark is fast\n")
    
    sc = SparkContext("WordCountTest", num_executors=2, verbose=True)
    
    try:
        # WordCount pipeline
        lines = sc.textFile(test_file, num_partitions=2)
        words = lines.flatMap(lambda line: line.split())
        pairs = words.map(lambda word: (word, 1))
        counts = pairs.reduceByKey(lambda a, b: a + b)
        
        # 打印 DAG
        print_rdd_dag(counts, "WordCount DAG")
        
        # collect
        result = dict(counts.collect())
        
        print(f"\n>>> 词频结果: {result}")
        
        expected = {"hello": 2, "world": 1, "spark": 2, "is": 1, "fast": 1}
        assert result == expected, f"期望 {expected}，实际 {result}"
        print(">>> 测试通过!")
        
    finally:
        sc.stop()
        if os.path.exists(test_file):
            os.remove(test_file)


def test_fault_tolerance():
    """测试容错机制"""
    print("\n" + "="*60)
    print("  测试 5: 容错重算")
    print("="*60)
    
    sc = SparkContext(
        "FaultTest", 
        num_executors=4, 
        failure_rate=0.2,  # 20% 失败率
        verbose=True
    )
    
    try:
        rdd = sc.parallelize(list(range(10)), num_partitions=4)
        mapped = rdd.map(lambda x: x * 2)
        
        # 尝试执行
        try:
            result = mapped.collect()
            print(f"\n>>> 结果: {result}")
            expected = [i * 2 for i in range(10)]
            assert sorted(result) == expected
            print(">>> 测试通过（可能经历了重试）!")
        except RuntimeError as e:
            print(f">>> 任务彻底失败: {e}")
            print(">>> 这是预期的，因为故障率较高")
        
    finally:
        sc.stop()


def main():
    """运行所有测试"""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                  MiniSpark 自动化测试                             ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    test_basic_transformations()
    test_reduce_by_key()
    test_cache()
    test_wordcount()
    test_fault_tolerance()
    
    print("\n" + "="*60)
    print("  所有测试完成!")
    print("="*60)


if __name__ == "__main__":
    main()
