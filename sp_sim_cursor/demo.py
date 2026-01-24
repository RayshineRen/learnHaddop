#!/usr/bin/env python3
"""
MiniSpark 演示程序
==================

这个演示展示了 MiniSpark 的核心功能：
1. RDD 创建和 transformations
2. 窄依赖和宽依赖的区别
3. Action 触发 Job 执行
4. Stage 切分（遇到 shuffle 边界）
5. Cache 机制
6. Lineage 容错重算

运行方式：
    python demo.py
"""

import os
import sys
import time

# 添加父目录到路径，以便导入 minispark
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from minispark import SparkContext, print_rdd_dag, print_lineage


def create_sample_text_file(path: str, num_lines: int = 100):
    """创建示例文本文件"""
    sample_words = [
        "spark", "hadoop", "mapreduce", "rdd", "transformation",
        "action", "shuffle", "partition", "executor", "driver",
        "stage", "task", "job", "dag", "lineage", "cache",
        "persist", "collect", "count", "filter", "map",
        "flatmap", "reduce", "groupby", "join", "python"
    ]
    
    import random
    random.seed(42)
    
    with open(path, 'w') as f:
        for i in range(num_lines):
            # 每行 5-10 个随机单词
            num_words = random.randint(5, 10)
            words = [random.choice(sample_words) for _ in range(num_words)]
            f.write(" ".join(words) + "\n")
    
    print(f"创建示例文件: {path} ({num_lines} 行)")


def pipeline1_wordcount_with_cache():
    """
    Pipeline 1: 词频统计（窄依赖 + 宽依赖 + Cache）
    
    数据流：
    textFile -> flatMap(split) -> map(word,1) -> filter -> reduceByKey -> cache -> count + collect
    
    关键点：
    1. flatMap, map, filter 都是窄依赖
    2. reduceByKey 是宽依赖，触发 shuffle
    3. cache 后第二次 action 应该命中缓存
    """
    print("\n")
    print("=" * 70)
    print("  Pipeline 1: 词频统计（窄依赖 + 宽依赖 + Cache）")
    print("=" * 70)
    
    # 创建示例文件
    sample_file = "/tmp/minispark_sample.txt"
    create_sample_text_file(sample_file, num_lines=50)
    
    # 创建 SparkContext
    sc = SparkContext(
        app_name="WordCount",
        num_executors=4,
        verbose=True
    )
    
    try:
        # ==================== 构建 RDD DAG ====================
        print("\n" + "="*60)
        print("  构建 RDD DAG (惰性，不触发计算)")
        print("="*60)
        
        # 1. 从文件读取
        lines = sc.textFile(sample_file, num_partitions=4)
        print(f"创建: {lines}")
        
        # 2. flatMap: 分割单词（窄依赖）
        words = lines.flatMap(lambda line: line.split())
        print(f"创建: {words}")
        
        # 3. map: 转换为 (word, 1)（窄依赖）
        pairs = words.map(lambda word: (word, 1))
        print(f"创建: {pairs}")
        
        # 4. filter: 过滤掉长度小于 4 的单词（窄依赖）
        filtered = pairs.filter(lambda x: len(x[0]) >= 4)
        print(f"创建: {filtered}")
        
        # 5. reduceByKey: 按 key 聚合（宽依赖 -> 触发 shuffle）
        counts = filtered.reduceByKey(lambda a, b: a + b)
        print(f"创建: {counts}")
        
        # 6. cache: 缓存结果
        counts.cache()
        print(f"标记缓存: {counts}")
        
        print("\n>>> 注意：以上操作都是惰性的，RDD DAG 已构建但未执行计算 <<<")
        
        # ==================== 第一次 Action: count ====================
        print("\n" + "="*60)
        print("  第一次 Action: count()")
        print("  预期: 触发完整计算链，包含 shuffle")
        print("="*60)
        
        start_time = time.time()
        word_count = counts.count()
        elapsed1 = time.time() - start_time
        
        print(f"\n>>> 结果: 共有 {word_count} 个不同的单词（长度>=4）")
        print(f">>> 执行时间: {elapsed1*1000:.2f} ms")
        
        # ==================== 第二次 Action: collect ====================
        print("\n" + "="*60)
        print("  第二次 Action: collect()")
        print("  预期: 命中 cache，减少计算量")
        print("="*60)
        
        start_time = time.time()
        all_counts = counts.collect()
        elapsed2 = time.time() - start_time
        
        # 排序并显示前 10 个
        sorted_counts = sorted(all_counts, key=lambda x: x[1], reverse=True)[:10]
        
        print(f"\n>>> Top 10 词频:")
        for word, count in sorted_counts:
            print(f"    {word}: {count}")
        
        print(f"\n>>> 执行时间: {elapsed2*1000:.2f} ms")
        print(f">>> 注意: 第二次 action 应该更快（cache 命中）")
        
        # ==================== 解释 Stage 切分 ====================
        print("\n" + "="*60)
        print("  Stage 切分解释")
        print("="*60)
        print("""
这个 pipeline 会被切分为 2 个 Stage:

Stage 1 (ShuffleMapStage):
  - 包含: textFile -> flatMap -> map -> filter
  - 这些都是窄依赖，可以在同一个 Stage 内流水线执行
  - 输出: shuffle write（按 key 的 hash 分区写入）

   [SHUFFLE BOUNDARY: reduceByKey 触发]

Stage 2 (ResultStage):
  - 包含: shuffle read -> reduceByKey 的 reduce 端 -> collect/count
  - 从 shuffle 文件读取数据，按 key 聚合

为什么这样切分？
  - 宽依赖（reduceByKey）需要所有分区的数据按 key 重新分布
  - 相同 key 的数据必须聚集到同一个 reduce 分区
  - 这就需要 shuffle：map 端按 key 的 hash 写出，reduce 端读取合并
""")
        
    finally:
        sc.stop()
        # 清理临时文件
        if os.path.exists(sample_file):
            os.remove(sample_file)


def pipeline2_fault_tolerance():
    """
    Pipeline 2: 容错重算演示（Lineage）
    
    模拟 Executor 故障，展示通过 lineage 重算恢复。
    """
    print("\n")
    print("=" * 70)
    print("  Pipeline 2: 容错重算（Lineage）")
    print("=" * 70)
    
    # 创建 SparkContext，设置故障率
    sc = SparkContext(
        app_name="FaultTolerance",
        num_executors=4,
        failure_rate=0.3,  # 30% 的任务会失败
        verbose=True
    )
    
    try:
        print("\n" + "="*60)
        print("  设置: 30% 的任务会随机失败")
        print("  预期: 系统自动重试并通过 lineage 重算")
        print("="*60)
        
        # 创建简单 pipeline
        data = list(range(1, 21))  # 1 到 20
        
        rdd = sc.parallelize(data, num_partitions=4)
        
        # 窄依赖链
        mapped = rdd.map(lambda x: x * 2)
        filtered = mapped.filter(lambda x: x > 10)
        result_rdd = filtered.map(lambda x: x + 1)
        
        # 打印 DAG
        print_rdd_dag(result_rdd, "Pipeline 2 DAG")
        
        # 执行 action
        print("\n>>> 执行 collect()，部分任务可能失败...")
        
        try:
            result = result_rdd.collect()
            print(f"\n>>> 成功收集 {len(result)} 条记录")
            print(f">>> 结果: {result}")
        except RuntimeError as e:
            print(f"\n>>> 任务彻底失败: {e}")
            print(">>> 这表示重试次数用尽（默认 3 次）")
        
        # ==================== 解释 Lineage 重算 ====================
        print("\n" + "="*60)
        print("  Lineage 重算解释")
        print("="*60)
        print("""
当任务失败时，MiniSpark 的处理流程：

1. Executor 执行任务时抛出异常
2. Scheduler 捕获失败，检查重试次数
3. 如果未超过最大重试次数（默认 3）：
   - 记录 TASK_FAILED 和 LINEAGE_RECOMPUTE 事件
   - 重新调度该任务到（可能不同的）Executor
   - 任务从该分区的源头重新计算
4. 因为 RDD 记录了完整的 lineage（血统），所以可以从源头重算
5. 如果该分区已被 cache，则直接从 cache 读取（跳过重算）

Lineage 是 Spark 容错的核心：
  - RDD 是不可变的，可以安全地重算
  - 每个 RDD 知道如何从父 RDD 计算得来
  - 无需检查点，只需重放计算即可恢复

在这个例子中：
  parallelize -> map(*2) -> filter(>10) -> map(+1)
  
如果 filter 阶段的某个分区失败：
  1. 重新从 parallelize 获取该分区数据
  2. 应用 map(*2)
  3. 应用 filter(>10)
  4. 继续后续计算
""")
        
    finally:
        sc.stop()


def pipeline3_join_example():
    """
    Pipeline 3: Join 操作演示（多个 shuffle）
    """
    print("\n")
    print("=" * 70)
    print("  Pipeline 3: Join 操作（多个 shuffle）")
    print("=" * 70)
    
    sc = SparkContext(
        app_name="JoinExample",
        num_executors=4,
        verbose=True
    )
    
    try:
        # 创建两个 RDD
        users = sc.parallelize([
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie"),
            (4, "David")
        ], num_partitions=2)
        
        orders = sc.parallelize([
            (1, "Order-A"),
            (1, "Order-B"),
            (2, "Order-C"),
            (3, "Order-D"),
            (3, "Order-E"),
            (3, "Order-F")
        ], num_partitions=2)
        
        # Join
        joined = users.join(orders)
        
        # 打印 DAG
        print_rdd_dag(joined, "Join Pipeline DAG")
        
        # 执行
        result = joined.collect()
        
        print(f"\n>>> Join 结果:")
        for user_id, (name, order) in result:
            print(f"    User {user_id} ({name}): {order}")
        
        # ==================== 解释 ====================
        print("\n" + "="*60)
        print("  Join Stage 切分解释")
        print("="*60)
        print("""
Join 操作涉及两个 RDD 的 shuffle：

Stage 1: 左侧 RDD (users) 的 ShuffleMap
  - 按 key 的 hash 重新分区
  - shuffle write 到 shuffle_left

Stage 2: 右侧 RDD (orders) 的 ShuffleMap  
  - 按 key 的 hash 重新分区
  - shuffle write 到 shuffle_right

Stage 3: Result Stage
  - 从两个 shuffle 读取数据
  - 按 key 进行 join
  - 返回结果

为什么需要 shuffle？
  - Join 需要相同 key 的数据在同一个分区
  - 左右两个 RDD 的数据分布可能不同
  - shuffle 确保相同 key 聚集到一起
""")
        
    finally:
        sc.stop()


def main():
    """主函数"""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                     MiniSpark 演示程序                           ║
║                                                                  ║
║  这个演示展示 Spark 的核心概念：                                   ║
║  - RDD DAG 构建（惰性求值）                                       ║
║  - Action 触发 Job                                               ║
║  - Job 切分为 Stage（shuffle 边界）                               ║
║  - Stage 内 Task 并行执行                                         ║
║  - Shuffle 写/读                                                  ║
║  - Cache 机制                                                     ║
║  - Lineage 容错重算                                               ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    # Pipeline 1: WordCount with Cache
    pipeline1_wordcount_with_cache()
    
    print("\n" + "="*70)
    print("  按 Enter 继续执行 Pipeline 2 (容错重算)...")
    print("="*70)
    input()
    
    # Pipeline 2: Fault Tolerance
    pipeline2_fault_tolerance()
    
    print("\n" + "="*70)
    print("  按 Enter 继续执行 Pipeline 3 (Join)...")
    print("="*70)
    input()
    
    # Pipeline 3: Join
    pipeline3_join_example()
    
    print("\n" + "="*70)
    print("  演示完成！")
    print("="*70)


if __name__ == "__main__":
    main()
