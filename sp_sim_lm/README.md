# MiniSpark (sp_sim_lm)

这是一个用 Python 模拟 Spark 核心机制的教学级 MiniSpark。该版本根据 Notebook LM 生成的提示词实现，并由 Codex 完成编码，专注演示 RDD、惰性执行、Driver/Executor 与容错。

## 运行方式

```bash
python demo.py
```

## 示例用法

```python
from minispark.runtime import MiniSparkContext

sc = MiniSparkContext()
input_rdd = sc.parallelize([15, 16, 17, 18, 19], num_partitions=2)
result_rdd = input_rdd.map(lambda x: x * 10).filter(lambda x: x > 25)
print(result_rdd.collect(sc.driver))
```

## 设计要点

- **RDD 不可变与分区**：`RDD` 保存 `num_partitions` 并通过 `ParallelizeRDD` 切分输入数据。
- **Lineage 血统**：每个 RDD 记录 `lineage` 字符串和依赖关系，故障时根据 lineage 重算。
- **惰性执行**：`map/filter` 仅构建 DAG，不会立即计算。
- **Action 驱动执行**：`collect/count` 触发 Driver → Scheduler → Executor 执行链。
- **内存计算**：Executor 持有 `cache`，模拟内存计算缓存分区结果。
- **DAG 优化展示**：Scheduler 输出 DAG 根 Stage 与 lineage 日志，便于教学说明。

## Notebook LM 提示词（由 Codex 实现）

Role: 你是一位顶级的分布式计算架构师，擅长用 Python 模拟复杂的大数据框架。
Task: 请编写一个名为 MiniSpark 的简化版计算框架，用以演示 Apache Spark 的核心运行机制。
Core Requirements (重点内容覆盖):
1. RDD 抽象 (Resilient Distributed Dataset):
    ◦ 创建一个 RDD 类，要求数据是不可变（Immutable）分区的（Partitioned）。
    ◦ 每个 RDD 必须能够记录其血统（Lineage），即指向父 RDD 的引用以及它所代表的转换操作。
2. 延迟计算机制 (Lazy Evaluation):
    ◦ 实现 Transformations（转换算子）：如 map 和 filter。这些操作不应立即触发计算，而只是将新的转换函数加入到 Lineage 图中，并返回一个新的 RDD。
3. 行动算子与执行引擎 (Actions & Execution):
    ◦ 实现 Actions（行动算子）：如 collect 和 count。
    ◦ 当 Action 被调用时，框架应触发底层的“执行引擎”，从结果 RDD 递归回溯 Lineage 图，依次在各分区上执行逻辑。
4. 架构模拟 (Driver & Executor):
    ◦ 模拟 Driver 程序：负责解析用户代码、构建逻辑 DAG。
    ◦ 模拟 Executor 节点：负责在数据分区上运行实际的任务（Task），并能够模拟内存计算以区别于 MapReduce 的落盘模式。
5. 容错演示 (Fault Tolerance):
    ◦ 展示当某个分区的数据“丢失”时，系统如何根据 Lineage 信息重新计算该分区，而不是依赖副本恢复。
