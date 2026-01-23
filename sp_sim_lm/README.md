# MiniSpark (sp_sim_lm)

这是一个“概念验证级别”的 MiniSpark，用于教学演示 Spark 的核心概念。项目由 Notebook LM 生成提示词，并由 Codex 实现，不依赖真实 Spark/Hadoop。运行方式：

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

## 模块结构

- `minispark/core.py`：RDD、依赖关系、DAG/Stage/Task、调度器与 Shuffle。
- `minispark/runtime.py`：Driver/Executor/Cluster 的最小运行时。
- `minispark/sql.py`：DataFrame、Schema、简单优化器（列裁剪/谓词下推）。
- `demo.py`：可运行的教学演示脚本。

## 关键概念对应

### RDD Lineage 表示

- 每个 RDD 记录 `lineage` 字符串，并保留父依赖（`Dependency`）。
- 当发生失败时，Scheduler 依据 lineage 和依赖链触发重算。实现见 `Scheduler.run` 与 `Driver.inject_failure`。 

### 宽/窄依赖建模

- `Dependency.dep_type` 取值为 `narrow` 或 `wide`。
- `reduceByKey`、`join` 会创建 `wide` 依赖，触发 shuffle 与 stage 切分。

### Action 才触发执行

- `map/filter/flatMap/reduceByKey/join` 仅构建 RDD DAG，不执行。
- `collect/count/take/saveAsTextFile` 触发 `Driver.run_job`，由 Scheduler 构建 DAG/Stage/Task 并调度执行。

### 内存计算与 DAG 优化

- Executor 在执行 Task 时将分区结果缓存到内存（`Executor.cache`），模拟内存计算优势。
- Scheduler 在 Action 时构建 DAG 并切分 Stage，展示宽依赖触发的 Shuffle 边界。

### DataFrame Schema/Optimizer

- Schema 由 `Field(name, dtype)` 列表构成。
- Optimizer 仅演示“列裁剪/谓词下推”：
  - `select` 会收缩列集合。
  - `where` 会在 RDD 上直接过滤。
- `DataFrame.optimized_plan()` 会输出优化前/后 Plan 的日志。

## 运行输出

运行 `python demo.py` 后可观察：

- DAG 结构与 Stage 切分
- Shuffle 路由（key -> partition）
- Task 分发到 Executor
- 注入失败后的 lineage 重算
- DataFrame 优化计划与执行输出

> 提示：该模拟框架强调“可读可演示”，并非性能实现。

## Notebook LM 提示词（由 Codex 实现）

你是一位顶级的分布式计算架构师，擅长用 Python 模拟复杂的大数据框架。
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
