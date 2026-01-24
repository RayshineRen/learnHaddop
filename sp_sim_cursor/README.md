# MiniSpark - 教学用 Spark 模拟框架

MiniSpark 是一个纯 Python 实现的 Apache Spark 核心抽象模拟器，专为教学目的设计。它不依赖真实的 Spark/JVM/集群，但尽可能复刻 Spark 的核心概念和执行流程。

## 设计目标

**可观测 + 可解释**：每次执行 Action，都输出清晰的日志与 ASCII 执行图，帮助理解从 DAG 到物理执行（Job/Stage/Task）的完整过程。

## 核心概念

### 1. RDD (Resilient Distributed Dataset)
- 不可变的分布式数据集
- 支持 Transformation（惰性）和 Action（触发执行）
- 记录血统（Lineage）支持容错重算

### 2. DAG 调度
- Action 触发 Job
- Job 按 shuffle 边界切分为 Stage
- Stage 内 Task 按分区并行执行

### 3. Shuffle
- 宽依赖触发 shuffle
- Map 端写入，Reduce 端读取
- 相同 key 的数据聚集到同一分区

### 4. Cache/Persist
- 缓存 RDD 计算结果
- 后续使用可直接读取，避免重算

### 5. 容错
- 通过 Lineage 重算失败的分区
- 支持任务重试

## 项目结构

```
sp_sim_cursor/
├── minispark/
│   ├── __init__.py      # 包初始化和导出
│   ├── rdd.py           # RDD 核心实现
│   ├── scheduler.py     # DAG 调度器
│   ├── cluster.py       # ClusterManager 和 Executor
│   ├── shuffle.py       # Shuffle 管理器
│   ├── logger.py        # 结构化日志系统
│   ├── dag.py           # DAG 可视化
│   └── context.py       # SparkContext
├── demo.py              # 演示程序
└── README.md
```

## 功能清单

### RDD Transformations（惰性）
| 操作 | 依赖类型 | 说明 |
|------|---------|------|
| `map(f)` | 窄依赖 | 对每个元素应用函数 |
| `filter(f)` | 窄依赖 | 过滤元素 |
| `flatMap(f)` | 窄依赖 | 映射并展平 |
| `mapPartitions(f)` | 窄依赖 | 分区级别映射 |
| `union(other)` | 窄依赖 | 合并两个 RDD |
| `keyBy(f)` | 窄依赖 | 生成 key-value 对 |
| `reduceByKey(f)` | **宽依赖** | 按 key 聚合（触发 shuffle） |
| `groupByKey()` | **宽依赖** | 按 key 分组（触发 shuffle） |
| `join(other)` | **宽依赖** | 连接两个 RDD（触发 shuffle） |

### RDD Actions（触发执行）
| 操作 | 说明 |
|------|------|
| `collect()` | 收集所有数据到 Driver |
| `count()` | 计算元素总数 |
| `take(n)` | 获取前 n 个元素 |
| `first()` | 获取第一个元素 |
| `reduce(f)` | 聚合所有元素 |
| `saveAsTextFile(path)` | 保存为文本文件 |

### 持久化
| 操作 | 说明 |
|------|------|
| `cache()` | 缓存 RDD（等同于 persist） |
| `persist()` | 持久化 RDD |
| `unpersist()` | 取消持久化 |

## 日志事件

结构化日志包含以下事件类型：

| 事件 | 说明 |
|------|------|
| `DAG_BUILT` | DAG 构建完成 |
| `ACTION_CALLED` | Action 被调用 |
| `JOB_SUBMITTED` | Job 提交 |
| `STAGE_PLANNED` | Stage 规划 |
| `STAGE_START/END` | Stage 开始/结束 |
| `TASK_START/END` | Task 开始/结束 |
| `SHUFFLE_WRITE` | Shuffle 写入 |
| `SHUFFLE_READ` | Shuffle 读取 |
| `CACHE_PUT` | 写入缓存 |
| `CACHE_HIT` | 缓存命中 |
| `LINEAGE_RECOMPUTE` | Lineage 重算 |
| `TASK_FAILED/RETRY` | 任务失败/重试 |

## 快速开始

```python
from minispark import SparkContext

# 创建 SparkContext
sc = SparkContext("MyApp", num_executors=4)

# 创建 RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Transformations（惰性，不触发计算）
result = rdd.map(lambda x: x * 2).filter(lambda x: x > 5)

# Action（触发执行）
print(result.collect())  # [6, 8, 10]

sc.stop()
```

## 运行演示

```bash
cd sp_sim_cursor
python demo.py
```

演示包含三个 Pipeline：
1. **词频统计**：展示窄依赖 + 宽依赖 + cache
2. **容错重算**：展示 Lineage 机制
3. **Join 操作**：展示多 shuffle 场景

## 示例输出

### 逻辑 DAG（血统图）

```
============================================================
  逻辑 DAG (执行 count 前)
============================================================

RDD[6] reduceByKey(filter(map(flatMap(textFile(sample.txt)))))
  (partitions=4)
  |
  | (wide: reduceByKey)
  | <== SHUFFLE BOUNDARY (shuffle_1)
  |
RDD[5] filter(map(flatMap(textFile(sample.txt))))
  (partitions=4)
  |
  | (narrow: filter)
  |
RDD[4] map(flatMap(textFile(sample.txt)))
  (partitions=4)
  |
  | (narrow: map)
  |
RDD[3] flatMap(textFile(sample.txt))
  (partitions=4)
  |
  | (narrow: flatMap)
  |
RDD[2] textFile(sample.txt)
  (partitions=4)

============================================================
```

### 物理执行计划

```
============================================================
  物理执行计划 - Job 1 (action=count)
============================================================

  STAGE 1 (ShuffleMap, RDD[5])
  --------------------------------------------------
    任务: T1[p0]  T2[p1]  T3[p2]  T4[p3]
    分区数: 4
    Shuffle: shuffle_1
         \
          --> SHUFFLE WRITE
         |
    SHUFFLE READ
         |
         v

  STAGE 2 (Result, RDD[6])
  --------------------------------------------------
    任务: T5[p0]  T6[p1]  T7[p2]  T8[p3]
    分区数: 4

  RESULT -> Driver
============================================================
```

### 执行日志

```
🎯 event=ACTION_CALLED | job_id=1 | rdd_id=6 | note=Action 'count' 被调用，触发 Job 1
📋 event=JOB_SUBMITTED | job_id=1 | note=Job 1 已提交，包含 2 个 Stage
▶️  event=STAGE_START | job_id=1 | stage_id=1 | note=Stage 1 开始执行，4 个分区
⚙️  event=TASK_START | executor_id=executor-1 | task_id=1 | partition_id=0
📤 event=SHUFFLE_WRITE | shuffle_id=1 | task_id=0 | output_records=42
✔️  event=TASK_END | task_id=1 | duration_ms=12.34 | input_records=50 | output_records=42
...
📥 event=SHUFFLE_READ | shuffle_id=1 | partition_id=0 | input_records=38
💾 event=CACHE_PUT | rdd_id=6 | partition_id=0 | output_records=12
✅ event=STAGE_END | stage_id=2 | note=Stage 2 执行完成
```

### 执行摘要

```
============================================================
  JOB 1 执行摘要
============================================================
  Stage 数量:        2
  Task 总数:         8
  Shuffle 操作:      8 (写: 4, 读: 4)
  Cache 命中:        0
  Cache 写入:        4
  Lineage 重算:      0
  Task 失败/重试:    0
  总输入记录:        242
  总输出记录:        18
  总执行时间:        156.78 ms
============================================================
```

## Stage 切分策略

MiniSpark 的 Stage 切分策略与真实 Spark 一致：

1. **遇到宽依赖就切分**：宽依赖（如 reduceByKey、groupByKey、join）需要 shuffle，产生 Stage 边界
2. **窄依赖流水线执行**：窄依赖（如 map、filter、flatMap）可以在同一 Stage 内串联执行
3. **Stage 顺序执行**：父 Stage 必须在子 Stage 之前完成
4. **Task 并行执行**：同一 Stage 内的 Task 可以并行执行

## 依赖

- Python 3.7+
- 纯标准库（无第三方依赖）

使用的标准库：
- `dataclasses`: 数据类定义
- `typing`: 类型注解
- `concurrent.futures`: 线程池（模拟 Executor）
- `threading`: 线程同步
- `json`: 序列化
- `uuid`: 唯一标识
- `time`: 计时
- `os`: 文件操作
- `enum`: 枚举类型

## 架构说明

### Driver/Executor 模拟

```
+------------------+          +-------------------+
|     Driver       |          |  ClusterManager   |
|                  |  submit  |                   |
| - SparkContext   |--------->| - Executor Pool   |
| - DAGScheduler   |          | - Task Scheduling |
| - 用户代码        |<---------|                   |
+------------------+  result  +-------------------+
                                     |
                                     | dispatch
                                     v
                    +-------+-------+-------+-------+
                    |  E0   |  E1   |  E2   |  E3   |
                    +-------+-------+-------+-------+
                         Executor Threads
```

- **Driver**: 运行用户代码，协调调度
- **ClusterManager**: 管理 Executor 生命周期
- **Executor**: 执行具体任务（用线程模拟）

### Shuffle 流程

```
Stage 1 (Map Side)              Stage 2 (Reduce Side)
+--------+                      +--------+
| Task 0 |--\                /--| Task 0 |
+--------+   \   Shuffle    /   +--------+
+--------+    \  Files     /    +--------+
| Task 1 |-----+=========+------| Task 1 |
+--------+    /           \     +--------+
+--------+   /             \    +--------+
| Task 2 |--/               \---| Task 2 |
+--------+                      +--------+
```

每个 Map Task 按 key 的 hash 将数据写入对应的 Reduce 分区文件。
每个 Reduce Task 读取所有 Map Task 为其分区写入的数据。

## 扩展阅读

- [Apache Spark 官方文档](https://spark.apache.org/docs/latest/)
- 《Spark 快速大数据分析》
- [RDD 论文](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
