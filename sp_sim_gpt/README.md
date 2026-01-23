# MiniSpark (sp_sim_gpt)

这是一个“概念验证级别”的 MiniSpark，用于教学演示 Spark 的核心概念。项目完全由 GPT 生成提示词，并由 Codex 实现，不依赖真实 Spark/Hadoop。运行方式：

```bash
python demo.py
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
