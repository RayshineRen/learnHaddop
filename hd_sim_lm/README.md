# hd_sim_lm

本目录包含用 Notebook LM 生成提示词、并由 Codex 逐阶段实现的 Hadoop 生态模拟项目代码。

## 快速开始

```bash
# 运行端到端演示（推荐入门）
python3 end_to_end_demo.py

# 单独运行各模块测试
python3 hdfs_sim.py
python3 mapreduce_sim.py
python3 yarn_sim.py
```

## 文件说明

### 核心模拟模块
- `hdfs_sim.py`：模拟 HDFS 存储层（NameNode/DataNode、Block 切分、副本、缓存等）
- `mapreduce_sim.py`：模拟 MapReduce 计算层（Mapper/Reducer、Shuffle 的分区/排序/分组、网络传输）
- `yarn_sim.py`：模拟 YARN 资源管理层（ResourceManager/NodeManager/ApplicationMaster、FIFO/Fair 调度）

### 教学案例
- `hadoop_learning_case.md`：**完整的端到端 Hadoop 学习案例文档**（重要！）
  - 案例背景：电商订单日志分析
  - HDFS 视角：Block 切分、副本策略、数据本地性
  - MapReduce 实现：详细的 Mapper/Reducer 代码及 Shuffle 解析
  - YARN 运行视角：资源调度流程
  - 执行流程串讲：时间线 + 角色视角
  - 学习总结：常见误解和后续学习铺垫

- `end_to_end_demo.py`：**可运行的完整演示脚本**，整合 HDFS/MapReduce/YARN 三层模拟

### 测试数据
- `test.txt`：HDFS 模块测试用文本数据
- `sample_access_logs.txt`：电商访问日志示例数据（用于 PV/UV 统计案例）

以下为 Notebook LM 生成该项目的分阶段提示词（HDFS -> MR -> YARN）：

阶段一：构建模拟 HDFS 系统
目标：实现文件切块（Block）、副本管理（Replication）以及缓存机制。
Codex 提示词：
“请使用 Python 编写一个模拟 HDFS 的系统。要求：
1. 实现 NameNode 类：负责管理元数据（文件到数据块的映射、块所在位置），不存储真实数据。
2. 实现 DataNode 类：模拟磁盘存储，负责存储实际的数据块（Blocks）。
3. 核心功能：支持将一个大文件切分为固定大小的 Block（如模拟 128MB），并分散存储在多个 DataNode 实例中，每个 Block 需有 3 个副本。
4. 缓存机制：模拟 Distributed Cache 机制。在读取数据时，DataNode 能够将高频使用的 Block 放置在模拟的‘任务本地内存’中，以减少模拟的磁盘 I/O 开销。
5. 接口：提供 put(file_path) 和 get(file_path) 方法，支持存取数据。
6. 测试案例：上传一个包含大量文本的 test.txt 文件，并演示缓存命中与未命中的读取速度差异。”

--------------------------------------------------------------------------------
阶段二：构建 MapReduce 计算框架
目标：在 HDFS 基础上实现并行计算，重点体现 Shuffle 过程。
Codex 提示词：
“在已有的 HDFS 模拟器基础上，编写一个 Python MapReduce 框架。要求：
1. Mapper 接口：支持自定义 map(key, value) 函数，对本地数据块进行处理并输出中间键值对。
2. Shuffle 过程（核心）：
    ◦ 分区（Partitioning）：根据键（Key）将中间结果分配给不同的 Reducer。
    ◦ 排序与分组（Sort & Group）：在数据到达 Reducer 前，必须按键进行排序，并把相同键的所有值聚合成一个集合。
    ◦ 网络传输模拟：模拟数据从 Map 节点通过‘网络’拉取到 Reduce 节点的过程。
3. Reducer 接口：支持自定义 reduce(key, values) 函数进行全局聚合。
4. 提交机制：实现一个 submit_job(mapper, reducer, input_path) 方法。
5. 测试案例：
    ◦ WordCount：统计文本中单词出现的次数。
    ◦ PV/UV 统计：分析模拟的 100TB 访问日志，计算每个 URL 的访问频率。”

--------------------------------------------------------------------------------
阶段三：构建 YARN 资源管理器
目标：实现计算与资源的解耦，统一调度多个 MR 任务。
Codex 提示词：
“为上述系统添加一个 YARN 资源管理器模拟层。要求：
1. 实现 ResourceManager (RM)：负责整个集群资源的统一调度。RM 需维护一个队列，决定哪个作业先运行、分配多少资源。
2. 实现 NodeManager (NM)：运行在每个数据节点上，管理节点内部的资源（如模拟 CPU 核心数和内存），并向 RM 汇报心跳。
3. 实现 ApplicationMaster (AM)：为每个作业创建一个 AM，负责向 RM 申请资源（Container），并监控该作业内所有任务的执行进度。
4. 资源调度算法：支持简单的 FIFO（先来先服务） 或 公平调度（Fair Scheduler） 策略。
5. 综合测试案例：
    ◦ 多任务并发测试：同时提交 3 个不同的 WordCount 任务，观察 YARN 如何根据资源限制挂起（Pending）任务或并行执行（Running）任务。
    ◦ 长尾效应模拟：设计一个包含极慢 Map 任务的案例，观察 YARN 和 MR 框架如何反馈进度状态。”

总结提示
通过这种分层构建的方式，你可以清晰地理解：
• HDFS 解决了数据如何在“机器必然会坏”的现实中可靠活下来的问题。
• MapReduce 解决了大规模数据如何通过 Shuffle 规则完成分治与跨机聚合的问题。
• YARN 解决了多个框架和作业如何共享同一套底座资源而不互相冲突的问题。
