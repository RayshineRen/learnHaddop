# hd_sim_cursor

Hadoop 端到端学习案例 - 电商订单日志分析

## 快速开始

```bash
# 运行端到端演示
python3 end_to_end_demo.py
```

## 文件说明

### 教学文档
- `hadoop_learning_case.md`：**完整的端到端 Hadoop 学习案例文档**
  - 案例背景：电商订单日志分析 PV/UV 统计
  - HDFS 视角：Block 切分、副本策略、数据本地性详解
  - MapReduce 实现：带教学注释的 Mapper/Reducer 代码
  - Shuffle 阶段详解：分区→排序→传输→分组
  - YARN 运行视角：资源调度、Container 封装
  - 完整时间线串讲：从作业提交到结果写回
  - 学习总结：常见误解和 Spark/Flink 铺垫

### 演示脚本
- `end_to_end_demo.py`：**可运行的完整演示脚本**
  - 整合 HDFS、MapReduce、YARN 三层模拟
  - 实现电商日志 PV/UV 统计案例
  - 详细的执行过程输出和结果展示

### 测试数据
- `sample_access_logs.txt`：电商访问日志示例数据

## 案例概述

**业务场景**：电商平台每天产生海量访问日志，需要分析：
- 每个商品页面的访问量（PV, Page View）
- 每个商品页面的独立访客数（UV, Unique Visitor）

**技术栈**：
- HDFS：分布式存储（Block 切分 + 多副本）
- MapReduce：分布式计算（Map → Shuffle → Reduce）
- YARN：资源调度（ResourceManager + NodeManager + Container）

## 学习目标

通过本案例，你将理解：

1. **HDFS 层面**
   - 文件如何被切分成 Block
   - Block 如何分布存储到多个 DataNode
   - 副本策略如何保证数据可靠性

2. **MapReduce 层面**
   - Map 阶段：每个 Block 对应一个 Map Task
   - Shuffle 阶段：分区→排序→传输→分组
   - Reduce 阶段：相同 Key 的 Values 被聚合处理

3. **YARN 层面**
   - ResourceManager：全局资源调度
   - NodeManager：节点资源管理
   - ApplicationMaster：作业生命周期管理
   - Container：资源分配的最小单位
