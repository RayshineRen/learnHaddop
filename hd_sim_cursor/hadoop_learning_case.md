# Hadoop 端到端学习案例：电商订单日志分析

## 目录

1. [案例背景](#一案例背景)
2. [HDFS 视角](#二hdfs-视角)
3. [MapReduce 实现](#三mapreduce-实现重点)
4. [YARN 运行视角](#四yarn-运行视角)
5. [执行流程串讲](#五执行流程串讲时间线--角色视角)
6. [结果与验证](#六结果与验证)
7. [学习总结](#七学习总结)

---

## 一、案例背景

### 1.1 业务场景

**场景**：某电商平台每天产生海量的用户访问日志，运营团队需要分析以下指标：
- 每个商品页面的访问量（PV, Page View）
- 每个商品页面的独立访客数（UV, Unique Visitor）
- 热门商品排行榜

### 1.2 数据格式

原始日志文件存储在 `/data/input/access_logs/` 目录，每行格式如下：

```
时间戳 | 用户ID | 商品页面URL | 访问来源 | 停留时长(秒)
```

**示例数据**：
```
2024-01-20 10:23:45 | user_001 | /product/phone_iphone15 | search | 120
2024-01-20 10:23:46 | user_002 | /product/laptop_macbook | direct | 85
2024-01-20 10:23:47 | user_001 | /product/phone_iphone15 | search | 30
2024-01-20 10:23:48 | user_003 | /product/phone_iphone15 | ad | 200
2024-01-20 10:23:49 | user_002 | /product/tablet_ipad | direct | 150
```

### 1.3 为什么适合用 Hadoop？

| 特征 | 单机脚本 | Hadoop |
|------|----------|--------|
| 数据量 | < 10GB | TB/PB 级别 |
| 处理速度 | 受限于单机 CPU/内存 | 多节点并行处理 |
| 容错性 | 程序崩溃需重跑 | 自动重试失败任务 |
| 可扩展性 | 需要更换更强的机器 | 加节点即可扩展 |

**本案例适合 Hadoop 的原因**：
1. **数据量大**：假设每天产生 100TB 访问日志，单机内存无法容纳
2. **计算可并行**：每条日志的解析互不依赖，天然适合分治
3. **需要容错**：7×24 小时运营，不能因单点故障而丢失分析结果

---

## 二、HDFS 视角

### 2.1 数据如何被切分成 Block

```
原始文件: access_logs_20240120.txt (384MB)
                    │
                    ▼
    ┌───────────────────────────────────────┐
    │           HDFS Block 切分              │
    │    (默认 Block Size = 128MB)           │
    └───────────────────────────────────────┘
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
    Block_0      Block_1      Block_2
    (128MB)      (128MB)      (128MB)
    行 1-2500    行 2501-5000  行 5001-7500
```

**关键概念**：
- **Block 是物理切分**：HDFS 按字节偏移量切分，不考虑行边界
- **InputFormat 处理边界问题**：`TextInputFormat` 会确保每个 Map Task 读取完整的行
- 本模拟中使用 `block_size=512` 字节以便演示

### 2.2 Block 与 Map Task 的关系

```
┌─────────────────────────────────────────────────────────────────┐
│                    InputFormat 的职责                           │
│                                                                 │
│   access_logs.txt                                               │
│        │                                                        │
│        ▼                                                        │
│   ┌─────────┐    ┌─────────┐    ┌─────────┐                    │
│   │ Block_0 │    │ Block_1 │    │ Block_2 │                    │
│   │ (DN-1)  │    │ (DN-2)  │    │ (DN-3)  │  ← 物理存储位置    │
│   └────┬────┘    └────┬────┘    └────┬────┘                    │
│        │              │              │                          │
│        ▼              ▼              ▼                          │
│   ┌─────────┐    ┌─────────┐    ┌─────────┐                    │
│   │InputSplit│    │InputSplit│    │InputSplit│ ← 逻辑切片       │
│   │    0    │    │    1    │    │    2    │                    │
│   └────┬────┘    └────┬────┘    └────┬────┘                    │
│        │              │              │                          │
│        ▼              ▼              ▼                          │
│   ┌─────────┐    ┌─────────┐    ┌─────────┐                    │
│   │ Map     │    │ Map     │    │ Map     │  ← Map Task        │
│   │ Task 0  │    │ Task 1  │    │ Task 2  │                    │
│   └─────────┘    └─────────┘    └─────────┘                    │
└─────────────────────────────────────────────────────────────────┘
```

**核心规则**：
- **1 个 InputSplit ≈ 1 个 Block**（默认情况）
- **1 个 InputSplit → 1 个 Map Task**
- Map Task 数量 = ceil(文件大小 / Block大小)

### 2.3 数据本地性（Data Locality）

```
┌────────────────────────────────────────────────────────────────┐
│                     数据本地性调度                              │
│                                                                │
│   场景：Block_0 存储在 DN-1, DN-2, DN-3（3副本）              │
│                                                                │
│   调度优先级（从高到低）：                                      │
│                                                                │
│   1. Node-Local（节点本地）                                    │
│      └─ Map Task 0 调度到 DN-1 ✓                               │
│         （直接从本地磁盘读取，无网络开销）                       │
│                                                                │
│   2. Rack-Local（机架本地）                                    │
│      └─ 如果 DN-1 忙，调度到同机架的其他节点                    │
│         （跨机内网络，延迟较低）                                │
│                                                                │
│   3. Off-Rack（跨机架）                                        │
│      └─ 最后选择，需要跨机架网络传输                           │
│         （带宽受限，尽量避免）                                  │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

**本案例体现**：
```python
# 在模拟中，我们为每个 Block 记录其所在 DataNode
# YARN 调度时会优先选择数据所在的 NodeManager
datanode_ids = self.namenode.get_block_locations(block.block_id)
# 返回: ["dn-1", "dn-2", "dn-3"]
# 调度器会优先在这些节点上启动 Map Task
```

---

## 三、MapReduce 实现（重点）

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         MapReduce 作业执行流程                           │
│                                                                         │
│  Input          Map           Shuffle          Reduce        Output     │
│  ─────          ───           ───────          ──────        ──────     │
│                                                                         │
│  ┌─────┐      ┌─────┐                                                   │
│  │Block│ ───▶ │ Map │ ──┐                                               │
│  │  0  │      │Task0│   │     ┌──────────┐     ┌────────┐               │
│  └─────┘      └─────┘   ├────▶│ Partition│────▶│Reduce  │    ┌──────┐  │
│                         │     │ 0 (a-m)  │     │Task 0  │───▶│Output│  │
│  ┌─────┐      ┌─────┐   │     └──────────┘     └────────┘    │Part 0│  │
│  │Block│ ───▶ │ Map │ ──┤                                    └──────┘  │
│  │  1  │      │Task1│   │     ┌──────────┐     ┌────────┐               │
│  └─────┘      └─────┘   ├────▶│ Partition│────▶│Reduce  │    ┌──────┐  │
│                         │     │ 1 (n-z)  │     │Task 1  │───▶│Output│  │
│  ┌─────┐      ┌─────┐   │     └──────────┘     └────────┘    │Part 1│  │
│  │Block│ ───▶ │ Map │ ──┘                                    └──────┘  │
│  │  2  │      │Task2│                                                   │
│  └─────┘      └─────┘                                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Mapper 实现（带教学注释）

```python
"""
===========================================
Mapper 函数详解：从日志行到 (key, value) 对
===========================================

【输入】
- key:   行偏移量（在本模拟中为 block_id）
- value: 日志行的原始字节内容

【输出】
- 一系列 (key, value) 对
- key:   商品页面 URL（例如 /product/phone_iphone15）
- value: 用户 ID（例如 user_001）

【分布式语义】
- 每个 Map Task 独立处理一个 Block 的数据
- 多个 Map Task 在不同节点上并行执行
- Map Task 之间完全无通信，这是并行化的关键
"""

import re
from typing import Iterable, Tuple

KeyValue = Tuple[str, str]

# 日志行解析正则表达式
# 格式: 时间戳 | 用户ID | 商品URL | 来源 | 停留时长
LOG_PATTERN = re.compile(
    r"^(?P<timestamp>[\d\-\s:]+)\s*\|\s*"     # 时间戳
    r"(?P<user_id>\S+)\s*\|\s*"               # 用户ID
    r"(?P<url>\S+)\s*\|\s*"                   # 商品URL
    r"(?P<source>\S+)\s*\|\s*"                # 来源
    r"(?P<duration>\d+)$"                     # 停留时长
)

def pv_uv_mapper(block_id: str, data: bytes) -> Iterable[KeyValue]:
    """
    Map 阶段：将日志行转换为 (URL, UserID) 键值对
    
    【教学重点】
    1. 输入是原始字节流，需要解码为字符串
    2. 每行独立解析，不依赖上下文
    3. 输出的 key 决定了数据会被发送到哪个 Reducer
    """
    # 【步骤1】将字节流解码为文本
    text = data.decode("utf-8")
    
    # 【步骤2】按行遍历
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
            
        # 【步骤3】解析日志格式
        match = LOG_PATTERN.match(line)
        if not match:
            # 遇到格式错误的行，可以记录 Counter 或跳过
            continue
        
        url = match.group("url")       # 商品页面URL
        user_id = match.group("user_id")  # 用户ID
        
        # 【步骤4】输出键值对
        # Key = URL，用于后续按页面聚合
        # Value = 用户ID，用于计算 UV（去重）
        yield url, user_id
        
        # 【分布式语义】
        # 此时这条记录尚未"发送"到任何地方
        # 它会被写入 Mapper 的本地缓冲区
        # 等待 Shuffle 阶段进行分区和传输
```

### 3.3 Shuffle 阶段详解（逻辑层面）

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Shuffle 阶段详解                                │
│                                                                         │
│  【Map 端 Shuffle】                                                     │
│  ────────────────                                                       │
│                                                                         │
│  Map Task 0 的输出缓冲区:                                               │
│  ┌────────────────────────────────────────┐                            │
│  │ (/product/phone, user_001)             │                            │
│  │ (/product/laptop, user_002)            │                            │
│  │ (/product/phone, user_003)             │                            │
│  └────────────────────────────────────────┘                            │
│                    │                                                    │
│                    ▼                                                    │
│  ┌────────────────────────────────────────┐                            │
│  │         1. 分区（Partitioning）         │                            │
│  │    hash("/product/phone") % 2 = 0      │                            │
│  │    hash("/product/laptop") % 2 = 1     │                            │
│  └────────────────────────────────────────┘                            │
│                    │                                                    │
│                    ▼                                                    │
│  ┌────────────────────────────────────────┐                            │
│  │         2. 排序（Sort）                 │                            │
│  │    按 Key 字典序排序                    │                            │
│  │    /product/laptop < /product/phone    │                            │
│  └────────────────────────────────────────┘                            │
│                    │                                                    │
│                    ▼                                                    │
│  ┌────────────────────────────────────────┐                            │
│  │     3. 溢写到本地磁盘（Spill）           │                            │
│  │    当内存缓冲区达到阈值时写入磁盘         │                            │
│  └────────────────────────────────────────┘                            │
│                                                                         │
│  ════════════════════════════════════════════════════════════════════  │
│                                                                         │
│  【Reduce 端 Shuffle】                                                  │
│  ─────────────────                                                      │
│                                                                         │
│  Reducer 0 从所有 Mapper 拉取属于自己分区的数据:                         │
│                                                                         │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐                               │
│  │ Map 0   │   │ Map 1   │   │ Map 2   │                               │
│  │ Part 0  │   │ Part 0  │   │ Part 0  │                               │
│  └────┬────┘   └────┬────┘   └────┬────┘                               │
│       │             │             │                                     │
│       └─────────────┼─────────────┘                                     │
│                     │                                                   │
│                     ▼ 4. 网络传输（Fetch）                              │
│                                                                         │
│  ┌────────────────────────────────────────┐                            │
│  │         5. 合并排序（Merge Sort）        │                            │
│  │    将来自不同 Mapper 的数据合并          │                            │
│  │    保持全局有序                         │                            │
│  └────────────────────────────────────────┘                            │
│                     │                                                   │
│                     ▼                                                   │
│  ┌────────────────────────────────────────┐                            │
│  │         6. 分组（Grouping）             │                            │
│  │    相同 Key 的 Values 聚合在一起        │                            │
│  │    /product/phone → [user_001,         │                            │
│  │                      user_003,         │                            │
│  │                      user_001]         │                            │
│  └────────────────────────────────────────┘                            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**代码实现**：

```python
def default_partitioner(key: str, num_reducers: int) -> int:
    """
    分区函数：决定 key 发送到哪个 Reducer
    
    【教学重点】
    - hash(key) % num_reducers 确保相同 key 一定去同一个 Reducer
    - 这是 MapReduce 正确性的基础：相同 key 必须被同一个 Reducer 处理
    - 分区不均匀会导致数据倾斜（Data Skew）
    """
    return hash(key) % num_reducers

def shuffle_phase(partitions, num_reducers):
    """
    Shuffle 阶段的核心逻辑
    """
    reducer_inputs = {}
    
    for partition_id in range(num_reducers):
        pairs = partitions.get(partition_id, [])
        
        # 【模拟网络传输】
        # 真实环境中，数据从 Map 节点传输到 Reduce 节点
        # 这是 MapReduce 中网络 I/O 最密集的阶段
        simulate_network_transfer(pairs)
        
        # 【排序 + 分组】
        # 1. 按 key 排序
        sorted_pairs = sorted(pairs, key=lambda x: x[0])
        
        # 2. 相同 key 的 values 聚合
        grouped = defaultdict(list)
        for key, value in sorted_pairs:
            grouped[key].append(value)
            
        reducer_inputs[partition_id] = grouped
    
    return reducer_inputs
```

### 3.4 Reducer 实现（带教学注释）

```python
from typing import Dict, Sequence, Iterable, Tuple

def pv_uv_reducer(url: str, user_ids: Sequence[str]) -> Iterable[Tuple[str, Dict[str, int]]]:
    """
    Reduce 阶段：计算每个 URL 的 PV 和 UV
    
    【输入】
    - url:      商品页面 URL（Shuffle 阶段分组的 Key）
    - user_ids: 该 URL 的所有访问用户 ID 列表（Shuffle 聚合的 Values）
    
    【输出】
    - (url, {"pv": 总访问次数, "uv": 独立访客数})
    
    【分布式语义】
    - 一个 Reducer 可能处理多个 key
    - 但同一个 key 的所有 values 一定在同一个 Reducer 中
    - 这保证了聚合的正确性
    """
    
    # 【PV 计算】
    # PV = Page View = 总访问次数
    # 直接统计 user_ids 列表长度
    pv = len(user_ids)
    
    # 【UV 计算】
    # UV = Unique Visitor = 独立访客数
    # 需要对用户 ID 去重
    uv = len(set(user_ids))
    
    # 【输出结果】
    yield url, {"pv": pv, "uv": uv}
    
    # 【教学说明】
    # 为什么 Reducer 能正确计算 UV？
    # 因为 Shuffle 保证了：所有访问同一 URL 的记录
    # 一定会被发送到同一个 Reducer
    # 所以这里的 set() 去重是全局正确的
```

### 3.5 完整 MapReduce 作业提交

```python
def submit_job(self, mapper, reducer, input_path):
    """
    提交 MapReduce 作业
    
    【执行步骤】
    1. 从 HDFS 读取输入文件的所有 Block
    2. 为每个 Block 执行 Mapper
    3. Shuffle：分区、排序、网络传输、分组
    4. 为每个分区执行 Reducer
    5. 返回最终结果
    """
    
    # ========== MAP 阶段 ==========
    # 从 HDFS 获取所有数据块
    blocks = self.hdfs.get_blocks(input_path)
    
    # 用于存储 Map 输出的分区数据
    partitions = defaultdict(list)
    
    # 【并行执行】在真实环境中，这些 Map Task 会并行运行
    for block_id, data in blocks:
        # 调用用户定义的 Mapper 函数
        for key, value in mapper(block_id, data):
            # 分区：决定这条记录发送到哪个 Reducer
            partition_id = default_partitioner(key, self.num_reducers)
            partitions[partition_id].append((key, value))
    
    # ========== SHUFFLE 阶段 ==========
    reducer_inputs = {}
    for partition_id in range(self.num_reducers):
        pairs = partitions.get(partition_id, [])
        
        # 模拟网络传输延迟
        self._simulate_network_transfer(pairs)
        
        # 排序 + 分组
        grouped = defaultdict(list)
        for key, value in sorted(pairs, key=lambda x: x[0]):
            grouped[key].append(value)
        reducer_inputs[partition_id] = grouped
    
    # ========== REDUCE 阶段 ==========
    outputs = {}
    for partition_id, grouped in reducer_inputs.items():
        reduced = []
        for key, values in grouped.items():
            # 调用用户定义的 Reducer 函数
            reduced.extend(list(reducer(key, values)))
        outputs[f"reducer-{partition_id}"] = reduced
    
    return outputs
```

---

## 四、YARN 运行视角

### 4.1 YARN 架构回顾

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           YARN 架构                                     │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     ResourceManager (RM)                        │   │
│  │  ┌───────────────────┐  ┌───────────────────┐                  │   │
│  │  │   Scheduler       │  │ ApplicationManager│                  │   │
│  │  │  (资源调度器)      │  │  (应用管理器)      │                  │   │
│  │  └───────────────────┘  └───────────────────┘                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│           ┌──────────────────┼──────────────────┐                      │
│           ▼                  ▼                  ▼                       │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐          │
│  │  NodeManager 1  │ │  NodeManager 2  │ │  NodeManager 3  │          │
│  │  (节点管理器)    │ │  (节点管理器)    │ │  (节点管理器)    │          │
│  │                 │ │                 │ │                 │          │
│  │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │          │
│  │ │ Container   │ │ │ │ Container   │ │ │ │ Container   │ │          │
│  │ │ (Map Task)  │ │ │ │ (Map Task)  │ │ │ │ (Reduce)    │ │          │
│  │ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │          │
│  │ ┌─────────────┐ │ │ ┌─────────────┐ │ │                 │          │
│  │ │ Container   │ │ │ │ Container   │ │ │                 │          │
│  │ │ (AM)        │ │ │ │ (Map Task)  │ │ │                 │          │
│  │ └─────────────┘ │ │ └─────────────┘ │ │                 │          │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.2 作业提交后各组件职责

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    作业提交流程（详细版）                                │
│                                                                         │
│  【第1步】客户端提交作业                                                │
│  ─────────────────────                                                  │
│  $ hadoop jar myjob.jar MainClass /input /output                       │
│                                                                         │
│  客户端做的事情：                                                       │
│  1. 计算 InputSplit（根据输入文件大小和 Block 位置）                    │
│  2. 将作业配置、JAR 包、Split 信息上传到 HDFS                           │
│  3. 向 ResourceManager 提交 Application                                 │
│                                                                         │
│  ═══════════════════════════════════════════════════════════════════   │
│                                                                         │
│  【第2步】ResourceManager 处理请求                                      │
│  ─────────────────────────────                                          │
│  ResourceManager 做的事情：                                             │
│  1. 为该 Application 分配一个唯一 ID（如 application_1234_0001）        │
│  2. 在某个 NodeManager 上启动 ApplicationMaster                         │
│     └─ AM 是该作业的"总管"                                             │
│     └─ AM 本身也运行在一个 Container 中                                │
│                                                                         │
│  ═══════════════════════════════════════════════════════════════════   │
│                                                                         │
│  【第3步】ApplicationMaster 初始化                                      │
│  ─────────────────────────────                                          │
│  ApplicationMaster 做的事情：                                           │
│  1. 从 HDFS 读取 Split 信息                                            │
│  2. 确定需要多少 Map Task 和 Reduce Task                               │
│  3. 向 ResourceManager 申请运行任务所需的 Container                     │
│                                                                         │
│  AM 的资源申请示例：                                                    │
│  {                                                                      │
│    "task_type": "MAP",                                                 │
│    "required_memory": "1024MB",                                        │
│    "required_vcores": 1,                                               │
│    "preferred_locations": ["node-1", "node-2"]  // 数据本地性偏好      │
│  }                                                                      │
│                                                                         │
│  ═══════════════════════════════════════════════════════════════════   │
│                                                                         │
│  【第4步】ResourceManager 分配 Container                                │
│  ─────────────────────────────────                                      │
│  Scheduler 做的事情：                                                   │
│  1. 根据集群资源状况决定分配                                            │
│  2. 考虑数据本地性（优先分配到数据所在节点）                            │
│  3. 返回 Container 分配结果给 AM                                       │
│                                                                         │
│  Container 分配结果示例：                                               │
│  {                                                                      │
│    "container_id": "container_1234_0001_01_000002",                    │
│    "node_id": "node-1",                                                │
│    "memory": "1024MB",                                                 │
│    "vcores": 1                                                         │
│  }                                                                      │
│                                                                         │
│  ═══════════════════════════════════════════════════════════════════   │
│                                                                         │
│  【第5步】NodeManager 启动 Container                                    │
│  ────────────────────────────                                           │
│  NodeManager 做的事情：                                                 │
│  1. 接收 AM 的启动请求                                                 │
│  2. 下载任务运行所需的资源（JAR、配置文件）                             │
│  3. 在 Container 中启动 JVM 执行 Map/Reduce Task                       │
│  4. 监控 Container 资源使用情况                                        │
│  5. 向 RM 汇报心跳（资源使用、Container 状态）                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.3 Map/Reduce Task 如何被封装成 Container

```python
@dataclass
class Container:
    """
    Container：YARN 的资源分配单位
    
    【教学说明】
    - Container 是 YARN 分配资源的最小单位
    - 每个 Container 包含固定的 CPU 和内存
    - Map Task 或 Reduce Task 在 Container 中运行
    - Container 是一次性的：任务结束，Container 释放
    """
    container_id: str      # 唯一标识符
    node_id: str          # 所在节点
    cores: int            # 分配的 CPU 核心数
    memory_mb: int        # 分配的内存（MB）
    task_id: str          # 运行的任务 ID

@dataclass
class Task:
    """
    Task：MapReduce 的任务单元
    """
    task_id: str          # 任务 ID，如 "job-1-map-0"
    duration_s: float     # 预计执行时间
    remaining_s: float    # 剩余执行时间
    task_type: str        # "map" 或 "reduce"
```

### 4.4 多节点任务调度

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     多节点调度示例                                      │
│                                                                         │
│  场景：3个 Map Task + 2个 Reduce Task，2个 NodeManager                  │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                 ResourceManager 调度队列                         │   │
│  │                                                                  │   │
│  │  等待队列: [Map-0, Map-1, Map-2, Reduce-0, Reduce-1]            │   │
│  │                                                                  │   │
│  │  调度策略: Fair Scheduler（公平调度）                            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  【时刻 T0】初始状态                                                    │
│  ─────────────────                                                      │
│  ┌──────────────────────┐    ┌──────────────────────┐                  │
│  │ NodeManager-1        │    │ NodeManager-2        │                  │
│  │ 可用: 2核, 2048MB    │    │ 可用: 2核, 2048MB    │                  │
│  │ 运行: (空)           │    │ 运行: (空)           │                  │
│  └──────────────────────┘    └──────────────────────┘                  │
│                                                                         │
│  【时刻 T1】分配 Map Task                                               │
│  ─────────────────────                                                  │
│  ┌──────────────────────┐    ┌──────────────────────┐                  │
│  │ NodeManager-1        │    │ NodeManager-2        │                  │
│  │ 可用: 1核, 1024MB    │    │ 可用: 1核, 1024MB    │                  │
│  │ 运行:                │    │ 运行:                │                  │
│  │   [Map-0] 1核 1024MB │    │   [Map-1] 1核 1024MB │                  │
│  └──────────────────────┘    └──────────────────────┘                  │
│                                                                         │
│  【时刻 T2】继续分配                                                    │
│  ────────────────                                                       │
│  ┌──────────────────────┐    ┌──────────────────────┐                  │
│  │ NodeManager-1        │    │ NodeManager-2        │                  │
│  │ 可用: 0核, 0MB       │    │ 可用: 0核, 0MB       │                  │
│  │ 运行:                │    │ 运行:                │                  │
│  │   [Map-0] 1核 1024MB │    │   [Map-1] 1核 1024MB │                  │
│  │   [Map-2] 1核 1024MB │    │   [Reduce-0] ...     │                  │
│  └──────────────────────┘    └──────────────────────┘                  │
│                                                                         │
│  ⚠️ Reduce-1 进入等待状态，等待资源释放                                │
│                                                                         │
│  【时刻 T3】Map-0 完成，释放资源                                        │
│  ────────────────────────────                                           │
│  ┌──────────────────────┐    ┌──────────────────────┐                  │
│  │ NodeManager-1        │    │ NodeManager-2        │                  │
│  │ 可用: 1核, 1024MB    │    │ 可用: 0核, 0MB       │                  │
│  │ 运行:                │    │ 运行:                │                  │
│  │   [Map-2] 1核 1024MB │    │   [Map-1] 1核 1024MB │                  │
│  │   [Reduce-1] ← 新分配│    │   [Reduce-0] ...     │                  │
│  └──────────────────────┘    └──────────────────────┘                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.5 调度策略比较

```python
class ResourceManager:
    """
    ResourceManager: 集群资源的全局调度者
    """
    
    def _schedule_fifo(self):
        """
        FIFO 调度（先来先服务）
        
        特点：
        - 简单直接
        - 先提交的作业先获得所有资源
        - 后提交的作业必须等待
        - 缺点：大作业会阻塞小作业
        """
        current = self.queue[0]
        self._allocate_for_app(current)
        if current.is_complete():
            self.queue.popleft()
    
    def _schedule_fair(self):
        """
        Fair 调度（公平调度）
        
        特点：
        - 所有作业共享资源
        - 每个作业轮流获得资源
        - 小作业能更快完成
        - 大作业执行时间可能变长
        """
        for _ in range(len(self.queue)):
            app = self.queue.popleft()
            if not app.is_complete():
                self._allocate_for_app(app)
                self.queue.append(app)
```

---

## 五、执行流程串讲（时间线 + 角色视角）

以下用时间线方式，从作业提交到完成，串起整个流程：

```
═══════════════════════════════════════════════════════════════════════════
                        完整执行流程时间线
═══════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────┐
│ T0: 用户提交作业                                                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 【操作】$ hadoop jar pv_uv.jar PVUVDriver /input/logs /output/result   │
│                                                                         │
│ 【角色】Client (客户端)                                                 │
│ 【动作】                                                                │
│   1. 解析命令行参数                                                     │
│   2. 初始化 Job 对象，设置 Mapper、Reducer、输入输出路径                │
│   3. 调用 job.submit()                                                  │
│                                                                         │
│ 【决策依据】用户指定的配置参数                                          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ T1: Client 计算 InputSplit                                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 【角色】Client + NameNode                                              │
│ 【动作】                                                                │
│   1. Client 查询 NameNode：获取 /input/logs 下所有文件的 Block 信息    │
│   2. NameNode 返回：                                                    │
│      - 文件列表及大小                                                   │
│      - 每个 Block 的 ID 和所在 DataNode                                │
│   3. Client 计算 InputSplit：                                          │
│      - 文件总大小 384MB ÷ Block大小 128MB = 3 个 Split                 │
│      - 每个 Split 记录对应的 Block 位置（用于数据本地性调度）           │
│                                                                         │
│ 【决策依据】Block 大小配置（dfs.blocksize）                            │
│                                                                         │
│ 【产出】                                                                │
│   Split-0: bytes[0, 128MB], locations=[DN-1, DN-2, DN-3]              │
│   Split-1: bytes[128MB, 256MB], locations=[DN-2, DN-3, DN-1]          │
│   Split-2: bytes[256MB, 384MB], locations=[DN-3, DN-1, DN-2]          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ T2: Client 提交作业到 ResourceManager                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 【角色】Client → ResourceManager                                       │
│ 【动作】                                                                │
│   1. Client 将以下内容上传到 HDFS 的作业提交目录：                      │
│      - 作业 JAR 包（pv_uv.jar）                                        │
│      - 作业配置（mapred-site.xml 等）                                  │
│      - InputSplit 信息                                                 │
│   2. Client 向 RM 提交 Application 请求                                │
│                                                                         │
│ 【ResourceManager 动作】                                               │
│   1. 分配 Application ID: application_20240120_0001                    │
│   2. 将请求放入调度队列                                                 │
│                                                                         │
│ 【决策依据】RM 是否有资源启动 ApplicationMaster                        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ T3: ResourceManager 启动 ApplicationMaster                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 【角色】ResourceManager → NodeManager                                  │
│ 【动作】                                                                │
│   1. RM 选择一个有资源的 NodeManager（假设 NM-1）                      │
│   2. RM 向 NM-1 发送启动 Container 的命令：                            │
│      {                                                                  │
│        container_id: "container_..._000001",                           │
│        resource: {memory: 1024MB, vcores: 1},                          │
│        command: "启动 MRAppMaster"                                     │
│      }                                                                  │
│   3. NM-1 启动 Container，运行 ApplicationMaster                       │
│                                                                         │
│ 【决策依据】节点可用资源、AM 所需资源（mapreduce.am.resource.*）       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ T4: ApplicationMaster 初始化                                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 【角色】ApplicationMaster                                              │
│ 【动作】                                                                │
│   1. 从 HDFS 读取作业配置和 InputSplit 信息                            │
│   2. 计算任务数量：                                                     │
│      - Map Task 数 = InputSplit 数 = 3                                 │
│      - Reduce Task 数 = 用户配置 = 2                                   │
│   3. 创建任务列表：                                                     │
│      Map:    [Map-0, Map-1, Map-2]                                     │
│      Reduce: [Reduce-0, Reduce-1]                                      │
│                                                                         │
│ 【决策依据】InputSplit 数量、mapreduce.job.reduces 配置                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ T5: ApplicationMaster 向 ResourceManager 申请资源                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 【角色】ApplicationMaster → ResourceManager                            │
│ 【动作】                                                                │
│   AM 发送资源请求（心跳中携带）：                                       │
│   [                                                                     │
│     {task: Map-0, memory: 1024MB, vcores: 1,                           │
│      preferred_locations: [DN-1, DN-2, DN-3]},  // Split-0 的位置      │
│     {task: Map-1, memory: 1024MB, vcores: 1,                           │
│      preferred_locations: [DN-2, DN-3, DN-1]},  // Split-1 的位置      │
│     {task: Map-2, memory: 1024MB, vcores: 1,                           │
│      preferred_locations: [DN-3, DN-1, DN-2]},  // Split-2 的位置      │
│   ]                                                                     │
│                                                                         │
│ 【决策依据】                                                            │
│   - 任务所需资源（mapreduce.map.memory.mb, mapreduce.map.cpu.vcores）  │
│   - 数据本地性偏好（InputSplit 的 Block 位置）                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ T6: ResourceManager 分配 Container                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 【角色】ResourceManager (Scheduler)                                    │
│ 【动作】                                                                │
│   Scheduler 执行分配逻辑：                                              │
│   1. 检查每个 NodeManager 的可用资源                                   │
│   2. 优先在数据所在节点分配（数据本地性）                               │
│   3. 如果本地节点无资源，降级到机架本地或任意节点                       │
│                                                                         │
│   分配结果：                                                            │
│   - Map-0 → NM-1 (Node-Local, Split-0 在 DN-1)                        │
│   - Map-1 → NM-2 (Node-Local, Split-1 在 DN-2)                        │
│   - Map-2 → NM-3 (Node-Local, Split-2 在 DN-3)                        │
│                                                                         │
│ 【决策依据】                                                            │
│   - 节点可用资源                                                        │
│   - AM 的 preferred_locations                                          │
│   - 调度策略（FIFO/Fair/Capacity）                                     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ T7: NodeManager 启动 Map Task                                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 【角色】NodeManager                                                    │
│ 【动作】                                                                │
│   1. 收到 AM 的启动请求                                                │
│   2. 从 HDFS 下载 JAR 包和配置文件（如果本地没有缓存）                  │
│   3. 创建 Container 工作目录                                           │
│   4. 启动 JVM 进程执行 MapTask                                         │
│                                                                         │
│ 【Map Task 执行】                                                       │
│   1. 从本地 DataNode 读取 InputSplit 对应的数据                        │
│   2. 调用用户定义的 Mapper.map() 方法                                  │
│   3. 将输出写入本地磁盘（环形缓冲区 → 溢写 → 合并）                    │
│   4. 向 AM 报告进度和状态                                              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ T8: Map 输出 & Shuffle                                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 【Map 端】                                                              │
│   Map-0 输出示例：                                                      │
│   (/product/phone, user_001) → Partition 0                             │
│   (/product/laptop, user_002) → Partition 1                            │
│   (/product/phone, user_003) → Partition 0                             │
│                                                                         │
│ 【Shuffle 过程】                                                        │
│   1. 分区：根据 key 的 hash 值决定去哪个 Reducer                       │
│   2. 排序：每个分区内按 key 排序                                        │
│   3. 溢写：内存缓冲区满时写入磁盘                                       │
│   4. 合并：多个溢写文件合并成一个                                       │
│                                                                         │
│ 【Reduce 端拉取】                                                       │
│   Reduce-0 从 Map-0, Map-1, Map-2 拉取 Partition-0 的数据             │
│   Reduce-1 从 Map-0, Map-1, Map-2 拉取 Partition-1 的数据             │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ T9: Reduce Task 执行                                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 【角色】Reduce Task                                                    │
│ 【动作】                                                                │
│   1. 等待所有 Map Task 完成（或足够多的 Map 完成）                      │
│   2. 从各 Map 节点拉取属于自己分区的数据                                │
│   3. 合并排序：将拉取的数据按 key 全局排序                              │
│   4. 分组：相同 key 的 values 聚合在一起                                │
│   5. 调用用户定义的 Reducer.reduce() 方法                              │
│   6. 将输出写入 HDFS                                                   │
│                                                                         │
│ 【Reduce-0 处理示例】                                                  │
│   输入（已排序分组）：                                                  │
│     /product/phone → [user_001, user_003, user_001, user_002]          │
│     /product/tablet → [user_002, user_005]                             │
│                                                                         │
│   Reducer 计算：                                                        │
│     /product/phone: PV=4, UV=3（去重后：user_001, user_002, user_003） │
│     /product/tablet: PV=2, UV=2                                        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ T10: 结果写入 HDFS & 作业完成                                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 【角色】Reduce Task → HDFS                                             │
│ 【动作】                                                                │
│   1. Reduce-0 输出写入 /output/result/part-r-00000                     │
│   2. Reduce-1 输出写入 /output/result/part-r-00001                     │
│   3. 创建 _SUCCESS 标记文件                                            │
│                                                                         │
│ 【ApplicationMaster 动作】                                             │
│   1. 确认所有任务成功完成                                               │
│   2. 向 ResourceManager 注销 Application                               │
│   3. 释放所有 Container                                                │
│                                                                         │
│ 【ResourceManager 动作】                                               │
│   1. 标记 Application 为 FINISHED                                      │
│   2. 更新集群资源可用量                                                 │
│                                                                         │
│ 【最终输出】                                                            │
│   /output/result/                                                       │
│   ├── _SUCCESS                                                          │
│   ├── part-r-00000 (Reduce-0 的输出)                                   │
│   └── part-r-00001 (Reduce-1 的输出)                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════
```

---

## 六、结果与验证

### 6.1 示例输入数据

```
# 文件: /input/logs/access_logs.txt

2024-01-20 10:00:01 | user_001 | /product/phone_iphone15 | search | 120
2024-01-20 10:00:02 | user_002 | /product/laptop_macbook | direct | 85
2024-01-20 10:00:03 | user_001 | /product/phone_iphone15 | search | 30
2024-01-20 10:00:04 | user_003 | /product/phone_iphone15 | ad | 200
2024-01-20 10:00:05 | user_002 | /product/tablet_ipad | direct | 150
2024-01-20 10:00:06 | user_004 | /product/laptop_macbook | search | 90
2024-01-20 10:00:07 | user_001 | /product/tablet_ipad | direct | 60
2024-01-20 10:00:08 | user_005 | /product/phone_iphone15 | ad | 180
```

### 6.2 期望输出

```
# 文件: /output/result/part-r-00000

/product/laptop_macbook    {"pv": 2, "uv": 2}
/product/phone_iphone15    {"pv": 4, "uv": 4}

# 文件: /output/result/part-r-00001

/product/tablet_ipad       {"pv": 2, "uv": 2}
```

### 6.3 验证步骤

```bash
# 1. 查看输出文件
$ hdfs dfs -ls /output/result/
Found 3 items
-rw-r--r--   3 hadoop supergroup          0 2024-01-20 11:00 /output/result/_SUCCESS
-rw-r--r--   3 hadoop supergroup        128 2024-01-20 11:00 /output/result/part-r-00000
-rw-r--r--   3 hadoop supergroup         64 2024-01-20 11:00 /output/result/part-r-00001

# 2. 查看 Reduce-0 的输出
$ hdfs dfs -cat /output/result/part-r-00000
/product/laptop_macbook    {"pv": 2, "uv": 2}
/product/phone_iphone15    {"pv": 4, "uv": 4}

# 3. 手动验证 /product/phone_iphone15 的 PV/UV
# 在输入数据中搜索：
$ grep "phone_iphone15" /input/logs/access_logs.txt
# 结果：4行（user_001, user_001, user_003, user_005）
# PV = 4 ✓
# UV = 4（user_001, user_003, user_005 去重后 = 3? 等等...
#         user_001 出现 2 次，user_003 出现 1 次，user_005 出现 1 次
#         去重后 = 3 个用户... 

# 修正：让我们重新检查
# user_001: 2次
# user_003: 1次
# user_005: 1次
# UV = 3 (不是4)

# 这说明验证发现了问题！UV 应该是 3，不是 4
```

### 6.4 正确的输出

```
# 经过验证修正后的正确输出：

/product/laptop_macbook    {"pv": 2, "uv": 2}    # user_002, user_004
/product/phone_iphone15    {"pv": 4, "uv": 3}    # user_001×2, user_003, user_005
/product/tablet_ipad       {"pv": 2, "uv": 2}    # user_002, user_001
```

---

## 七、学习总结

### 7.1 HDFS 维度

| 学到的核心概念 | 在本案例中的体现 |
|---------------|-----------------|
| Block 切分机制 | 384MB 日志文件被切成 3 个 128MB 的 Block |
| 副本策略 | 每个 Block 存储 3 份，分布在不同节点 |
| NameNode 元数据管理 | 记录文件-Block-位置的三层映射 |
| 数据本地性 | Map Task 优先调度到 Block 所在节点 |

**容易误解的点**：
1. ❌ "Block 按行切分" → ✅ Block 按字节切分，InputFormat 处理行边界
2. ❌ "副本是为了性能" → ✅ 副本主要是为了容错，其次才是并行读取
3. ❌ "NameNode 存储数据" → ✅ NameNode 只存元数据，数据在 DataNode

### 7.2 MapReduce 维度

| 学到的核心概念 | 在本案例中的体现 |
|---------------|-----------------|
| Map 并行化 | 每个 Block 对应一个 Map Task，并行处理 |
| Shuffle 的核心作用 | 保证相同 URL 的记录发送到同一个 Reducer |
| Partitioner 决定数据流向 | hash(url) % num_reducers |
| Reducer 的全局聚合 | UV 去重正确性依赖于 Shuffle |

**容易误解的点**：
1. ❌ "Map 之间会通信" → ✅ Map Task 完全独立，无任何通信
2. ❌ "Shuffle 发生在 Map 和 Reduce 之间" → ✅ Shuffle 跨越 Map 端和 Reduce 端
3. ❌ "Reduce 数量等于 Map 数量" → ✅ 两者独立，由配置决定

### 7.3 YARN 维度

| 学到的核心概念 | 在本案例中的体现 |
|---------------|-----------------|
| ResourceManager 全局调度 | 决定哪个 Application 先运行，分配多少资源 |
| ApplicationMaster 作业管家 | 跟踪任务进度，向 RM 申请资源 |
| NodeManager 节点代理 | 启动 Container，监控资源使用 |
| Container 资源隔离 | 每个 Task 在独立的 Container 中运行 |

**容易误解的点**：
1. ❌ "RM 直接运行 Task" → ✅ RM 只做调度，Task 在 NM 的 Container 中运行
2. ❌ "AM 是集群常驻服务" → ✅ 每个作业有自己的 AM，作业结束 AM 消失
3. ❌ "Container 可以调整大小" → ✅ Container 资源是固定的，申请多少用多少

### 7.4 后续学习铺垫（Spark / Flink）

| Hadoop 概念 | Spark 对应概念 | Flink 对应概念 | 演进方向 |
|------------|---------------|---------------|---------|
| Block | Partition | Partition | 逻辑分区更灵活 |
| Map Task | Task (in Stage) | SubTask | 任务粒度更细 |
| Shuffle | Shuffle (宽依赖) | Exchange | 优化策略更多 |
| YARN Container | Executor | TaskManager Slot | 资源复用 |
| 磁盘中间结果 | 内存缓存 (RDD) | 内存 + 状态后端 | 减少 I/O |

**重要铺垫**：
1. **理解 Shuffle 是关键**：Spark/Flink 的性能优化核心就是减少 Shuffle
2. **分区决定并行度**：Hadoop 的 Block → Spark 的 Partition → Flink 的 Parallelism
3. **资源管理解耦**：Spark/Flink 都可以运行在 YARN 上，复用资源管理能力
4. **流批一体趋势**：MapReduce 是批处理，Flink 实现了流批统一
