# 🌐 FederatedScope 网络拓扑构建功能使用指南

## 📖 概述

FederatedScope 现在支持在客户端之间构建自定义网络拓扑，允许客户端在开始联邦学习训练之前建立点对点连接。这个功能在传统的服务器-客户端通信之外，增加了客户端之间的直接通信能力。

## 🚀 核心功能

### ✅ 已实现的功能

1. **多种拓扑类型支持**：
   - **Star (星型)**: 客户端按序连接 (A-B-C, B连接A和C)
   - **Ring (环型)**: 客户端形成环形连接
   - **Mesh (网格)**: 所有客户端相互连接
   - **Tree (树型)**: 二叉树结构连接
   - **Custom (自定义)**: 用户定义连接关系

2. **自动拓扑构建流程**：
   - 服务端计算目标拓扑结构
   - 发送连接指令给各客户端
   - 客户端按指令建立连接
   - 服务端监控连接进度
   - 拓扑完成后开始训练

3. **连接监控和日志**：
   - 实时连接事件跟踪
   - 连接消息保存到本地文件
   - 拓扑构建进度监控
   - 详细的调试信息

## 📋 配置方法

### 1. 基本配置

在你的 YAML 配置文件中添加 `topology` 部分：

\`\`\`yaml
# 启用拓扑构建
topology:
  use: True
  type: 'star'  # 或 'ring', 'mesh', 'tree', 'custom'
  timeout: 60.0
  require_full_topology: True
\`\`\`

### 2. 高级配置选项

\`\`\`yaml
topology:
  use: True
  type: 'star'
  
  # 连接超时设置
  timeout: 60.0
  max_connection_attempts: 3
  connection_retry_delay: 2.0
  
  # 拓扑要求
  require_full_topology: True  # 是否要求完整拓扑
  
  # 日志设置
  save_construction_log: True
  log_dir: 'topology_logs'
  verbose: True
  
  # 自定义拓扑 (仅当type='custom'时使用)
  custom_graph:
    1: [2, 3]    # 客户端1连接到客户端2和3
    2: [1, 4]    # 客户端2连接到客户端1和4
    3: [1]       # 客户端3连接到客户端1
    4: [2]       # 客户端4连接到客户端2
\`\`\`

### 3. 不同拓扑类型示例

#### Star (星型) 拓扑
对于3个客户端，星型拓扑创建：`1-2-3`
- 客户端1连接到客户端2
- 客户端2连接到客户端1和3
- 客户端3连接到客户端2

\`\`\`yaml
topology:
  use: True
  type: 'star'
\`\`\`

#### Ring (环型) 拓扑
对于3个客户端，环型拓扑创建：`1-2-3-1`
- 客户端1连接到客户端2和3
- 客户端2连接到客户端1和3
- 客户端3连接到客户端1和2

\`\`\`yaml
topology:
  use: True
  type: 'ring'
\`\`\`

#### Mesh (网格) 拓扑
所有客户端相互连接，形成全连接图。

\`\`\`yaml
topology:
  use: True
  type: 'mesh'
\`\`\`

#### Custom (自定义) 拓扑
完全自定义的连接关系。

\`\`\`yaml
topology:
  use: True
  type: 'custom'
  custom_graph:
    1: [2]
    2: [1, 3]
    3: [2, 4]
    4: [3]
\`\`\`

## 🔧 运行步骤

### 1. 创建配置文件

创建服务端配置文件 `server_topology.yaml`：

\`\`\`yaml
use_gpu: False
federate:
  client_num: 3
  mode: 'distributed'
  total_round_num: 10
distribute:
  use: True
  server_host: '127.0.0.1'
  server_port: 50051
  role: 'server'
topology:
  use: True
  type: 'star'
  timeout: 60.0
  verbose: True
# ... 其他配置
\`\`\`

创建客户端配置文件 `client_topology.yaml`：

\`\`\`yaml
# 基本配置与服务端相同，但修改：
distribute:
  role: 'client'
  client_host: '127.0.0.1'
  client_port: 50052  # 每个客户端使用不同端口
  data_idx: 1         # 每个客户端使用不同数据索引
\`\`\`

### 2. 启动分布式训练

**步骤1**: 启动服务端
\`\`\`bash
python federatedscope/main.py --cfg server_topology.yaml
\`\`\`

**步骤2**: 启动客户端 (分别在不同终端或机器上)
\`\`\`bash
# 客户端1
python federatedscope/main.py --cfg client_topology.yaml distribute.client_port 50052 distribute.data_idx 1

# 客户端2  
python federatedscope/main.py --cfg client_topology.yaml distribute.client_port 50053 distribute.data_idx 2

# 客户端3
python federatedscope/main.py --cfg client_topology.yaml distribute.client_port 50054 distribute.data_idx 3
\`\`\`

### 3. 观察拓扑构建过程

运行时你会看到如下日志：

\`\`\`
🌐 Starting network topology construction...
📋 Computed topology: {1: [2], 2: [1, 3], 3: [2]}
📤 Sending topology instructions to clients...
📨 Sent topology instruction to Client 1: connect to [2]
📨 Sent topology instruction to Client 2: connect to [1, 3]
📨 Sent topology instruction to Client 3: connect to [2]
⏳ Waiting for topology construction to complete...
✅ Client 1: Successfully connected to Client 2
✅ Client 2: Successfully connected to Client 1
✅ Client 2: Successfully connected to Client 3
✅ Client 3: Successfully connected to Client 2
🎉 Network topology construction completed!
----------- Starting training (Round #0) -------------
\`\`\`

## 📊 监控和调试

### 1. 连接日志文件

连接事件会自动保存到 `connection_logs/connection_messages.jsonl`：

\`\`\`json
{"timestamp": 1755710280.24, "datetime": "2025-08-20T19:18:00.24", 
 "message_info": {"msg_type": "connect_msg", "sender": 1, "receiver": 0}, 
 "connection_data": {"event_type": "connect", "client_id": 1, "peer_id": 2, 
                    "details": {"topology_connection": true}}}
\`\`\`

### 2. 拓扑状态查看

在verbose模式下，服务端会定期打印拓扑状态：

\`\`\`
🌐 TOPOLOGY STATUS (STAR):
   Clients: [1, 2, 3]
   Target topology: {1: [2], 2: [1, 3], 3: [2]}
   ✅ Client 1: 1/1 connections
   ✅ Client 2: 2/2 connections  
   ✅ Client 3: 1/1 connections
   🎉 Topology construction COMPLETE!
\`\`\`

### 3. 测试验证

运行测试脚本验证功能：

\`\`\`bash
python test_topology_construction.py
\`\`\`

## ⚙️ 配置选项说明

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `topology.use` | bool | False | 是否启用拓扑构建 |
| `topology.type` | str | 'star' | 拓扑类型 |
| `topology.timeout` | float | 60.0 | 构建超时时间(秒) |
| `topology.max_connection_attempts` | int | 3 | 最大连接尝试次数 |
| `topology.connection_retry_delay` | float | 2.0 | 重试间隔(秒) |
| `topology.require_full_topology` | bool | True | 是否要求完整拓扑 |
| `topology.save_construction_log` | bool | True | 是否保存构建日志 |
| `topology.log_dir` | str | 'topology_logs' | 日志目录 |
| `topology.verbose` | bool | True | 详细输出 |
| `topology.custom_graph` | dict | {} | 自定义拓扑图 |

## 🎯 使用场景

### 1. 研究应用
- 研究不同网络拓扑对FL性能的影响
- 模拟真实网络环境的连接约束
- 测试去中心化FL算法

### 2. 实际部署
- 减少服务端通信负载
- 实现客户端间的直接模型交换
- 构建更健壮的分布式系统

### 3. 故障测试
- 测试网络分区情况
- 验证连接失败处理
- 评估系统鲁棒性

## 🔍 故障排除

### 常见问题

1. **拓扑构建超时**
   - 增加 `topology.timeout` 值
   - 检查网络连接性
   - 降低 `topology.max_connection_attempts`

2. **客户端连接失败**
   - 确保客户端能相互访问
   - 检查防火墙设置
   - 验证端口配置

3. **配置错误**
   - 检查客户端数量设置
   - 验证拓扑类型支持的最小客户端数
   - 确保分布式模式已启用

### 调试技巧

1. 启用详细日志：`topology.verbose: True`
2. 检查连接日志文件
3. 运行测试脚本验证功能
4. 逐步减少客户端数量进行测试

## 🎉 总结

FederatedScope的网络拓扑构建功能为联邦学习提供了更灵活的网络架构选择。通过简单的配置，你可以轻松实现：

- ✅ 多种预定义拓扑类型
- ✅ 自动化拓扑构建流程  
- ✅ 实时连接监控
- ✅ 完整的日志记录
- ✅ 灵活的自定义配置

这个功能为联邦学习研究和实际部署开辟了新的可能性！