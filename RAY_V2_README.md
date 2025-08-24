# Ray-Powered FederatedScope V2 脚本

## 🎯 概述

Ray V2脚本完全替代了传统的shell脚本，提供一键启动的分布式联邦学习系统。

### ✨ 主要特性

- **一键启动**: 直接运行即可启动完整的FL系统（1个server + N个client）
- **智能资源管理**: 自动检测和分配GPU/CPU资源  
- **动态网络配置**: 自动IP和端口分配，避免冲突
- **实时监控**: 集成Ray Dashboard和详细日志系统
- **配置集中管理**: 所有设置在文件顶部，方便修改
- **云扩展就绪**: 支持无缝连接云服务器

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install ray[default] torch torchvision
```

### 2. 快速测试

```bash
# 快速测试（2客户端，1轮训练，3分钟监控）
python quick_ray_test.py
```

### 3. 完整运行

```bash
# 标准配置（3客户端，3轮训练，10分钟监控）
python ray_v2_script.py
```

### 4. 查看结果

- **Ray Dashboard**: http://127.0.0.1:8265
- **日志文件**: `ray_v2_logs/server.log`, `ray_v2_logs/client_*.log`
- **训练结果**: `ray_v2_output/server_output/`, `ray_v2_output/client_*_output/`

## ⚙️ 配置说明

所有配置参数都在 `ray_v2_script.py` 文件顶部的 `FLConfig` 类中：

```python
@dataclass
class FLConfig:
    # === 基础设置 ===
    CLIENT_NUM: int = 3                    # 客户端数量 ⭐
    TOTAL_ROUNDS: int = 3                  # 训练轮数 ⭐  
    CHUNK_NUM: int = 10                    # chunk数量
    IMPORTANCE_METHOD: str = "snip"         # 重要度方法
    
    # === 数据集设置 ===
    DATASET: str = "CIFAR10@torchvision"   # 数据集类型
    BATCH_SIZE: int = 32                   # 批处理大小
    
    # === 模型设置 ===
    MODEL_TYPE: str = "convnet2"          # 模型类型 ⭐
    MODEL_HIDDEN: int = 512               # 隐藏层大小
    
    # === 其他设置... ===
```

### 🎛️ 常用配置修改

| 参数 | 说明 | 示例值 |
|------|------|--------|
| `CLIENT_NUM` | 客户端数量 | 2, 3, 5 |
| `TOTAL_ROUNDS` | 训练轮数 | 1, 3, 10 |
| `MODEL_TYPE` | 模型类型 | "convnet2", "resnet18" |
| `DATASET` | 数据集 | "CIFAR10@torchvision", "FEMNIST" |
| `MONITOR_DURATION` | 监控时长（秒） | 180, 600, 1200 |

## 📊 与传统V2脚本对比

| 特性 | 传统Shell脚本 | Ray V2脚本 |
|------|--------------|-----------|
| 启动方式 | `bash multi_process_fl_test_v2.sh` | `python ray_v2_script.py` |
| 配置管理 | 脚本内bash变量 | Python类，类型安全 |
| IP端口 | 固定端口50051-54 | 动态自动分配 |
| GPU分配 | 手动轮换 | 智能自动分配 |
| 进程管理 | pkill/kill | Ray Actor优雅管理 |
| 监控 | 基础进程检查 | Ray Dashboard + 详细日志 |
| 故障恢复 | 手动重启 | Ray自动故障恢复 |
| 云扩展 | 需要手动配置 | 原生支持 |

## 🌟 高级功能

### 1. 自定义GPU分配

```python
# 在配置中修改
CONFIG.RAY_AUTO_GPU_DETECTION = True   # 自动检测
CONFIG.RAY_MAX_GPUS = 4                # 或手动限制GPU数
```

### 2. 云服务器集成

```bash
# 连接到远程Ray集群
export RAY_ADDRESS="ray://remote_head_ip:10001"
python ray_v2_script.py
```

### 3. 监控和调试

- **Ray Dashboard**: 实时查看任务执行、资源使用
- **详细日志**: 每个进程单独日志文件
- **结果摘要**: 自动生成 `results_summary.yaml`

## 🔧 故障排除

### 常见问题

1. **端口冲突**: Ray自动分配端口，无需手动处理
2. **GPU不足**: 系统自动降级到CPU模式
3. **进程残留**: 脚本自动清理，无需手动pkill

### 调试技巧

```bash
# 查看Ray集群状态
ray status

# 查看详细日志
tail -f ray_v2_logs/server.log
tail -f ray_v2_logs/client_1.log

# 查看Ray Dashboard
open http://127.0.0.1:8265
```

## 📈 性能优化建议

1. **多GPU环境**: 增加 `CLIENT_NUM` 充分利用GPU
2. **大内存**: 增加 `BATCH_SIZE` 和 `CHUNK_NUM`
3. **云扩展**: 使用 `ray_cloud_config.py` 添加云GPU
4. **监控优化**: 调整 `MONITOR_DURATION` 匹配训练时长

## 🚀 未来扩展

- [ ] 支持更多云平台（GCP, Azure）
- [ ] 集成更多FL算法
- [ ] 添加自动超参调优
- [ ] 支持动态客户端加入/退出

## 📝 更新日志

- **v2.0**: Ray版本重写，一键启动
- **v1.0**: 传统shell脚本版本

---

🎉 **Ray V2脚本让分布式联邦学习变得前所未有的简单！**