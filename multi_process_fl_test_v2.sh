#!/bin/bash

# 多进程联邦学习测试脚本（模仿官方分布式脚本）
set -e

echo "🧪 多进程联邦学习 + 拓扑测试（官方模式）"
echo "=============================================================="

# 配置参数
CLIENT_NUM=3
TOTAL_ROUNDS=5
TEST_DIR="multi_process_test_v2"

# 清理和创建目录
echo "📁 设置测试目录..."
rm -rf $TEST_DIR
mkdir -p $TEST_DIR/{configs,logs}

# 创建服务器配置 - 模仿官方femnist server配置
cat > "$TEST_DIR/configs/server.yaml" << EOF
use_gpu: False
device: 0
seed: 12345

federate:
  client_num: $CLIENT_NUM
  mode: 'distributed'
  total_round_num: $TOTAL_ROUNDS
  sample_client_num: $CLIENT_NUM

distribute:
  use: True
  server_host: '127.0.0.1'
  server_port: 50051
  role: 'server'
  data_idx: 0

data:
  type: 'toy'
  sizes: [10, 5]

model:
  type: 'lr'
  input_shape: [10]

train:
  local_update_steps: 2
  optimizer:
    lr: 0.01
    type: SGD

criterion:
  type: MSELoss

topology:
  use: True
  type: 'star'
  timeout: 60.0
  verbose: True

bittorrent:
  enable: True
  timeout: 60.0
  verbose: True
  chunk_selection: 'rarest_first'
  min_completion_ratio: 0.8

outdir: '$TEST_DIR/server_output'
EOF

# 创建客户端配置 - 模仿官方femnist client配置
for i in $(seq 1 $CLIENT_NUM); do
    client_port=$((50051 + i))
    seed=$((12345 + i))
    
    cat > "$TEST_DIR/configs/client_${i}.yaml" << EOF
use_gpu: False
device: 0
seed: $seed

federate:
  client_num: $CLIENT_NUM
  mode: 'distributed'

distribute:
  use: True
  server_host: '127.0.0.1'
  server_port: 50051
  client_host: '127.0.0.1'
  client_port: $client_port
  role: 'client'
  data_idx: $i

data:
  type: 'toy'
  sizes: [10, 5]

model:
  type: 'lr'
  input_shape: [10]

train:
  local_update_steps: 2
  optimizer:
    lr: 0.01
    type: SGD

criterion:
  type: MSELoss

bittorrent:
  enable: True
  timeout: 60.0
  verbose: True
  chunk_selection: 'rarest_first'
  min_completion_ratio: 0.8

outdir: '$TEST_DIR/client_${i}_output'
EOF
done

echo "✅ 配置文件创建完成"

# 🔧 在启动新实例前停止和清理旧实例
echo "🧹 快速清理旧实例..."
pkill -9 -f "python.*federatedscope" 2>/dev/null || true
rm -rf tmp/client_*/client_*_chunks.db 2>/dev/null || true
rm -rf connection_logs/ 2>/dev/null || true
rm -rf topology_logs/ 2>/dev/null || true
rm -rf bittorrent_logs/ 2>/dev/null || true
sleep 1

echo "✅ 旧实例清理完成"

# 启动服务器和客户端 - 模仿官方脚本方式
echo "🚀 启动分布式FL..."

echo "📡 启动服务器..."
PYTHONPATH=. python federatedscope/main.py --cfg "$TEST_DIR/configs/server.yaml" \
    > "$TEST_DIR/logs/server.log" 2>&1 &
SERVER_PID=$!
echo "   服务器 PID: $SERVER_PID"
sleep 3

echo "👥 启动客户端..."
CLIENT_PIDS=()
for i in $(seq 1 $CLIENT_NUM); do
    echo "   启动客户端 $i..."
    PYTHONPATH=. python federatedscope/main.py --cfg "$TEST_DIR/configs/client_${i}.yaml" \
        > "$TEST_DIR/logs/client_${i}.log" 2>&1 &
    client_pid=$!
    CLIENT_PIDS+=($client_pid)
    echo "   客户端 $i PID: $client_pid"
    sleep 2
done

echo "✅ 所有参与者已启动！"
echo "📊 监控训练进度..."

# 简单监控
monitor_duration=120  # 监控2分钟
start_time=$(date +%s)

while true; do
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    
    if [ $elapsed -gt $monitor_duration ]; then
        echo "⏰ 监控时间结束"
        break
    fi
    
    # 检查进程状态
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "🛑 服务器进程已停止"
        break
    fi
    
    running_clients=0
    for pid in "${CLIENT_PIDS[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            ((running_clients++))
        fi
    done
    
    echo "⏰ 运行时间: ${elapsed}s | 服务器: 运行中 | 客户端: $running_clients/$CLIENT_NUM 运行中"
    
    if [ $running_clients -eq 0 ]; then
        echo "🛑 所有客户端已停止"
        break
    fi
    
    sleep 10
done

# 分析结果
echo ""
echo "📈 分析结果..."
echo "=== 服务器日志摘要 ==="
if [ -f "$TEST_DIR/logs/server.log" ]; then
    echo "最后10行服务器日志:"
    tail -10 "$TEST_DIR/logs/server.log" | head -5
    echo ""
    echo "拓扑相关日志:"
    grep -i "topology\|star\|connect" "$TEST_DIR/logs/server.log" | tail -5 || echo "未找到拓扑日志"
else
    echo "❌ 服务器日志文件不存在"
fi

echo ""
echo "=== 客户端状态 ==="
for i in $(seq 1 $CLIENT_NUM); do
    log_file="$TEST_DIR/logs/client_${i}.log"
    if [ -f "$log_file" ]; then
        echo "客户端 $i:"
        if grep -q "ERROR\|Traceback\|Exception" "$log_file"; then
            echo "  ❌ 发现错误:"
            grep -E "ERROR|Traceback|Exception" "$log_file" | tail -2 | sed 's/^/    /'
        else
            echo "  ✅ 运行正常"
            # 显示最后一条重要信息
            grep -E "(assigned|train|round)" "$log_file" | tail -1 | sed 's/^/    /' || echo "    无训练日志"
        fi
    else
        echo "客户端 $i: ❌ 日志文件不存在"
    fi
done

# 清理进程
echo ""
echo "🧹 清理进程..."
echo "   优雅停止已知进程..."
kill $SERVER_PID 2>/dev/null || true
for pid in "${CLIENT_PIDS[@]}"; do
    kill $pid 2>/dev/null || true
done

echo "   等待进程退出..."
sleep 3

echo "   强制清理所有相关进程..."
pkill -f "python.*federatedscope" 2>/dev/null || true
pkill -f "multi_process.*test" 2>/dev/null || true
sleep 2

echo "   最终检查和强制清理..."
remaining_processes=$(ps aux | grep -E "python.*federatedscope|multi_process.*test" | grep -v grep | wc -l)
if [ $remaining_processes -gt 0 ]; then
    echo "   🔪 强制结束 $remaining_processes 个残留进程..."
    pkill -9 -f "python.*federatedscope" 2>/dev/null || true
    pkill -9 -f "multi_process.*test" 2>/dev/null || true
fi

echo "✅ 进程清理完成"

echo ""
echo "📁 日志文件位置:"
echo "   服务器: $TEST_DIR/logs/server.log"
for i in $(seq 1 $CLIENT_NUM); do
    echo "   客户端 $i: $TEST_DIR/logs/client_${i}.log"
done

echo ""
echo "🎉 测试完成！"