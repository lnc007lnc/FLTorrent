#!/bin/bash

# Multi-process federated learning test script - Real CIFAR-10+ConvNet2 test
set -e

echo "🧪 Real dataset P2P federated learning test (CIFAR-10 + ConvNet2 + GPU acceleration)"
echo "======================================================================"

# Configuration parameters
CLIENT_NUM=3
TOTAL_ROUNDS=3
TEST_DIR="multi_process_test_v2"
CHUNK_NUM=10  # Number of chunks per client model split
IMPORTANCE_METHOD="snip"  # Chunk importance calculation method: magnitude, l2_norm, snip, fisher

# Fix SSL certificate issues 
echo "🔧 Fixing SSL certificate issues..."
export PYTHONHTTPSVERIFY=0
export SSL_VERIFY=False
export CURL_CA_BUNDLE=""

# Clean and create directories
echo "📁 Setting up test directories..."
rm -rf $TEST_DIR
mkdir -p $TEST_DIR/{configs,logs}

# Create server configuration - Enable GPU acceleration
cat > "$TEST_DIR/configs/server.yaml" << EOF
use_gpu: True
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
  root: data/
  type: 'CIFAR10@torchvision'
  splits: [0.8, 0.1, 0.1]
  num_workers: 0
  transform: [['ToTensor'], ['Normalize', {'mean': [0.4914, 0.4822, 0.4465], 'std': [0.2470, 0.2435, 0.2616]}]]
  test_transform: [['ToTensor'], ['Normalize', {'mean': [0.4914, 0.4822, 0.4465], 'std': [0.2470, 0.2435, 0.2616]}]]
  args: [{'download': True}]
  splitter: 'lda'
  splitter_args: [{'alpha': 0.1}]

dataloader:
  batch_size: 32

model:
  type: convnet2
  hidden: 512
  out_channels: 10
  dropout: 0.0

train:
  local_update_steps: 5
  batch_or_epoch: epoch
  optimizer:
    lr: 0.01
    type: SGD
    weight_decay: 0.0001

grad:
  grad_clip: 5.0

criterion:
  type: CrossEntropyLoss

trainer:
  type: cvtrainer

eval:
  freq: 1
  metrics: ['acc', 'correct']
  best_res_update_round_wise_key: test_acc

topology:
  use: True
  type: 'star'
  timeout: 600.0
  verbose: True

bittorrent:
  enable: True
  timeout: 600.0
  verbose: True
  chunk_selection: 'rarest_first'
  min_completion_ratio: 0.8

chunk:
  num_chunks: $CHUNK_NUM
  importance_method: '$IMPORTANCE_METHOD'

chunk_num: $CHUNK_NUM
chunk_importance_method: '$IMPORTANCE_METHOD'

outdir: '$TEST_DIR/server_output'
EOF

# Create client configuration - Enable GPU acceleration and allocate different GPU devices
for i in $(seq 1 $CLIENT_NUM); do
    client_port=$((50051 + i))
    seed=$((12345 + i))
    # Allocate different GPU devices for different clients for parallel training
    device_id=$(((i - 1) % 2))  # Rotate between GPU 0 and 1
    
    cat > "$TEST_DIR/configs/client_${i}.yaml" << EOF
use_gpu: True
device: $device_id
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
  root: data/
  type: 'CIFAR10@torchvision'
  splits: [0.8, 0.1, 0.1]
  num_workers: 0
  transform: [['ToTensor'], ['Normalize', {'mean': [0.4914, 0.4822, 0.4465], 'std': [0.2470, 0.2435, 0.2616]}]]
  test_transform: [['ToTensor'], ['Normalize', {'mean': [0.4914, 0.4822, 0.4465], 'std': [0.2470, 0.2435, 0.2616]}]]
  args: [{'download': True}]
  splitter: 'lda'
  splitter_args: [{'alpha': 0.1}]

dataloader:
  batch_size: 32

model:
  type: convnet2
  hidden: 512
  out_channels: 10
  dropout: 0.0

train:
  local_update_steps: 5
  batch_or_epoch: epoch
  optimizer:
    lr: 0.01
    type: SGD
    weight_decay: 0.0001

grad:
  grad_clip: 5.0

criterion:
  type: CrossEntropyLoss

trainer:
  type: cvtrainer

eval:
  freq: 1
  metrics: ['acc', 'correct']
  best_res_update_round_wise_key: test_acc

bittorrent:
  enable: True
  timeout: 600.0
  verbose: True
  chunk_selection: 'rarest_first'
  min_completion_ratio: 0.8

chunk:
  num_chunks: $CHUNK_NUM
  importance_method: '$IMPORTANCE_METHOD'

chunk_num: $CHUNK_NUM
chunk_importance_method: '$IMPORTANCE_METHOD'

outdir: '$TEST_DIR/client_${i}_output'
EOF
done

echo "✅ Configuration files created"
echo "📊 Test configuration:"
echo "   - Number of clients: $CLIENT_NUM"
echo "   - Training rounds: $TOTAL_ROUNDS"
echo "   - Chunks per client: $CHUNK_NUM"
echo "   - Total expected chunks: $((CLIENT_NUM * CHUNK_NUM))"
echo "   - Chunk importance method: $IMPORTANCE_METHOD"

# 🔧 Stop and clean old instances before starting new ones
echo "🧹 Quick cleanup of old instances..."
pkill -9 -f "python.*federatedscope" 2>/dev/null || true
rm -rf tmp/client_*/client_*_chunks.db 2>/dev/null || true
rm -rf connection_logs/ 2>/dev/null || true
rm -rf topology_logs/ 2>/dev/null || true
rm -rf bittorrent_logs/ 2>/dev/null || true
sleep 1

echo "✅ Old instance cleanup completed"

# Start server and clients - Following official script approach
echo "🚀 Start distributed FL..."

echo "📡 Start server..."
PYTHONPATH=. python run_with_ssl_fix.py federatedscope/main.py --cfg "$TEST_DIR/configs/server.yaml" \
    > "$TEST_DIR/logs/server.log" 2>&1 &
SERVER_PID=$!
echo "   Server PID: $SERVER_PID"
sleep 3

echo "👥 Start clients..."
CLIENT_PIDS=()
for i in $(seq 1 $CLIENT_NUM); do
    echo "   Start client $i..."
    PYTHONPATH=. python run_with_ssl_fix.py federatedscope/main.py --cfg "$TEST_DIR/configs/client_${i}.yaml" \
        > "$TEST_DIR/logs/client_${i}.log" 2>&1 &
    client_pid=$!
    CLIENT_PIDS+=($client_pid)
    echo "   Client $i PID: $client_pid"
    sleep 2
done

echo "✅ All participants started!"
echo "📊 Monitor training progress..."

# Simple monitoring - CIFAR-10 training requires longer time
monitor_duration=600  # Monitor for 10 minutes
start_time=$(date +%s)

while true; do
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    
    if [ $elapsed -gt $monitor_duration ]; then
        echo "⏰ Monitoring time ended"
        break
    fi
    
    # Check process status
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "🛑 Server process stopped"
        break
    fi
    
    running_clients=0
    for pid in "${CLIENT_PIDS[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            ((running_clients++))
        fi
    done
    
    echo "⏰ Runtime: ${elapsed}s | Server: running | Clients: $running_clients/$CLIENT_NUM running"
    
    if [ $running_clients -eq 0 ]; then
        echo "🛑 All clients stopped"
        break
    fi
    
    sleep 10
done

# Analyze results
echo ""
echo "📈 Analyze results..."
echo "=== Server log summary ==="
if [ -f "$TEST_DIR/logs/server.log" ]; then
    echo "Last 10 lines of server log:"
    tail -10 "$TEST_DIR/logs/server.log" | head -5
    echo ""
    echo "P2P BitTorrent logs:"
    grep -i "BT-FL\|BitTorrent\|chunks" "$TEST_DIR/logs/server.log" | tail -5 || echo "BitTorrent logs not found"
    echo ""
    echo "Model performance logs:"
    grep -E "acc.*|test_acc" "$TEST_DIR/logs/server.log" | tail -3 || echo "Performance logs not found"
else
    echo "❌ Server log file does not exist"
fi

echo ""
echo "=== Client status ==="
for i in $(seq 1 $CLIENT_NUM); do
    log_file="$TEST_DIR/logs/client_${i}.log"
    if [ -f "$log_file" ]; then
        echo "Client $i:"
        if grep -q "ERROR\|Traceback\|Exception" "$log_file"; then
            echo "  ❌ Found errors:"
            grep -E "ERROR|Traceback|Exception" "$log_file" | tail -2 | sed 's/^/    /'
        else
            echo "  ✅ Running normally"
            # Show last important information
            grep -E "(assigned|train|round|acc)" "$log_file" | tail -1 | sed 's/^/    /' || echo "    No training logs"
        fi
    else
        echo "Client $i: ❌ Log file does not exist"
    fi
done

# Cleanup processes
echo ""
echo "🧹 Cleanup processes..."
echo "   Gracefully stop known processes..."
kill $SERVER_PID 2>/dev/null || true
for pid in "${CLIENT_PIDS[@]}"; do
    kill $pid 2>/dev/null || true
done

echo "   Wait for processes to exit..."
sleep 3

echo "   Force cleanup all related processes..."
pkill -f "python.*federatedscope" 2>/dev/null || true
pkill -f "multi_process.*test" 2>/dev/null || true
sleep 2

echo "   Final check and force cleanup..."
remaining_processes=$(ps aux | grep -E "python.*federatedscope|multi_process.*test" | grep -v grep | wc -l)
if [ $remaining_processes -gt 0 ]; then
    echo "   🔪 Force terminate $remaining_processes remaining processes..."
    pkill -9 -f "python.*federatedscope" 2>/dev/null || true
    pkill -9 -f "multi_process.*test" 2>/dev/null || true
fi

echo "✅ Process cleanup completed"

echo ""
echo "📁 Log file locations:"
echo "   Server: $TEST_DIR/logs/server.log"
for i in $(seq 1 $CLIENT_NUM); do
    echo "   Client $i: $TEST_DIR/logs/client_${i}.log"
done

echo ""
echo "🎉 CIFAR-10 P2P federated learning test completed!"