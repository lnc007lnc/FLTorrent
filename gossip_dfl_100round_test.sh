#!/bin/bash

# Gossip-style DFL test: 5 clients, 100 rounds, Ring topology
# Purpose: Verify gossip bug fixes - chunks should only come from neighbors
set -e

echo "========================================================================"
echo "Gossip-style DFL Test: 5 Clients, 100 Rounds, Ring Topology"
echo "Verify: Each client should only collect chunks from neighbors"
echo "Ring: 1-2-3-4-5-1 => expected_chunks = 3 * 8 = 24 per client"
echo "========================================================================"

# Configuration
CLIENT_NUM=5
TOTAL_ROUNDS=100
TEST_DIR="gossip_dfl_100round_test"
CHUNK_NUM=8
GPU_DEVICE=3  # Use GPU 3

# Fix SSL certificate issues
export PYTHONHTTPSVERIFY=0
export SSL_VERIFY=False
export CURL_CA_BUNDLE=""

# Clean and create directories
echo "Setting up test directories..."
rm -rf $TEST_DIR
mkdir -p $TEST_DIR/{configs,logs}

# Create server configuration
cat > "$TEST_DIR/configs/server.yaml" << EOF
use_gpu: True
device: $GPU_DEVICE
seed: 12345

federate:
  client_num: $CLIENT_NUM
  mode: 'distributed'
  total_round_num: $TOTAL_ROUNDS
  sample_client_num: $CLIENT_NUM

distribute:
  use: True
  server_host: '127.0.0.1'
  server_port: 50061
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
  splitter_args: [{'alpha': 0.5}]

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
  freq: 10
  metrics: ['acc', 'correct']
  best_res_update_round_wise_key: test_acc

# Ring topology for gossip test
topology:
  use: True
  type: 'ring'
  timeout: 600.0
  verbose: True
  gossip:
    enable: True
    mode: neighbor_avg
    iterations: 2
    mixing_weight: 0.5

bittorrent:
  enable: True
  timeout: 120.0
  verbose: True
  chunk_selection: 'rarest_first'
  min_completion_ratio: 0.9
  neighbor_only_collection: True

chunk:
  num_chunks: $CHUNK_NUM
  importance_method: 'magnitude'

outdir: '$TEST_DIR/server_output'
EOF

# Create client configurations
for i in $(seq 1 $CLIENT_NUM); do
    client_port=$((50061 + i))
    seed=$((12345 + i))

    cat > "$TEST_DIR/configs/client_${i}.yaml" << EOF
use_gpu: True
device: $GPU_DEVICE
seed: $seed

federate:
  client_num: $CLIENT_NUM
  mode: 'distributed'

distribute:
  use: True
  server_host: '127.0.0.1'
  server_port: 50061
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
  splitter_args: [{'alpha': 0.5}]

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
  freq: 10
  metrics: ['acc', 'correct']
  best_res_update_round_wise_key: test_acc

# Ring topology for gossip test
topology:
  use: True
  type: 'ring'
  timeout: 600.0
  verbose: True
  gossip:
    enable: True
    mode: neighbor_avg
    iterations: 2
    mixing_weight: 0.5

bittorrent:
  enable: True
  timeout: 120.0
  verbose: True
  chunk_selection: 'rarest_first'
  min_completion_ratio: 0.9
  neighbor_only_collection: True

chunk:
  num_chunks: $CHUNK_NUM
  importance_method: 'magnitude'

outdir: '$TEST_DIR/client_${i}_output'
EOF
done

echo "Configuration files created"
echo "Test configuration:"
echo "   - Number of clients: $CLIENT_NUM"
echo "   - Training rounds: $TOTAL_ROUNDS"
echo "   - Topology: Ring (each client has 2 neighbors)"
echo "   - Chunks per client: $CHUNK_NUM"
echo "   - Expected chunks per client (Gossip): 3 * $CHUNK_NUM = $((3 * CHUNK_NUM)) (self + 2 neighbors)"
echo "   - GPU device: $GPU_DEVICE"
echo "   - neighbor_only_collection: True"

# Stop and clean old instances
echo "Cleaning old instances..."
pkill -9 -f "python.*federatedscope" 2>/dev/null || true
rm -rf tmp/client_*/client_*_chunks.db 2>/dev/null || true
rm -rf connection_logs/ 2>/dev/null || true
rm -rf topology_logs/ 2>/dev/null || true
rm -rf bittorrent_logs/ 2>/dev/null || true
sleep 2

echo "Starting distributed FL..."

# Start server
echo "Starting server..."
CUDA_VISIBLE_DEVICES=$GPU_DEVICE PYTHONPATH=. nohup python federatedscope/main.py --cfg "$TEST_DIR/configs/server.yaml" \
    > "$TEST_DIR/logs/server.log" 2>&1 &
SERVER_PID=$!
echo "   Server PID: $SERVER_PID"
sleep 5

# Start clients
echo "Starting clients..."
CLIENT_PIDS=()
for i in $(seq 1 $CLIENT_NUM); do
    echo "   Starting client $i..."
    CUDA_VISIBLE_DEVICES=$GPU_DEVICE PYTHONPATH=. nohup python federatedscope/main.py --cfg "$TEST_DIR/configs/client_${i}.yaml" \
        > "$TEST_DIR/logs/client_${i}.log" 2>&1 &
    client_pid=$!
    CLIENT_PIDS+=($client_pid)
    echo "   Client $i PID: $client_pid"
    sleep 2
done

echo ""
echo "All processes started!"
echo "========================================================================"
echo "Log files:"
echo "   Server: $TEST_DIR/logs/server.log"
for i in $(seq 1 $CLIENT_NUM); do
    echo "   Client $i: $TEST_DIR/logs/client_${i}.log"
done
echo ""
echo "Monitor gossip behavior with:"
echo "   grep -E 'Gossip mode|expected_total|expected chunks|neighbor' $TEST_DIR/logs/client_*.log"
echo ""
echo "Check if chunks only from neighbors:"
echo "   grep -E 'filtered bitfield|skipping HAVE broadcast' $TEST_DIR/logs/client_*.log"
echo ""
echo "Monitor progress:"
echo "   tail -f $TEST_DIR/logs/server.log"
echo "========================================================================"
