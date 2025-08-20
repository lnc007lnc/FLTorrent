#!/bin/bash

# 真实的多进程分布式FL拓扑构建测试脚本
# 此脚本将启动1个服务端和3个客户端进程

set -e  # 出错时退出

echo "🌐 REAL DISTRIBUTED FL TOPOLOGY CONSTRUCTION TEST"
echo "=" * 60
echo "Starting 1 server + 3 clients with STAR topology..."
echo ""

# 设置环境变量
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
export PYTHONPATH=$(pwd):$PYTHONPATH

# 清理之前的日志和输出
echo "🧹 Cleaning up previous test results..."
rm -rf real_topology_test_exp/
rm -rf topology_logs/
mkdir -p logs/

echo "✅ Cleanup completed"
echo ""

# 定义日志文件
SERVER_LOG="logs/server.log"
CLIENT1_LOG="logs/client1.log"
CLIENT2_LOG="logs/client2.log"
CLIENT3_LOG="logs/client3.log"

# 启动服务端
echo "🖥️  Starting server..."
python federatedscope/main.py --cfg real_test_server.yaml > $SERVER_LOG 2>&1 &
SERVER_PID=$!
echo "Server started with PID: $SERVER_PID"
echo "Server log: $SERVER_LOG"

# 等待服务端启动
echo "⏳ Waiting for server to initialize (5 seconds)..."
sleep 5

# 启动客户端1
echo "💻 Starting client 1..."
python federatedscope/main.py --cfg real_test_client_1.yaml > $CLIENT1_LOG 2>&1 &
CLIENT1_PID=$!
echo "Client 1 started with PID: $CLIENT1_PID"

# 短暂延迟
sleep 2

# 启动客户端2
echo "💻 Starting client 2..."
python federatedscope/main.py --cfg real_test_client_2.yaml > $CLIENT2_LOG 2>&1 &
CLIENT2_PID=$!
echo "Client 2 started with PID: $CLIENT2_PID"

# 短暂延迟
sleep 2

# 启动客户端3
echo "💻 Starting client 3..."
python federatedscope/main.py --cfg real_test_client_3.yaml > $CLIENT3_LOG 2>&1 &
CLIENT3_PID=$!
echo "Client 3 started with PID: $CLIENT3_PID"

echo ""
echo "🚀 All processes started! PIDs: Server=$SERVER_PID, Client1=$CLIENT1_PID, Client2=$CLIENT2_PID, Client3=$CLIENT3_PID"
echo ""

# 清理函数
cleanup() {
    echo ""
    echo "🛑 Cleaning up processes..."
    kill $SERVER_PID $CLIENT1_PID $CLIENT2_PID $CLIENT3_PID 2>/dev/null || true
    wait 2>/dev/null || true
    echo "✅ All processes terminated"
}

# 设置信号处理
trap cleanup EXIT INT TERM

# 监控进程运行
echo "📊 Monitoring test progress (will run for 90 seconds)..."
echo "Press Ctrl+C to stop early"
echo ""

# 实时显示重要日志
monitor_logs() {
    for i in {1..90}; do
        echo "⏰ Time: ${i}s/90s"
        
        # 检查拓扑构建相关的日志
        if [ -f "$SERVER_LOG" ]; then
            # 显示服务端拓扑相关日志
            tail -n 20 "$SERVER_LOG" | grep -E "(topology|TOPOLOGY|🌐|📋|📨|🎉)" | tail -n 3 2>/dev/null || true
        fi
        
        # 检查连接状态
        if [ -f "topology_logs/connection_messages.jsonl" ]; then
            CONNECTION_COUNT=$(wc -l < "topology_logs/connection_messages.jsonl" 2>/dev/null || echo "0")
            echo "📡 Connection messages logged: $CONNECTION_COUNT"
        fi
        
        # 检查进程是否还在运行
        if ! kill -0 $SERVER_PID 2>/dev/null; then
            echo "⚠️  Server process finished"
            break
        fi
        
        echo "---"
        sleep 1
    done
}

# 开始监控
monitor_logs

echo ""
echo "⏹️  Test monitoring completed"

# 显示最终结果
echo ""
echo "📊 FINAL TEST RESULTS"
echo "=" * 40

# 检查服务端日志中的关键信息
if [ -f "$SERVER_LOG" ]; then
    echo "🖥️  Server key events:"
    grep -E "(topology|TOPOLOGY|🌐|📋|📨|🎉|Starting training)" "$SERVER_LOG" | tail -n 10 || echo "   No topology events found"
fi

# 检查连接日志
if [ -f "topology_logs/connection_messages.jsonl" ]; then
    echo ""
    echo "🔗 Connection Messages:"
    echo "   Total messages: $(wc -l < topology_logs/connection_messages.jsonl)"
    echo "   Sample connections:"
    tail -n 3 "topology_logs/connection_messages.jsonl" | jq -r '.connection_data | "   \(.client_id) -> \(.peer_id) (\(.event_type))"' 2>/dev/null || echo "   Could not parse connection data"
fi

# 检查是否有训练日志
echo ""
echo "🏃 Training Progress:"
if grep -q "Starting training" "$SERVER_LOG" 2>/dev/null; then
    echo "   ✅ FL training started successfully"
    ROUNDS=$(grep -c "Starting training" "$SERVER_LOG" 2>/dev/null || echo "0")
    echo "   🔄 Training rounds completed: $ROUNDS"
else
    echo "   ⚠️  FL training may not have started"
fi

echo ""
echo "📁 Generated Files:"
[ -d "real_topology_test_exp" ] && echo "   ✅ Experiment output: real_topology_test_exp/" || echo "   ❌ No experiment output"
[ -d "topology_logs" ] && echo "   ✅ Topology logs: topology_logs/" || echo "   ❌ No topology logs"
[ -d "logs" ] && echo "   ✅ Process logs: logs/" || echo "   ❌ No process logs"

echo ""
echo "🔍 To view detailed logs:"
echo "   Server log: cat $SERVER_LOG"
echo "   Client 1 log: cat $CLIENT1_LOG"
echo "   Client 2 log: cat $CLIENT2_LOG"
echo "   Client 3 log: cat $CLIENT3_LOG"

if [ -f "topology_logs/connection_messages.jsonl" ]; then
    echo "   Connection log: cat topology_logs/connection_messages.jsonl"
fi

echo ""
echo "🎉 Real distributed FL topology test completed!"