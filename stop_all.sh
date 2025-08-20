#!/bin/bash
# Stop all FederatedScope processes
echo "🛑 Stopping all FederatedScope processes..."

# Kill processes by name
pkill -f "federatedscope/main.py" || echo "No FederatedScope processes found"

# Kill processes by port (if using specific ports)
for port in 50051 50052 50053 50054; do
    PID=$(lsof -ti:$port 2>/dev/null || true)
    if [ ! -z "$PID" ]; then
        echo "Killing process on port $port (PID: $PID)"
        kill $PID
    fi
done

echo "✅ Cleanup completed!"
