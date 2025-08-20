#!/bin/bash
# Launch complete FederatedScope distributed FL setup
set -e

echo "🚀 Starting FederatedScope Distributed FL Setup..."
echo "   Server: 127.0.0.1:50051"
echo "   Clients: 3 clients on ports 50052-50054"
echo ""

# Start server in background
echo "📡 Starting server..."
./launch_server.sh &
SERVER_PID=$!
echo "   Server PID: $SERVER_PID"
sleep 3

# Start clients in background
echo "👥 Starting clients..."
./launch_client_1.sh &
CLIENT1_PID=$!
echo "   Client 1 PID: $CLIENT1_PID"
sleep 2

./launch_client_2.sh &
CLIENT2_PID=$!
echo "   Client 2 PID: $CLIENT2_PID"
sleep 2

./launch_client_3.sh &
CLIENT3_PID=$!
echo "   Client 3 PID: $CLIENT3_PID"

echo ""
echo "✅ All participants started!"
echo "📊 Monitor the training progress in the terminal output"
echo ""
echo "🛑 To stop all processes:"
echo "   kill $SERVER_PID $CLIENT1_PID $CLIENT2_PID $CLIENT3_PID"

# Wait for all processes
wait $SERVER_PID $CLIENT1_PID $CLIENT2_PID $CLIENT3_PID
