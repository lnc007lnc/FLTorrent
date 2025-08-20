#!/usr/bin/env python3
"""
Setup script for FederatedScope distributed federated learning
Creates 1 server + 3 client setup with synthetic data
"""

import os
import sys
import argparse
import numpy as np
import pickle
from pathlib import Path

def generate_distributed_data(client_num=3, instance_num=1000, feature_num=5, data_dir="fl_data"):
    """
    Generate synthetic data for distributed FL training
    """
    print(f"Generating synthetic data for {client_num} clients...")
    
    # Create data directory
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate synthetic linear regression problem
    weights = np.random.normal(loc=0.0, scale=1.0, size=feature_num)
    bias = np.random.normal(loc=0.0, scale=1.0)
    
    data = dict()
    
    # Generate client data (each with different distribution)
    for client_id in range(1, client_num + 1):
        data[client_id] = dict()
        
        # Different clients have different data distributions
        client_x = np.random.normal(loc=0.0, 
                                  scale=0.5 * client_id, 
                                  size=(instance_num, feature_num))
        client_y = np.sum(client_x * weights, axis=-1) + bias
        client_y = np.expand_dims(client_y, -1)
        
        data[client_id]['train'] = {'x': client_x, 'y': client_y}
        print(f"  - Client {client_id}: {client_x.shape} samples, std={0.5 * client_id:.1f}")
    
    # Generate test data (shared across all participants)
    test_x = np.random.normal(loc=0.0, scale=1.0, size=(instance_num, feature_num))
    test_y = np.sum(test_x * weights, axis=-1) + bias
    test_y = np.expand_dims(test_y, -1)
    test_data = {'x': test_x, 'y': test_y}
    
    # Generate validation data
    val_x = np.random.normal(loc=0.0, scale=1.0, size=(instance_num, feature_num))
    val_y = np.sum(val_x * weights, axis=-1) + bias
    val_y = np.expand_dims(val_y, -1)
    val_data = {'x': val_x, 'y': val_y}
    
    # Add test/val data to all clients
    for client_id in range(1, client_num + 1):
        data[client_id]['test'] = test_data
        data[client_id]['val'] = val_data
    
    # Server data (for global evaluation)
    data[0] = {
        'train': None,  # Server doesn't have training data
        'val': val_data,
        'test': test_data
    }
    
    # Save individual data files
    for client_id in range(0, client_num + 1):
        if client_id == 0:
            filename = f'{data_dir}/server_data'
        else:
            filename = f'{data_dir}/client_{client_id}_data'
        
        # Convert numpy arrays to lists for serialization
        save_data = {}
        for split in ['train', 'test', 'val']:
            if data[client_id][split] is not None:
                save_data[split] = {k: v.tolist() for k, v in data[client_id][split].items()}
            else:
                save_data[split] = None
        
        with open(filename, 'wb') as f:
            pickle.dump(save_data, f)
        
        print(f"  Saved: {filename}")
    
    # Save combined data file
    with open(f'{data_dir}/all_data', 'wb') as f:
        pickle.dump(data, f)
    
    print(f"Data generation completed! Files saved in '{data_dir}' directory")
    return data_dir

def create_launch_scripts(server_host="127.0.0.1", server_port=50051, data_dir="fl_data"):
    """
    Create launch scripts for server and clients
    """
    print("Creating launch scripts...")
    
    # Server launch script
    server_script = f"""#!/bin/bash
# Launch FederatedScope Server
echo "Starting FederatedScope Server on {server_host}:{server_port}..."
python federatedscope/main.py \\
    --cfg scripts/distributed_scripts/distributed_configs/distributed_server.yaml \\
    distribute.data_file '{data_dir}/server_data' \\
    distribute.server_host {server_host} \\
    distribute.server_port {server_port}
"""
    
    with open("launch_server.sh", "w") as f:
        f.write(server_script)
    os.chmod("launch_server.sh", 0o755)
    
    # Client launch scripts
    client_ports = [50052, 50053, 50054]
    
    for i, client_port in enumerate(client_ports, 1):
        client_script = f"""#!/bin/bash
# Launch FederatedScope Client {i}
echo "Starting FederatedScope Client {i} on {server_host}:{client_port}..."
python federatedscope/main.py \\
    --cfg scripts/distributed_scripts/distributed_configs/distributed_client_{i}.yaml \\
    distribute.data_file '{data_dir}/client_{i}_data' \\
    distribute.server_host {server_host} \\
    distribute.server_port {server_port} \\
    distribute.client_host {server_host} \\
    distribute.client_port {client_port}
"""
        
        script_name = f"launch_client_{i}.sh"
        with open(script_name, "w") as f:
            f.write(client_script)
        os.chmod(script_name, 0o755)
        print(f"  Created: {script_name}")
    
    # Complete launch script (all participants)
    complete_script = f"""#!/bin/bash
# Launch complete FederatedScope distributed FL setup
set -e

echo "🚀 Starting FederatedScope Distributed FL Setup..."
echo "   Server: {server_host}:{server_port}"
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
"""
    
    with open("launch_all.sh", "w") as f:
        f.write(complete_script)
    os.chmod("launch_all.sh", 0o755)
    
    print(f"  Created: launch_server.sh")
    print(f"  Created: launch_all.sh (complete setup)")
    print("")

def create_stop_script():
    """Create a script to stop all FL processes"""
    stop_script = """#!/bin/bash
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
"""
    
    with open("stop_all.sh", "w") as f:
        f.write(stop_script)
    os.chmod("stop_all.sh", 0o755)
    print("  Created: stop_all.sh")

def main():
    parser = argparse.ArgumentParser(description="Setup FederatedScope distributed FL environment")
    parser.add_argument("--clients", type=int, default=3, help="Number of clients (default: 3)")
    parser.add_argument("--samples", type=int, default=1000, help="Samples per client (default: 1000)")
    parser.add_argument("--features", type=int, default=5, help="Number of features (default: 5)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=50051, help="Server port (default: 50051)")
    parser.add_argument("--data-dir", type=str, default="fl_data", help="Data directory (default: fl_data)")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🔧 FederatedScope Distributed FL Setup")
    print("=" * 60)
    print(f"Configuration:")
    print(f"  - Clients: {args.clients}")
    print(f"  - Samples per client: {args.samples}")
    print(f"  - Features: {args.features}")
    print(f"  - Server: {args.host}:{args.port}")
    print(f"  - Data directory: {args.data_dir}")
    print("")
    
    # Step 1: Generate data
    data_dir = generate_distributed_data(
        client_num=args.clients,
        instance_num=args.samples,
        feature_num=args.features,
        data_dir=args.data_dir
    )
    print("")
    
    # Step 2: Create launch scripts
    create_launch_scripts(
        server_host=args.host,
        server_port=args.port,
        data_dir=data_dir
    )
    
    # Step 3: Create stop script
    create_stop_script()
    print("")
    
    print("🎉 Setup completed!")
    print("=" * 60)
    print("📋 Next steps:")
    print("  1. Start all participants: ./launch_all.sh")
    print("  2. Or start individually:")
    print("     - Server: ./launch_server.sh")
    print("     - Clients: ./launch_client_1.sh, ./launch_client_2.sh, ./launch_client_3.sh")
    print("  3. Stop all: ./stop_all.sh")
    print("")
    print("📊 Monitor the FL training progress in terminal output!")

if __name__ == "__main__":
    main()