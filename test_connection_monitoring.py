#!/usr/bin/env python3
"""
Test script for connection monitoring functionality
"""

import os
import sys
import time
import signal
import subprocess
import threading
from pathlib import Path

def test_connection_monitoring():
    """Test the connection monitoring feature"""
    
    print("🔧 Testing FederatedScope Connection Monitoring")
    print("=" * 60)
    
    # Ensure we have test data
    if not os.path.exists('fl_data'):
        print("📊 Generating test data...")
        subprocess.run(['python', 'setup_distributed_fl.py', '--samples', '100', '--features', '5'])
        print("")
    
    # Start server
    print("🖥️ Starting server with connection monitoring...")
    server_process = subprocess.Popen(
        ['./launch_server.sh'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    time.sleep(3)  # Wait for server to start
    
    # Start clients one by one with delays
    client_processes = []
    for i in range(1, 4):
        print(f"👤 Starting client {i}...")
        client_process = subprocess.Popen(
            [f'./launch_client_{i}.sh'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        client_processes.append(client_process)
        time.sleep(2)  # Delay between client starts
    
    print("")
    print("🔍 Monitoring connection events for 30 seconds...")
    print("   Look for connection messages in the output:")
    print("   - ✅ Client X connected to peer Y")
    print("   - 💓 Heartbeat from Client X")
    print("   - ❌ Client X disconnected from peer Y")
    print("")
    
    # Monitor output for connection events
    def monitor_process_output(process, process_name):
        """Monitor and display process output"""
        while True:
            try:
                line = process.stdout.readline()
                if line:
                    line = line.strip()
                    # Filter for connection-related messages
                    if any(keyword in line.lower() for keyword in 
                          ['connect', 'disconnect', 'heartbeat', 'connection', '🔌', '✅', '❌', '💓']):
                        print(f"[{process_name}] {line}")
                elif process.poll() is not None:
                    break
            except Exception as e:
                print(f"Error reading {process_name} output: {e}")
                break
    
    # Start output monitoring threads
    monitor_threads = []
    
    server_thread = threading.Thread(
        target=monitor_process_output, 
        args=(server_process, "SERVER"),
        daemon=True
    )
    server_thread.start()
    monitor_threads.append(server_thread)
    
    for i, client_process in enumerate(client_processes, 1):
        client_thread = threading.Thread(
            target=monitor_process_output,
            args=(client_process, f"CLIENT_{i}"),
            daemon=True
        )
        client_thread.start()
        monitor_threads.append(client_thread)
    
    # Wait and then test disconnection
    time.sleep(15)
    
    print("🔌 Testing disconnection by stopping client 2...")
    if len(client_processes) > 1:
        client_processes[1].terminate()
        time.sleep(2)
    
    print("🔌 Testing reconnection by restarting client 2...")
    if len(client_processes) > 1:
        new_client_2 = subprocess.Popen(
            ['./launch_client_2.sh'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        client_processes[1] = new_client_2
        
        # Add monitoring for new client
        client2_thread = threading.Thread(
            target=monitor_process_output,
            args=(new_client_2, "CLIENT_2_NEW"),
            daemon=True
        )
        client2_thread.start()
    
    # Wait for more monitoring
    time.sleep(15)
    
    print("")
    print("🛑 Stopping all processes...")
    
    # Cleanup
    def cleanup():
        """Clean up all processes"""
        try:
            server_process.terminate()
            for client_process in client_processes:
                if client_process.poll() is None:
                    client_process.terminate()
            
            # Wait for processes to terminate gracefully
            time.sleep(2)
            
            # Force kill if still running
            try:
                server_process.kill()
                for client_process in client_processes:
                    client_process.kill()
            except:
                pass
                
        except Exception as e:
            print(f"Error during cleanup: {e}")
    
    try:
        cleanup()
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
        cleanup()
    
    print("✅ Connection monitoring test completed!")
    print("")
    print("📋 Expected behaviors observed:")
    print("   - Connection establishment messages when clients start")
    print("   - Periodic heartbeat messages")
    print("   - Disconnection messages when clients stop")
    print("   - Reconnection messages when clients restart")

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    print("\n🛑 Received interrupt signal, cleaning up...")
    # Kill all federatedscope processes
    subprocess.run(['pkill', '-f', 'federatedscope/main.py'], 
                  capture_output=True)
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        test_connection_monitoring()
    except KeyboardInterrupt:
        signal_handler(None, None)
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        # Cleanup
        subprocess.run(['pkill', '-f', 'federatedscope/main.py'], 
                      capture_output=True)
        sys.exit(1)