#!/usr/bin/env python3

"""
测试地址分配修复的快速验证脚本
验证server是否正确提供peer地址信息给clients
"""

import sys
import os
import time
import subprocess
import signal
from pathlib import Path

def run_server():
    """启动server"""
    print("🚀 Starting server...")
    server_cmd = [
        sys.executable, 
        "federatedscope/main.py", 
        "--cfg", "multi_process_test_v2/configs/server.yaml"
    ]
    
    server_process = subprocess.Popen(
        server_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    
    return server_process

def run_client(client_id):
    """启动指定的client"""
    print(f"🚀 Starting client {client_id}...")
    client_cmd = [
        sys.executable,
        "federatedscope/main.py",
        "--cfg", f"multi_process_test_v2/configs/client_{client_id}.yaml"
    ]
    
    client_process = subprocess.Popen(
        client_cmd,
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    
    return client_process

def monitor_process_output(process, process_name, timeout=30):
    """监控进程输出，寻找关键信息"""
    start_time = time.time()
    topology_info_found = False
    address_info_found = False
    connection_success = False
    
    while time.time() - start_time < timeout:
        try:
            line = process.stdout.readline()
            if not line:
                if process.poll() is not None:  # Process terminated
                    break
                continue
                
            print(f"[{process_name}] {line.strip()}")
            
            # 检查关键信息
            if "📍 With addresses:" in line:
                address_info_found = True
                print(f"✅ [{process_name}] Found address info in topology instruction")
                
            if "Neighbor addresses:" in line:
                topology_info_found = True
                print(f"✅ [{process_name}] Client received neighbor addresses")
                
            if "Added peer" in line and "to comm_manager" in line:
                connection_success = True
                print(f"✅ [{process_name}] Peer connection established successfully")
                
            # 如果找到所有关键信息，提前结束
            if topology_info_found and address_info_found and connection_success:
                print(f"🎉 [{process_name}] All key information found!")
                break
                
        except Exception as e:
            print(f"❌ Error reading output from {process_name}: {e}")
            break
    
    return {
        'topology_info': topology_info_found,
        'address_info': address_info_found, 
        'connection_success': connection_success
    }

def cleanup_processes(processes):
    """清理所有进程"""
    print("🧹 Cleaning up processes...")
    for name, process in processes.items():
        if process and process.poll() is None:
            try:
                process.terminate()
                process.wait(timeout=5)
                print(f"✅ {name} terminated successfully")
            except subprocess.TimeoutExpired:
                print(f"⚠️ {name} didn't terminate gracefully, killing...")
                process.kill()
                process.wait()
            except Exception as e:
                print(f"❌ Error terminating {name}: {e}")

def main():
    """主测试流程"""
    print("🧪 Testing Address Allocation Fix")
    print("=" * 50)
    
    # 切换到项目根目录
    os.chdir("/mnt/g/FLtorrent_combine/FederatedScope-master")
    
    processes = {}
    results = {}
    
    try:
        # 启动server
        server_process = run_server()
        processes['server'] = server_process
        
        # 等待server启动
        print("⏱️ Waiting for server to start...")
        time.sleep(3)
        
        # 启动clients
        client_processes = []
        for client_id in [1, 2, 3]:
            client_process = run_client(client_id)
            processes[f'client_{client_id}'] = client_process
            client_processes.append(client_process)
            time.sleep(1)  # 间隔启动
        
        # 监控server输出（寻找地址信息）
        print("\n📋 Monitoring server output for address allocation...")
        server_results = monitor_process_output(server_process, "SERVER", timeout=20)
        results['server'] = server_results
        
        # 监控client输出（寻找地址接收）
        print("\n📋 Monitoring client outputs for address reception...")
        for i, client_process in enumerate(client_processes, 1):
            client_results = monitor_process_output(client_process, f"CLIENT_{i}", timeout=15)
            results[f'client_{i}'] = client_results
        
        # 分析结果
        print("\n📊 Test Results Analysis:")
        print("=" * 50)
        
        server_success = results['server']['address_info']
        print(f"Server address allocation: {'✅ SUCCESS' if server_success else '❌ FAILED'}")
        
        total_clients = 3
        successful_clients = 0
        for i in range(1, total_clients + 1):
            client_key = f'client_{i}'
            if client_key in results:
                success = (results[client_key]['topology_info'] and 
                          results[client_key]['connection_success'])
                print(f"Client {i} address reception: {'✅ SUCCESS' if success else '❌ FAILED'}")
                if success:
                    successful_clients += 1
        
        # 整体评估
        print(f"\n🎯 Overall Success Rate: {successful_clients}/{total_clients} clients")
        
        if server_success and successful_clients >= 2:
            print("🎉 ADDRESS ALLOCATION FIX: SUCCESS!")
            print("✅ Server correctly provides peer addresses in topology instructions")
            print("✅ Clients successfully receive and use real addresses")
        else:
            print("❌ ADDRESS ALLOCATION FIX: NEEDS MORE WORK")
            print("⚠️ Some components are not working as expected")
        
    except KeyboardInterrupt:
        print("\n⏹️ Test interrupted by user")
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
    finally:
        cleanup_processes(processes)
        print("\n✅ Test completed")

if __name__ == "__main__":
    main()