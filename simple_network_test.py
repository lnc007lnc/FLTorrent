#!/usr/bin/env python3
"""
简化的网络模式测试
"""

import docker
import socket
import time

def simple_test():
    client = docker.from_env()
    
    # 获取本地IP
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    local_ip = s.getsockname()[0]
    s.close()
    
    print(f"本地IP: {local_ip}")
    
    # 测试1: 基本的host网络模式
    print("\n=== 测试1: Host网络模式基本功能 ===")
    
    try:
        container = client.containers.run(
            "python:3.9-slim",
            command="python -c \"import socket; s=socket.socket(); s.bind(('0.0.0.0', 8888)); s.listen(1); print('Server started on 0.0.0.0:8888'); conn,addr=s.accept(); print(f'Connected: {addr}'); conn.send(b'Hello'); conn.close()\"",
            detach=True,
            remove=True,
            network_mode="host"
        )
        
        time.sleep(2)
        
        # 尝试连接
        try:
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_sock.settimeout(3)
            client_sock.connect((local_ip, 8888))
            response = client_sock.recv(1024)
            client_sock.close()
            print(f"✅ Host网络模式正常，收到: {response.decode()}")
        except Exception as e:
            print(f"❌ Host网络模式连接失败: {e}")
        
        # 查看日志
        print("容器日志:")
        try:
            logs = container.logs().decode('utf-8')
            print(logs)
        except:
            print("无法获取日志")
        
        container.stop()
        
    except Exception as e:
        print(f"Host网络测试失败: {e}")
    
    # 测试2: Bridge网络模式对比
    print("\n=== 测试2: Bridge网络模式对比 ===")
    
    try:
        container = client.containers.run(
            "python:3.9-slim",
            command="python -c \"import socket; s=socket.socket(); s.bind(('0.0.0.0', 8889)); s.listen(1); print('Server started on 0.0.0.0:8889'); conn,addr=s.accept(); print(f'Connected: {addr}'); conn.send(b'Hello Bridge'); conn.close()\"",
            detach=True,
            remove=True,
            ports={8889: 8889},
            network_mode="bridge"
        )
        
        time.sleep(2)
        
        # 尝试连接
        try:
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_sock.settimeout(3)
            client_sock.connect((local_ip, 8889))
            response = client_sock.recv(1024)
            client_sock.close()
            print(f"✅ Bridge网络模式正常，收到: {response.decode()}")
        except Exception as e:
            print(f"❌ Bridge网络模式连接失败: {e}")
        
        # 查看日志
        print("容器日志:")
        try:
            logs = container.logs().decode('utf-8')
            print(logs)
        except:
            print("无法获取日志")
        
        container.stop()
        
    except Exception as e:
        print(f"Bridge网络测试失败: {e}")

    print("\n=== 宿主机端口占用检查 ===")
    for port in [8888, 8889]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((local_ip, port))
        sock.close()
        status = "占用" if result == 0 else "空闲"
        print(f"端口 {port}: {status}")

if __name__ == "__main__":
    simple_test()