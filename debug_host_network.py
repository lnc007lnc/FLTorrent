#!/usr/bin/env python3
"""
调试Docker host网络模式的具体问题
"""

import docker
import socket
import time

def debug_host_network():
    """调试host网络模式的问题"""
    
    # 获取本地IP
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    local_ip = s.getsockname()[0]
    s.close()
    
    print(f"本地IP: {local_ip}")
    
    # 动态分配端口
    sock = socket.socket()
    sock.bind(('', 0))
    test_port = sock.getsockname()[1]
    sock.close()
    
    print(f"测试端口: {test_port}")
    
    # 创建详细的调试服务器
    debug_server_code = f'''
import socket
import sys
import os
import subprocess

def get_network_info():
    print("=== 容器内网络信息 ===")
    try:
        result = subprocess.run(['ip', 'addr', 'show'], capture_output=True, text=True)
        print("网络接口:")
        print(result.stdout)
    except:
        print("无法获取网络接口信息")
    
    try:
        result = subprocess.run(['netstat', '-tlnp'], capture_output=True, text=True)
        print("\\n监听端口:")
        print(result.stdout)
    except:
        print("无法获取监听端口信息")

def test_bind_addresses():
    print("\\n=== 绑定地址测试 ===")
    test_addresses = [
        ('0.0.0.0', '所有接口'),
        ('127.0.0.1', 'localhost'),
        ('{local_ip}', '本地IP')
    ]
    
    port = {test_port}
    
    for addr, desc in test_addresses:
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((addr, port))
            server.listen(1)
            print(f"✅ {{desc}} ({{addr}}:{{port}}) 绑定成功")
            
            # 测试是否真的可以接受连接
            server.settimeout(2)
            try:
                # 在另一个线程中尝试连接
                import threading
                import time
                
                def test_connect():
                    time.sleep(0.5)
                    try:
                        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client.settimeout(1)
                        client.connect((addr if addr != '0.0.0.0' else '127.0.0.1', port))
                        client.send(b"test")
                        client.close()
                        print(f"  🔗 {{desc}}连接测试成功")
                    except Exception as e:
                        print(f"  ❌ {{desc}}连接测试失败: {{e}}")
                
                threading.Thread(target=test_connect).start()
                conn, addr_info = server.accept()
                conn.close()
                print(f"  ✅ {{desc}}接受连接成功")
            except socket.timeout:
                print(f"  ⏰ {{desc}}连接超时")
            except Exception as e:
                print(f"  ❌ {{desc}}接受连接失败: {{e}}")
            
            server.close()
            
        except Exception as e:
            print(f"❌ {{desc}} ({{addr}}:{{port}}) 绑定失败: {{e}}")

def main():
    print("Docker Host网络模式调试")
    print("=" * 50)
    
    get_network_info()
    test_bind_addresses()
    
    print("\\n=== 最终服务器启动 ===")
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', {test_port}))
        server.listen(5)
        print(f"✅ 服务器启动成功，监听 0.0.0.0:{test_port}")
        
        # 运行10秒钟接受连接
        server.settimeout(10)
        start_time = time.time()
        
        while time.time() - start_time < 10:
            try:
                conn, addr = server.accept()
                print(f"🔗 收到连接: {{addr}}")
                conn.send(b"Hello from Docker host network!")
                conn.close()
            except socket.timeout:
                print("⏰ 10秒内没有收到连接")
                break
            except Exception as e:
                print(f"❌ 连接处理错误: {{e}}")
                break
        
        server.close()
        
    except Exception as e:
        print(f"❌ 服务器启动失败: {{e}}")

if __name__ == "__main__":
    main()
'''
    
    with open('/tmp/debug_server.py', 'w') as f:
        f.write(debug_server_code)
    
    # Docker客户端
    client = docker.from_env()
    
    print("\n开始调试容器...")
    
    try:
        # 在host网络模式下运行调试服务器
        container = client.containers.run(
            "python:3.9-slim",
            command="sh -c 'apt-get update && apt-get install -y net-tools iproute2 && python /app/debug_server.py'",
            detach=True,
            remove=True,
            name=f"debug_host_network_{test_port}",
            volumes={'/tmp/debug_server.py': {'bind': '/app/debug_server.py', 'mode': 'ro'}},
            network_mode="host"
        )
        
        print("等待容器启动...")
        time.sleep(8)  # 等待apt-get完成
        
        # 查看容器日志
        print("\n=== 容器日志 ===")
        logs = container.logs().decode('utf-8')
        print(logs)
        
        # 尝试连接
        print(f"\n=== 外部连接测试 ===")
        test_addresses = [
            (local_ip, test_port, "本地IP"),
            ('127.0.0.1', test_port, "localhost")
        ]
        
        for addr, port, desc in test_addresses:
            try:
                print(f"尝试连接 {desc} ({addr}:{port})...")
                client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_sock.settimeout(3)
                client_sock.connect((addr, port))
                client_sock.send(b"Hello from external client")
                response = client_sock.recv(1024)
                client_sock.close()
                print(f"✅ {desc} 连接成功，回复: {response.decode()}")
            except Exception as e:
                print(f"❌ {desc} 连接失败: {e}")
        
        # 停止容器
        container.stop()
        print("\n容器已停止")
        
    except Exception as e:
        print(f"调试过程出错: {e}")
    
    # 清理
    try:
        import os
        os.remove('/tmp/debug_server.py')
    except:
        pass

if __name__ == "__main__":
    debug_host_network()