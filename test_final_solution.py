#!/usr/bin/env python3
"""
测试最终的Docker网络解决方案
"""

import docker
import socket
import time

def test_final_solution():
    """测试bridge + 端口映射的解决方案"""
    
    client = docker.from_env()
    
    # 获取本地IP
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    local_ip = s.getsockname()[0]
    s.close()
    
    print(f"本地IP (Ray节点IP): {local_ip}")
    
    # 动态分配端口
    sock = socket.socket()
    sock.bind(('', 0))
    host_port = sock.getsockname()[1]
    sock.close()
    
    print(f"宿主机映射端口: {host_port}")
    print(f"容器内端口: 50051")
    
    # 创建模拟FederatedScope服务器
    server_code = f'''
import socket
import threading
import time

def handle_client(conn, addr):
    try:
        print(f"✅ 服务器收到客户端连接: {{addr}}")
        
        # 模拟FederatedScope的握手过程
        data = conn.recv(1024).decode()
        print(f"收到数据: {{data}}")
        
        # 发送回复
        response = "Server Response: Connection Established"
        conn.send(response.encode())
        
        conn.close()
        print(f"✅ 已处理客户端 {{addr}}")
        
    except Exception as e:
        print(f"❌ 处理客户端时出错: {{e}}")

def run_server():
    # 容器内绑定配置 - 绑定到0.0.0.0:50051
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server.bind(('0.0.0.0', 50051))
        server.listen(5)
        print("🚀 服务器启动成功！绑定到 0.0.0.0:50051")
        print("💡 外部可通过宿主机IP:{host_port}访问")
        
        # 运行30秒接受连接
        server.settimeout(1)
        start_time = time.time()
        
        while time.time() - start_time < 30:
            try:
                conn, addr = server.accept()
                print(f"🔗 新连接: {{addr}}")
                threading.Thread(target=handle_client, args=(conn, addr)).start()
            except socket.timeout:
                continue
            except Exception as e:
                print(f"❌ 服务器错误: {{e}}")
                break
        
        print("⏰ 服务器运行时间到，正常关闭")
        
    except Exception as e:
        print(f"❌ 服务器启动失败: {{e}}")
    finally:
        server.close()

if __name__ == "__main__":
    run_server()
'''
    
    with open('/tmp/fl_server_test.py', 'w') as f:
        f.write(server_code)
    
    print("\n=== 启动Docker服务器容器 ===")
    
    server_container = None
    try:
        # 启动服务器容器（bridge网络 + 端口映射）
        server_container = client.containers.run(
            "python:3.9-slim",
            command="python /app/fl_server_test.py",
            detach=True,
            remove=True,
            name=f"fl_server_test_{host_port}",
            ports={50051: host_port},  # 容器内50051 -> 主机host_port
            volumes={'/tmp/fl_server_test.py': {'bind': '/app/fl_server_test.py', 'mode': 'ro'}}
        )
        
        print(f"✅ 服务器容器已启动")
        print(f"📍 外部访问地址: {local_ip}:{host_port}")
        print(f"🔗 容器内绑定地址: 0.0.0.0:50051")
        
        time.sleep(3)  # 等待服务器启动
        
        # 创建客户端测试
        print("\n=== 测试客户端连接 ===")
        
        # 动态分配客户端端口
        sock = socket.socket()
        sock.bind(('', 0))
        client_host_port = sock.getsockname()[1]
        sock.close()
        
        # 创建模拟FederatedScope客户端
        client_code = f'''
import socket
import time

def test_client():
    try:
        # 连接到服务器 - 使用外部地址
        print(f"🔍 尝试连接到服务器: {local_ip}:{host_port}")
        
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.settimeout(5)
        
        # 这里使用的是宿主机IP和映射端口
        client_sock.connect(('{local_ip}', {host_port}))
        
        # 发送测试数据
        client_sock.send(b"Hello from FL Client")
        
        # 接收服务器回复
        response = client_sock.recv(1024).decode()
        print(f"✅ 收到服务器回复: {{response}}")
        
        client_sock.close()
        print("✅ 客户端连接测试成功")
        return True
        
    except Exception as e:
        print(f"❌ 客户端连接失败: {{e}}")
        return False

# 同时测试容器内绑定
def test_client_binding():
    try:
        print(f"🧪 测试客户端容器内绑定 0.0.0.0:50052")
        
        client_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client_server.bind(('0.0.0.0', 50052))
        client_server.listen(1)
        
        print("✅ 客户端容器内绑定成功")
        client_server.close()
        return True
        
    except Exception as e:
        print(f"❌ 客户端绑定失败: {{e}}")
        return False

if __name__ == "__main__":
    print("=== 客户端测试开始 ===")
    
    # 测试连接到服务器
    connect_success = test_client()
    
    # 测试自己的绑定
    bind_success = test_client_binding()
    
    print(f"连接测试: {{'✅ 成功' if connect_success else '❌ 失败'}}")
    print(f"绑定测试: {{'✅ 成功' if bind_success else '❌ 失败'}}")
'''
        
        with open('/tmp/fl_client_test.py', 'w') as f:
            f.write(client_code)
        
        # 启动客户端容器测试
        client_container = client.containers.run(
            "python:3.9-slim",
            command="python /app/fl_client_test.py",
            remove=True,
            ports={50052: client_host_port},  # 客户端端口映射
            volumes={'/tmp/fl_client_test.py': {'bind': '/app/fl_client_test.py', 'mode': 'ro'}}
        )
        
        print("客户端容器输出:")
        print(client_container.decode())
        
        # 查看服务器日志
        print("\n=== 服务器容器日志 ===")
        try:
            logs = server_container.logs().decode('utf-8')
            print(logs)
        except Exception as e:
            print(f"获取服务器日志失败: {e}")
        
    except Exception as e:
        print(f"❌ 测试过程出错: {e}")
    
    finally:
        # 清理
        if server_container:
            try:
                server_container.stop()
            except:
                pass
        
        # 清理临时文件
        import os
        for f in ['/tmp/fl_server_test.py', '/tmp/fl_client_test.py']:
            try:
                os.remove(f)
            except:
                pass
    
    print("\n=== 总结 ===")
    print("💡 最终方案:")
    print("  - Docker模式: bridge网络 + 端口映射")
    print("  - 服务器: 容器内绑定0.0.0.0:50051，外部访问node_ip:映射端口")
    print("  - 客户端: 容器内绑定0.0.0.0:50052，连接服务器使用外部地址")
    print("  - 非Docker模式: 直接使用node_ip，与4ff7版本一致")

if __name__ == "__main__":
    test_final_solution()