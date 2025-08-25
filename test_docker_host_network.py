#!/usr/bin/env python3
"""
测试Docker host网络模式下的网络连接问题
"""

import docker
import socket
import time
import subprocess
import threading
import os
import ray

def get_local_ip():
    """获取本地IP地址"""
    try:
        # 方法1: 通过socket连接获取
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except:
        return "127.0.0.1"

def get_ray_node_ip():
    """获取Ray节点IP"""
    try:
        ray.init(ignore_reinit_error=True)
        node_ip = ray.util.get_node_ip_address()
        ray.shutdown()
        return node_ip
    except Exception as e:
        print(f"Ray节点IP获取失败: {e}")
        return get_local_ip()

def test_port_availability(host, port):
    """测试端口是否可用"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False

def create_simple_server(port):
    """创建简单的TCP服务器"""
    server_code = f'''
import socket
import time

def run_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # 尝试不同的绑定地址
    bind_addresses = ['0.0.0.0', '{get_local_ip()}', '127.0.0.1']
    
    for addr in bind_addresses:
        try:
            print(f"尝试绑定到 {{addr}}:{port}")
            server.bind((addr, {port}))
            print(f"✅ 成功绑定到 {{addr}}:{port}")
            break
        except Exception as e:
            print(f"❌ 绑定 {{addr}}:{port} 失败: {{e}}")
            continue
    else:
        print("❌ 所有绑定尝试都失败")
        return
    
    server.listen(5)
    print(f"服务器正在监听端口 {port}...")
    
    while True:
        try:
            client, address = server.accept()
            print(f"✅ 收到来自 {{address}} 的连接")
            client.send(b"Hello from server!")
            client.close()
        except Exception as e:
            print(f"服务器错误: {{e}}")
            break

if __name__ == "__main__":
    run_server()
'''
    
    with open(f'/tmp/test_server_{port}.py', 'w') as f:
        f.write(server_code)

def test_docker_host_network():
    """测试Docker host网络模式"""
    print("=" * 60)
    print("Docker Host网络模式测试")
    print("=" * 60)
    
    # 获取IP地址信息
    local_ip = get_local_ip()
    ray_ip = get_ray_node_ip()
    
    print(f"本地IP: {local_ip}")
    print(f"Ray节点IP: {ray_ip}")
    print(f"IP地址一致性: {'✅' if local_ip == ray_ip else '❌'}")
    
    # 动态分配端口
    sock = socket.socket()
    sock.bind(('', 0))
    test_port = sock.getsockname()[1]
    sock.close()
    
    print(f"测试端口: {test_port}")
    
    # 创建测试服务器代码
    create_simple_server(test_port)
    
    # Docker客户端
    client = docker.from_env()
    
    print("\n" + "=" * 40)
    print("测试1: 标准bridge网络模式")
    print("=" * 40)
    
    try:
        # 测试1: 标准bridge网络 + 端口映射
        bridge_container = client.containers.run(
            "python:3.9-slim",
            command=f"python /app/test_server_{test_port}.py",
            detach=True,
            remove=True,
            name=f"test_bridge_{test_port}",
            ports={test_port: test_port},
            volumes={f'/tmp/test_server_{test_port}.py': {'bind': f'/app/test_server_{test_port}.py', 'mode': 'ro'}},
            network_mode="bridge"
        )
        
        time.sleep(3)  # 等待容器启动
        
        # 检查容器状态
        bridge_container.reload()
        print(f"Bridge容器状态: {bridge_container.status}")
        
        # 获取容器IP
        container_ip = bridge_container.attrs['NetworkSettings']['IPAddress']
        print(f"Bridge容器IP: {container_ip}")
        
        # 测试连接
        test_addresses = [
            (local_ip, test_port, "本地IP"),
            ('127.0.0.1', test_port, "localhost"),
            (container_ip, test_port, "容器IP")
        ]
        
        for addr, port, desc in test_addresses:
            if addr:  # 如果IP不为空
                available = test_port_availability(addr, port)
                print(f"连接测试 {desc} ({addr}:{port}): {'✅' if available else '❌'}")
        
        bridge_container.stop()
        print("Bridge容器已停止")
        
    except Exception as e:
        print(f"Bridge网络测试失败: {e}")
    
    print("\n" + "=" * 40)
    print("测试2: Host网络模式")
    print("=" * 40)
    
    try:
        # 重新分配端口避免冲突
        sock = socket.socket()
        sock.bind(('', 0))
        host_test_port = sock.getsockname()[1]
        sock.close()
        
        create_simple_server(host_test_port)
        
        # 测试2: host网络模式
        host_container = client.containers.run(
            "python:3.9-slim",
            command=f"python /app/test_server_{host_test_port}.py",
            detach=True,
            remove=True,
            name=f"test_host_{host_test_port}",
            volumes={f'/tmp/test_server_{host_test_port}.py': {'bind': f'/app/test_server_{host_test_port}.py', 'mode': 'ro'}},
            network_mode="host"
        )
        
        time.sleep(3)  # 等待容器启动
        
        # 检查容器状态
        host_container.reload()
        print(f"Host容器状态: {host_container.status}")
        
        # Host模式下容器没有独立IP
        print("Host模式下容器共享宿主机网络栈")
        
        # 测试连接
        test_addresses = [
            (local_ip, host_test_port, "本地IP"),
            (ray_ip, host_test_port, "Ray节点IP"),
            ('127.0.0.1', host_test_port, "localhost")
        ]
        
        for addr, port, desc in test_addresses:
            if addr:  # 如果IP不为空
                available = test_port_availability(addr, port)
                print(f"连接测试 {desc} ({addr}:{port}): {'✅' if available else '❌'}")
        
        host_container.stop()
        print("Host容器已停止")
        
    except Exception as e:
        print(f"Host网络测试失败: {e}")
    
    print("\n" + "=" * 40)
    print("测试3: 客户端连接测试")
    print("=" * 40)
    
    # 启动一个简单的echo服务器用于客户端测试
    echo_port = None
    echo_process = None
    
    try:
        sock = socket.socket()
        sock.bind(('', 0))
        echo_port = sock.getsockname()[1]
        sock.close()
        
        # 创建echo服务器
        echo_server_code = f'''
import socket
import threading

def handle_client(client_socket, address):
    try:
        print(f"客户端连接: {{address}}")
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            print(f"收到数据: {{data.decode()}}")
            client_socket.send(f"Echo: {{data.decode()}}".encode())
    except:
        pass
    finally:
        client_socket.close()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('{ray_ip}', {echo_port}))
server.listen(5)
print(f"Echo服务器启动在 {ray_ip}:{echo_port}")

while True:
    client, address = server.accept()
    threading.Thread(target=handle_client, args=(client, address)).start()
'''
        
        with open('/tmp/echo_server.py', 'w') as f:
            f.write(echo_server_code)
        
        # 启动echo服务器
        echo_process = subprocess.Popen(['python', '/tmp/echo_server.py'])
        time.sleep(2)
        
        # 创建Docker客户端来连接echo服务器
        client_code = f'''
import socket
import time

def test_connection():
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(5)
        print(f"尝试连接到 {ray_ip}:{echo_port}")
        client.connect(('{ray_ip}', {echo_port}))
        
        # 发送测试消息
        client.send(b"Hello from Docker client!")
        response = client.recv(1024)
        print(f"收到回复: {{response.decode()}}")
        
        client.close()
        print("✅ 客户端连接成功")
        return True
    except Exception as e:
        print(f"❌ 客户端连接失败: {{e}}")
        return False

if __name__ == "__main__":
    success = test_connection()
    exit(0 if success else 1)
'''
        
        with open('/tmp/test_client.py', 'w') as f:
            f.write(client_code)
        
        # 在host网络模式下运行客户端
        try:
            client_container = client.containers.run(
                "python:3.9-slim",
                command="python /app/test_client.py",
                remove=True,
                volumes={'/tmp/test_client.py': {'bind': '/app/test_client.py', 'mode': 'ro'}},
                network_mode="host"
            )
            print("Host网络模式客户端测试完成")
        except Exception as e:
            print(f"Host网络模式客户端测试失败: {e}")
        
        # 在bridge网络模式下运行客户端
        try:
            client_container = client.containers.run(
                "python:3.9-slim",
                command="python /app/test_client.py",
                remove=True,
                volumes={'/tmp/test_client.py': {'bind': '/app/test_client.py', 'mode': 'ro'}},
                network_mode="bridge"
            )
            print("Bridge网络模式客户端测试完成")
        except Exception as e:
            print(f"Bridge网络模式客户端测试失败: {e}")
            
    finally:
        if echo_process:
            echo_process.terminate()
            echo_process.wait()
    
    # 清理临时文件
    for f in [f'/tmp/test_server_{test_port}.py', f'/tmp/test_server_{host_test_port}.py', '/tmp/echo_server.py', '/tmp/test_client.py']:
        try:
            os.remove(f)
        except:
            pass
    
    print("\n" + "=" * 40)
    print("测试完成")
    print("=" * 40)

if __name__ == "__main__":
    test_docker_host_network()