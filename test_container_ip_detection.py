#!/usr/bin/env python3
"""
测试容器内Ray IP检测的实际结果
"""

import ray
import socket
import subprocess
import os

def test_ip_detection_methods():
    """测试各种IP检测方法在容器内的结果"""
    print("=== 容器内IP检测方法对比 ===")
    
    # 检查是否在容器内
    in_container = os.path.exists('/.dockerenv')
    print(f"是否在容器内: {'是' if in_container else '否'}")
    print()
    
    methods = []
    
    # 方法1: Ray的get_node_ip_address (这是问题所在)
    try:
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
        ray_ip = ray.util.get_node_ip_address()
        methods.append(("ray.util.get_node_ip_address()", ray_ip))
        print(f"Ray检测IP: {ray_ip}")
    except Exception as e:
        methods.append(("ray.util.get_node_ip_address()", f"错误: {e}"))
        print(f"Ray检测IP: 错误 - {e}")
    
    # 方法2: Socket连接法
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            socket_ip = s.getsockname()[0]
        methods.append(("socket连接8.8.8.8", socket_ip))
        print(f"Socket连接法: {socket_ip}")
    except Exception as e:
        methods.append(("socket连接8.8.8.8", f"错误: {e}"))
        print(f"Socket连接法: 错误 - {e}")
    
    # 方法3: hostname -I
    try:
        result = subprocess.run(['hostname', '-I'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            hostname_ip = result.stdout.strip().split()[0]
            methods.append(("hostname -I", hostname_ip))
            print(f"hostname -I: {hostname_ip}")
    except Exception as e:
        methods.append(("hostname -I", f"错误: {e}"))
        print(f"hostname -I: 错误 - {e}")
    
    # 方法4: socket.gethostbyname
    try:
        hostname = socket.gethostname()
        gethostbyname_ip = socket.gethostbyname(hostname)
        methods.append((f"socket.gethostbyname({hostname})", gethostbyname_ip))
        print(f"gethostbyname: {gethostbyname_ip}")
    except Exception as e:
        methods.append((f"socket.gethostbyname", f"错误: {e}"))
        print(f"gethostbyname: 错误 - {e}")
    
    print()
    print("=== 总结 ===")
    for method, result in methods:
        print(f"{method}: {result}")
    
    print()
    print("=== 绑定测试 ===")
    # 测试每个IP是否可以绑定
    for method, result in methods:
        if result.startswith("错误:"):
            continue
        try:
            test_ip = result
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((test_ip, 0))
            port = sock.getsockname()[1]
            sock.close()
            print(f"✅ {test_ip} ({method}): 可以绑定，测试端口 {port}")
        except Exception as e:
            print(f"❌ {test_ip} ({method}): 绑定失败 - {e}")

if __name__ == "__main__":
    test_ip_detection_methods()