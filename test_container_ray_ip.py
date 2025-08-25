#!/usr/bin/env python3
"""
测试容器内Ray IP检测的实际结果
"""

import ray
import socket
import subprocess
import os

def test_ray_ip_in_container():
    """测试Ray在容器内的IP检测"""
    print("=== 容器内Ray IP检测测试 ===")
    
    # 检查是否在容器内
    in_container = os.path.exists('/.dockerenv')
    print(f"是否在容器内: {'是' if in_container else '否'}")
    
    # 测试Ray IP检测（不需要初始化Ray集群）
    try:
        # 模拟ray.util.get_node_ip_address()的逻辑
        # Ray通常使用连接外部地址的方法来检测本地IP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            detected_ip = s.getsockname()[0]
        print(f"Ray风格IP检测结果: {detected_ip}")
    except Exception as e:
        print(f"Ray风格IP检测失败: {e}")
    
    # 测试其他IP检测方法
    print("\n=== 其他IP检测方法 ===")
    
    # hostname -I
    try:
        result = subprocess.run(['hostname', '-I'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            hostname_i = result.stdout.strip().split()[0]
            print(f"hostname -I: {hostname_i}")
    except Exception as e:
        print(f"hostname -I 失败: {e}")
    
    # socket.gethostbyname
    try:
        hostname = socket.gethostname()
        socket_ip = socket.gethostbyname(hostname)
        print(f"socket.gethostbyname({hostname}): {socket_ip}")
    except Exception as e:
        print(f"socket.gethostbyname 失败: {e}")

if __name__ == "__main__":
    test_ray_ip_in_container()