#!/usr/bin/env python3
"""
测试容器内gRPC绑定和调试
"""

import os
import socket
import subprocess

def test_container_ip_binding():
    """测试容器内能绑定哪些IP地址"""
    print("=== 容器内IP绑定测试 ===")
    
    # 检查是否在容器内
    in_container = os.path.exists('/.dockerenv')
    print(f"是否在容器内: {'是' if in_container else '否'}")
    
    # 测试可绑定的IP地址
    test_addresses = [
        ('127.0.0.1', 'localhost'),
        ('0.0.0.0', 'all interfaces'),
        ('172.24.158.88', 'host IP (should fail in container)'),
    ]
    
    # 获取容器IP
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            container_ip = s.getsockname()[0]
        test_addresses.append((container_ip, 'container IP'))
    except:
        container_ip = None
    
    print(f"检测到的容器IP: {container_ip}")
    print()
    
    for ip, desc in test_addresses:
        print(f"测试绑定 {ip} ({desc}):")
        try:
            # 测试TCP绑定
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((ip, 0))
            port = sock.getsockname()[1]
            sock.close()
            print(f"  ✅ 可以绑定到 {ip}:{port}")
        except Exception as e:
            print(f"  ❌ 绑定失败: {e}")
        print()

def show_grpc_debug_settings():
    """显示gRPC调试设置方法"""
    print("=== gRPC调试设置 ===")
    print("设置环境变量来启用gRPC详细调试：")
    print("export GRPC_VERBOSITY=debug")
    print("export GRPC_TRACE=all")
    print()
    print("在Docker容器中设置：")
    print("docker run -e GRPC_VERBOSITY=debug -e GRPC_TRACE=all ...")
    print()
    print("在run_ray.py中添加环境变量：")
    print('''
container_config = {
    "environment": {
        "PYTHONPATH": "/app",
        "GRPC_VERBOSITY": "debug",  # 添加这行
        "GRPC_TRACE": "all"         # 添加这行
    },
    # ... 其他配置
}
''')

def show_correct_container_binding():
    """显示正确的容器绑定方法"""
    print("=== 正确的容器网络配置 ===")
    print("问题：容器内不能绑定宿主机IP")
    print("解决方案：")
    print()
    print("1. 监听配置 (gRPC服务器绑定):")
    print("   - 容器内绑定: '0.0.0.0' 或容器真实IP")
    print("   - 不能绑定: 宿主机IP (172.24.158.88)")
    print()
    print("2. 通告配置 (其他容器如何连接):")
    print("   - 使用: 容器真实IP (例如172.17.0.2)")
    print("   - 不能使用: 0.0.0.0 (无意义)")
    print()
    print("修复代码示例：")
    print("""
# 获取容器真实IP
container_ip = get_container_real_ip()  # 例如: 172.17.0.2

# 正确配置:
# gRPC服务器监听 - 使用0.0.0.0或容器IP
self.config['distribute']['server_host'] = '0.0.0.0'  # 监听所有接口

# P2P通告地址 - 使用容器真实IP  
self.config['distribute']['client_host'] = container_ip  # 供其他容器连接
    """)

def get_container_real_ip():
    """获取容器真实IP的方法"""
    print("=== 获取容器真实IP的方法 ===")
    
    methods = [
        ("socket连接法", "with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s: s.connect(('8.8.8.8', 80)); return s.getsockname()[0]"),
        ("hostname -I", "hostname -I | awk '{print $1}'"),
        ("ip route", "ip route get 1 | head -1 | cut -d' ' -f7"),
        ("docker inspect", "docker inspect <container_id> --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'")
    ]
    
    for method, code in methods:
        print(f"{method}: {code}")
    print()

if __name__ == "__main__":
    test_container_ip_binding()
    show_grpc_debug_settings()
    show_correct_container_binding()
    get_container_real_ip()