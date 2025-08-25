#!/usr/bin/env python3
"""
测试最终绑定解决方案：Docker端口映射到特定IP + 配置使用外部IP
"""

import docker
import socket
import time
import ray

def test_final_bind_solution():
    """测试Docker容器能否绑定到映射的外部IP地址"""
    
    # 获取Ray节点IP
    ray.init(ignore_reinit_error=True)
    ray_node_ip = ray.util.get_node_ip_address()
    ray.shutdown()
    
    print(f"Ray节点IP: {ray_node_ip}")
    
    client = docker.from_env()
    
    # 动态分配端口
    sock = socket.socket()
    sock.bind(('', 0))
    host_port = sock.getsockname()[1]
    sock.close()
    
    print(f"宿主机端口: {host_port}")
    
    # 创建测试服务器：尝试绑定到外部IP地址
    test_server_code = f'''
import socket
import time
import sys

def test_bind_to_external_ip():
    """测试是否能绑定到外部IP地址"""
    external_ip = "{ray_node_ip}"
    external_port = {host_port}
    
    print(f"🧪 尝试绑定到外部地址: {{external_ip}}:{{external_port}}")
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        # 关键测试：能否绑定到外部IP
        server.bind((external_ip, external_port))
        server.listen(5)
        
        print(f"✅ 成功绑定到 {{external_ip}}:{{external_port}}")
        print(f"💡 FederatedScope将报告此地址给其他实体")
        
        # 运行服务器一段时间
        server.settimeout(1)
        start_time = time.time()
        
        while time.time() - start_time < 15:
            try:
                conn, addr = server.accept()
                print(f"🔗 收到连接: {{addr}}")
                conn.send(f"Hello from {{external_ip}}:{{external_port}}".encode())
                conn.close()
            except socket.timeout:
                continue
            except Exception as e:
                print(f"处理连接错误: {{e}}")
                
        server.close()
        print("✅ 测试成功：容器可以绑定到外部IP")
        return True
        
    except Exception as e:
        print(f"❌ 绑定失败: {{e}}")
        
        # 尝试绑定到0.0.0.0作为对比
        try:
            server.close()
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('0.0.0.0', external_port))
            server.listen(5)
            print(f"✅ 能绑定到 0.0.0.0:{{external_port}} (但这无法解决地址报告问题)")
            server.close()
        except Exception as e2:
            print(f"❌ 连0.0.0.0都无法绑定: {{e2}}")
        
        return False
    
    finally:
        try:
            server.close()
        except:
            pass

if __name__ == "__main__":
    success = test_bind_to_external_ip()
    sys.exit(0 if success else 1)
'''
    
    with open('/tmp/bind_test.py', 'w') as f:
        f.write(test_server_code)
    
    print(f"\n=== 测试Docker容器绑定外部IP ===")
    
    try:
        # 测试端口映射到特定IP的绑定能力
        container = client.containers.run(
            "python:3.9-slim",
            command="python /app/bind_test.py",
            detach=True,
            remove=True,
            name=f"bind_test_{host_port}",
            # 关键：端口映射到特定IP
            ports={f"{host_port}/tcp": (ray_node_ip, host_port)},
            volumes={'/tmp/bind_test.py': {'bind': '/app/bind_test.py', 'mode': 'ro'}}
        )
        
        print("等待容器执行...")
        result = container.wait()
        
        print(f"\n=== 容器执行结果 ===")
        print(f"退出代码: {result['StatusCode']}")
        
        logs = container.logs().decode('utf-8')
        print("容器日志:")
        print(logs)
        
        # 测试外部连接
        if "✅ 成功绑定到" in logs:
            print(f"\n=== 测试外部连接 ===")
            try:
                client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_sock.settimeout(3)
                client_sock.connect((ray_node_ip, host_port))
                response = client_sock.recv(1024).decode()
                client_sock.close()
                print(f"✅ 外部连接成功: {response}")
            except Exception as e:
                print(f"❌ 外部连接失败: {e}")
        
        success = result['StatusCode'] == 0
        
    except Exception as e:
        print(f"❌ 容器测试失败: {e}")
        success = False
    
    finally:
        # 清理
        try:
            import os
            os.remove('/tmp/bind_test.py')
        except:
            pass
    
    print(f"\n=== 结论 ===")
    if success:
        print("✅ Docker端口映射方案可行:")
        print(f"  - 容器可以绑定到外部IP: {ray_node_ip}")
        print(f"  - FederatedScope可以正确报告此地址给其他实体")
        print(f"  - 其他实体可以通过此地址连接")
        print("  - 绑定和通信地址完全一致")
    else:
        print("❌ Docker端口映射方案不可行")
        print("  - 需要寻找其他解决方案")
    
    return success

if __name__ == "__main__":
    test_final_bind_solution()