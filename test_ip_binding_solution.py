#!/usr/bin/env python3
"""
测试IP绑定解决方案：端口映射到特定IP
"""

import docker
import socket
import time
import ray

def test_ip_binding_solution():
    """测试端口映射到特定IP的解决方案"""
    
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
    
    # 创建模拟服务器，报告自己的绑定地址
    server_code = f'''
import socket
import time

def run_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # 容器内绑定到所有接口
    bind_addr = '0.0.0.0'
    bind_port = 50051
    
    server.bind((bind_addr, bind_port))
    server.listen(5)
    
    print(f"🚀 服务器绑定到: {{bind_addr}}:{{bind_port}}")
    print(f"📍 外部访问地址: {ray_node_ip}:{host_port}")
    print(f"🔗 实体将告诉其他实体连接地址: {ray_node_ip}:{host_port}")
    
    # 模拟实体向server注册时的行为
    def get_my_address():
        # 这是关键：实体告诉server自己的地址
        # 应该是外部可访问的地址，不是容器内绑定地址
        return f"{ray_node_ip}:{host_port}"
    
    print(f"💡 我会告诉server我的地址是: {{get_my_address()}}")
    
    server.settimeout(1)
    start_time = time.time()
    
    while time.time() - start_time < 20:
        try:
            conn, addr = server.accept()
            print(f"✅ 收到连接: {{addr}}")
            
            # 发送我的地址信息（模拟FL握手）
            my_addr = get_my_address()
            conn.send(f"MY_ADDRESS:{{my_addr}}".encode())
            
            conn.close()
            
        except socket.timeout:
            continue
        except Exception as e:
            print(f"服务器错误: {{e}}")
            break
    
    server.close()
    print("服务器关闭")

if __name__ == "__main__":
    run_server()
'''
    
    with open('/tmp/ip_binding_server.py', 'w') as f:
        f.write(server_code)
    
    print("\n=== 启动服务器容器（端口映射到特定IP）===")
    
    try:
        # 关键：端口映射到特定的Ray节点IP
        server_container = client.containers.run(
            "python:3.9-slim",
            command="python /app/ip_binding_server.py",
            detach=True,
            remove=True,
            name=f"ip_binding_test_{host_port}",
            ports={f"{50051}/tcp": (ray_node_ip, host_port)},  # 映射到特定IP！
            volumes={'/tmp/ip_binding_server.py': {'bind': '/app/ip_binding_server.py', 'mode': 'ro'}}
        )
        
        print(f"✅ 服务器容器已启动")
        print(f"🔗 端口映射: 容器内0.0.0.0:50051 -> 宿主机{ray_node_ip}:{host_port}")
        
        time.sleep(3)
        
        # 测试连接并获取服务器报告的地址
        print("\n=== 测试客户端连接 ===")
        
        try:
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_sock.settimeout(5)
            
            # 连接到映射的特定IP地址
            print(f"🔍 连接到: {ray_node_ip}:{host_port}")
            client_sock.connect((ray_node_ip, host_port))
            
            # 接收服务器报告的地址
            response = client_sock.recv(1024).decode()
            print(f"✅ 服务器报告的地址: {response}")
            
            client_sock.close()
            
            # 验证地址正确性
            if f"MY_ADDRESS:{ray_node_ip}:{host_port}" in response:
                print("✅ 地址一致性检查通过")
            else:
                print("❌ 地址不一致")
                
        except Exception as e:
            print(f"❌ 连接失败: {e}")
        
        # 查看容器日志
        print("\n=== 服务器日志 ===")
        try:
            logs = server_container.logs().decode('utf-8')
            print(logs)
        except:
            print("无法获取日志")
        
        server_container.stop()
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
    
    finally:
        # 清理
        try:
            import os
            os.remove('/tmp/ip_binding_server.py')
        except:
            pass
    
    print("\n=== 解决方案总结 ===")
    print("💡 最终方案:")
    print("  1. 容器内绑定: 0.0.0.0:容器端口 (成功绑定)")
    print(f"  2. 端口映射: {{容器端口}}/tcp -> ({ray_node_ip}, {{宿主机端口}})")
    print("  3. 实体报告地址: 使用外部可访问的Ray节点IP:端口")
    print("  4. 其他实体连接: 使用相同的Ray节点IP:端口")
    print("  ✅ 绑定和通信地址统一")

if __name__ == "__main__":
    test_ip_binding_solution()