#!/usr/bin/env python3
"""
测试重复请求的原因分析程序
分析为什么节点收到chunk后还会继续请求同一个chunk
"""

import sys
import os
import re

# 检查log中的重复请求模式
def analyze_duplicate_requests(log_file):
    """分析log文件中的重复请求模式"""
    print(f"\n🔍 分析 {log_file} 中的重复请求...")
    
    if not os.path.exists(log_file):
        print(f"❌ 文件不存在: {log_file}")
        return
    
    requests_sent = {}  # {chunk: [timestamps]}
    requests_received = {}  # {chunk: [timestamps]}  
    chunks_received = {}  # {chunk: [timestamps]}
    
    with open(log_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            # 匹配发送的请求
            match = re.search(r'Sending request for chunk (\d+:\d+)', line)
            if match:
                chunk = match.group(1)
                timestamp = line.split()[0] + " " + line.split()[1]
                if chunk not in requests_sent:
                    requests_sent[chunk] = []
                requests_sent[chunk].append((timestamp, line_num))
            
            # 匹配收到的请求
            match = re.search(r'Received request.*for chunk (\d+:\d+)', line)
            if match:
                chunk = match.group(1)
                timestamp = line.split()[0] + " " + line.split()[1]
                if chunk not in requests_received:
                    requests_received[chunk] = []
                requests_received[chunk].append((timestamp, line_num))
            
            # 匹配收到的chunk
            match = re.search(r'Received chunk (\d+:\d+)', line)
            if match:
                chunk = match.group(1)
                timestamp = line.split()[0] + " " + line.split()[1]
                if chunk not in chunks_received:
                    chunks_received[chunk] = []
                chunks_received[chunk].append((timestamp, line_num))
    
    print(f"📊 发送请求数量: {sum(len(reqs) for reqs in requests_sent.values())}")
    print(f"📊 接收请求数量: {sum(len(reqs) for reqs in requests_received.values())}")
    print(f"📊 接收chunk数量: {sum(len(chunks) for chunks in chunks_received.values())}")
    
    # 检查重复发送请求的chunk
    print(f"\n🔄 重复发送请求的chunks:")
    for chunk, reqs in requests_sent.items():
        if len(reqs) > 1:
            print(f"   Chunk {chunk}: 请求了 {len(reqs)} 次")
            for i, (timestamp, line_num) in enumerate(reqs[:5]):  # 只显示前5次
                print(f"     {i+1}. {timestamp} (line {line_num})")
            if len(reqs) > 5:
                print(f"     ... 还有 {len(reqs) - 5} 次请求")
            
            # 检查是否收到了这个chunk
            if chunk in chunks_received:
                print(f"     ✅ 已收到该chunk {len(chunks_received[chunk])} 次")
                for timestamp, line_num in chunks_received[chunk]:
                    print(f"       收到时间: {timestamp} (line {line_num})")
            else:
                print(f"     ❌ 从未收到该chunk")
    
    # 检查是否有chunk被收到但仍继续请求
    print(f"\n⚠️  收到chunk后仍继续请求的情况:")
    for chunk in requests_sent:
        if chunk in chunks_received:
            requests = requests_sent[chunk]
            received = chunks_received[chunk]
            
            # 检查是否有在收到chunk后还继续发送请求的情况
            for recv_time, recv_line in received:
                later_requests = [req for req in requests if req[1] > recv_line]
                if later_requests:
                    print(f"   Chunk {chunk}:")
                    print(f"     收到时间: {recv_time} (line {recv_line})")
                    print(f"     之后仍请求 {len(later_requests)} 次:")
                    for req_time, req_line in later_requests[:3]:
                        print(f"       {req_time} (line {req_line})")

def main():
    """主函数"""
    print("🔄 重复请求分析工具")
    print("=" * 50)
    
    logs_dir = "/mnt/g/FLtorrent_combine/FederatedScope-master/multi_process_test_v2/logs"
    
    if not os.path.exists(logs_dir):
        print(f"❌ logs目录不存在: {logs_dir}")
        return
    
    # 分析每个客户端的log
    for i in range(1, 4):
        log_file = os.path.join(logs_dir, f"client_{i}.log")
        analyze_duplicate_requests(log_file)
    
    print("\n✅ 分析完成")

if __name__ == "__main__":
    main()