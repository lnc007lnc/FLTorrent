#!/usr/bin/env python3
"""
分析轮次时序：确定是数据保存错误还是请求错误
"""

import re
import os

def analyze_round_timing():
    """分析轮次时序，确定问题根源"""
    print("🕰️ 轮次时序分析工具")
    print("分析FL训练轮次、数据保存轮次、BitTorrent请求轮次的时序关系")
    print("=" * 80)
    
    # 分析客户端2的日志（最详细）
    log_file = "multi_process_test_v2/logs/client_2.log"
    
    if not os.path.exists(log_file):
        print("❌ 日志文件不存在")
        return
    
    print("📋 分析Client 2的时序日志...")
    
    with open(log_file, 'r') as f:
        lines = f.readlines()
    
    # 提取关键时序事件
    events = []
    
    for i, line in enumerate(lines):
        # 提取时间戳
        time_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
        if not time_match:
            continue
        timestamp = time_match.group(1)
        
        # 1. FL训练轮次信息
        if "Round" in line and "Results_raw" in line:
            round_match = re.search(r"'Round': (\d+)", line)
            if round_match:
                events.append((timestamp, "FL_TRAINING", f"完成训练 Round {round_match.group(1)}", i+1))
        
        # 2. 数据保存事件
        if "保存了" in line and "chunks" in line:
            round_match = re.search(r"第(\d+)轮保存了", line)
            if round_match:
                events.append((timestamp, "CHUNK_SAVE", f"保存chunks到 Round {round_match.group(1)}", i+1))
        
        # 3. BitTorrent触发事件
        if "BitTorrent will exchange chunks from round" in line:
            round_match = re.search(r"round (\d+)", line)
            if round_match:
                events.append((timestamp, "BT_TRIGGER", f"BitTorrent交换 Round {round_match.group(1)}", i+1))
        
        # 4. BitTorrent请求事件（第一次出现）
        if "Handling request" in line and "round" in line:
            round_match = re.search(r"chunk \d+:\d+ \(round (\d+)\)", line)
            if round_match:
                # 只记录第一次出现的请求
                request_round = round_match.group(1)
                if not any(event[1] == "BT_REQUEST" and f"Round {request_round}" in event[2] for event in events):
                    events.append((timestamp, "BT_REQUEST", f"处理BitTorrent请求 Round {request_round}", i+1))
        
        # 5. Bitfield生成事件
        if "My bitfield for round" in line:
            round_match = re.search(r"round (\d+)", line)
            if round_match:
                bitfield_round = round_match.group(1)
                if not any(event[1] == "BITFIELD_GEN" and f"Round {bitfield_round}" in event[2] for event in events):
                    events.append((timestamp, "BITFIELD_GEN", f"生成bitfield Round {bitfield_round}", i+1))
    
    # 按时间排序
    events.sort()
    
    print(f"📊 发现 {len(events)} 个关键时序事件:")
    print()
    
    current_fl_round = -1
    
    for timestamp, event_type, description, line_num in events:
        # 跟踪当前FL轮次
        if event_type == "FL_TRAINING":
            round_match = re.search(r"Round (\d+)", description)
            if round_match:
                current_fl_round = int(round_match.group(1))
        
        # 格式化输出
        if event_type == "FL_TRAINING":
            print(f"🎯 {timestamp} | {description:30} | 行{line_num}")
        elif event_type == "CHUNK_SAVE":
            round_match = re.search(r"Round (\d+)", description)
            save_round = int(round_match.group(1)) if round_match else -1
            
            if save_round == current_fl_round:
                status = "✅ 正确"
            else:
                status = f"❌ 错误 (FL轮次:{current_fl_round})"
            
            print(f"💾 {timestamp} | {description:30} | 行{line_num} | {status}")
            
        elif event_type == "BT_TRIGGER":
            round_match = re.search(r"Round (\d+)", description) 
            bt_round = int(round_match.group(1)) if round_match else -1
            
            if bt_round == current_fl_round:
                status = "✅ 正确"
            else:
                status = f"❌ 错误 (FL轮次:{current_fl_round})"
                
            print(f"🚀 {timestamp} | {description:30} | 行{line_num} | {status}")
            
        elif event_type == "BT_REQUEST":
            round_match = re.search(r"Round (\d+)", description)
            req_round = int(round_match.group(1)) if round_match else -1
            
            if req_round == current_fl_round:
                status = "✅ 正确"
            else:
                status = f"❌ 错误 (FL轮次:{current_fl_round})"
                
            print(f"📨 {timestamp} | {description:30} | 行{line_num} | {status}")
            
        elif event_type == "BITFIELD_GEN":
            print(f"📡 {timestamp} | {description:30} | 行{line_num}")
        
        print()
    
    print("=" * 80)
    print("🎯 时序分析结论:")
    print()
    print("检查以下关键点:")
    print("1. 【数据保存轮次】是否与【FL训练轮次】一致？")
    print("2. 【BitTorrent触发轮次】是否与【FL训练轮次】一致？") 
    print("3. 【BitTorrent请求轮次】是否与【BitTorrent触发轮次】一致？")
    print()
    print("如果:")
    print("- 数据保存错误 = FL训练Round 0，但保存到Round 1")
    print("- 请求错误 = FL训练Round 1，但请求Round 0")

if __name__ == "__main__":
    analyze_round_timing()