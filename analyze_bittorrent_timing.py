#!/usr/bin/env python3
"""
分析BitTorrent交换的时间序列
找出chunk_9未交换的时间窗口问题
"""

import sqlite3
import os
from datetime import datetime

def analyze_bittorrent_timing():
    """分析BitTorrent交换的时间问题"""
    print("🕐 分析：BitTorrent交换的时间序列问题")
    print("=" * 80)
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    # 1. 分析每个客户端的时间线
    print("📊 第一阶段：各客户端的时间线分析")
    print("-" * 60)
    
    all_events = []  # [(timestamp, client_id, event_type, details)]
    
    for client_id, db_path in client_dbs.items():
        print(f"\n🔍 客户端 {client_id} 时间线:")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # 收集本地chunk生成事件
            cursor.execute("""
                SELECT round_num, chunk_id, created_at, chunk_hash
                FROM chunk_metadata 
                WHERE round_num IN (3, 4)
                ORDER BY created_at
            """)
            
            local_chunks = cursor.fetchall()
            
            for round_num, chunk_id, created_at, chunk_hash in local_chunks:
                all_events.append((created_at, client_id, 'local_gen', f"轮次{round_num}_chunk{chunk_id}"))
                
                try:
                    time_str = datetime.fromtimestamp(created_at).strftime('%H:%M:%S.%f')[:-3]
                except:
                    time_str = str(created_at)
                
                if chunk_id == 9:  # 重点关注chunk_9
                    print(f"   🎯 {time_str}: 生成轮次{round_num}_chunk{chunk_id} (重点)")
                elif chunk_id in [0, 1, 8]:  # 关键chunk
                    print(f"   📦 {time_str}: 生成轮次{round_num}_chunk{chunk_id}")
            
            # 收集BitTorrent交换事件
            cursor.execute("""
                SELECT round_num, chunk_id, source_client_id, 
                       received_time as event_time, chunk_hash
                FROM bt_chunks 
                WHERE round_num IN (3, 4)
                ORDER BY event_time
            """)
            
            bt_chunks = cursor.fetchall()
            
            for round_num, chunk_id, source_client, event_time, chunk_hash in bt_chunks:
                all_events.append((event_time, client_id, 'bt_receive', f"轮次{round_num}_chunk{chunk_id}来自客户端{source_client}"))
                
                try:
                    time_str = datetime.fromtimestamp(event_time).strftime('%H:%M:%S.%f')[:-3]
                except:
                    time_str = str(event_time)
                
                if chunk_id == 9:  # 重点关注chunk_9
                    print(f"   🎯 {time_str}: 接收轮次{round_num}_chunk{chunk_id}来自客户端{source_client} (重点)")
                elif chunk_id in [0, 1, 8]:  # 关键chunk  
                    print(f"   📥 {time_str}: 接收轮次{round_num}_chunk{chunk_id}来自客户端{source_client}")
            
            # 检查BitTorrent会话信息
            cursor.execute("""
                SELECT round_num, start_time, end_time, status 
                FROM bt_sessions 
                WHERE round_num IN (3, 4)
                ORDER BY round_num
            """)
            
            sessions = cursor.fetchall()
            
            for round_num, start_time, end_time, status in sessions:
                try:
                    start_str = datetime.fromtimestamp(start_time).strftime('%H:%M:%S.%f')[:-3]
                    end_str = datetime.fromtimestamp(end_time).strftime('%H:%M:%S.%f')[:-3] if end_time else "进行中"
                except:
                    start_str = str(start_time)
                    end_str = str(end_time) if end_time else "进行中"
                
                print(f"   🔄 轮次{round_num}: BitTorrent会话 {start_str} - {end_str} ({status})")
                
                all_events.append((start_time, client_id, 'bt_start', f"轮次{round_num}会话开始"))
                if end_time:
                    all_events.append((end_time, client_id, 'bt_end', f"轮次{round_num}会话结束"))
                    
        finally:
            conn.close()
    
    # 2. 全局时间线分析
    print(f"\n📊 第二阶段：全局时间线分析")
    print("-" * 60)
    
    all_events.sort(key=lambda x: x[0])  # 按时间排序
    
    print("🕐 关键事件时间线 (只显示chunk_9和会话事件):")
    
    for timestamp, client_id, event_type, details in all_events:
        try:
            time_str = datetime.fromtimestamp(timestamp).strftime('%H:%M:%S.%f')[:-3]
        except:
            time_str = str(timestamp)
        
        # 只显示chunk_9和会话相关事件
        if 'chunk9' in details or 'chunk_9' in details or 'session' in event_type or 'bt_' in event_type:
            if event_type == 'local_gen':
                icon = "🏠"
            elif event_type == 'bt_receive':
                icon = "📥"
            elif event_type == 'bt_start':
                icon = "🚀"
            elif event_type == 'bt_end':
                icon = "🏁"
            else:
                icon = "📝"
            
            print(f"   {time_str} {icon} 客户端{client_id}: {details}")
    
    # 3. 分析chunk_9的时间窗口问题
    print(f"\n📊 第三阶段：chunk_9时间窗口问题分析")
    print("-" * 60)
    
    # 分析每个轮次的chunk_9生成和BitTorrent会话时间
    for round_num in [3, 4]:
        print(f"\n🔄 轮次 {round_num} 的时间窗口分析:")
        
        round_events = [(ts, client_id, event_type, details) for ts, client_id, event_type, details in all_events 
                       if f'轮次{round_num}' in details]
        
        # 找到chunk_9生成时间
        chunk9_gen_times = {}
        bt_session_ends = {}
        
        for timestamp, client_id, event_type, details in round_events:
            if event_type == 'local_gen' and f'chunk{9}' in details:
                chunk9_gen_times[client_id] = timestamp
            elif event_type == 'bt_end':
                bt_session_ends[client_id] = timestamp
        
        print("   📋 chunk_9生成时间 vs BitTorrent会话结束时间:")
        for client_id in sorted(chunk9_gen_times.keys()):
            try:
                gen_time_str = datetime.fromtimestamp(chunk9_gen_times[client_id]).strftime('%H:%M:%S.%f')[:-3]
            except:
                gen_time_str = str(chunk9_gen_times[client_id])
            
            if client_id in bt_session_ends:
                try:
                    end_time_str = datetime.fromtimestamp(bt_session_ends[client_id]).strftime('%H:%M:%S.%f')[:-3]
                    time_diff = bt_session_ends[client_id] - chunk9_gen_times[client_id]
                    if time_diff > 0:
                        status = f"✅ 会话在chunk_9生成后{time_diff:.1f}秒结束"
                    else:
                        status = f"⚠️ 会话在chunk_9生成前{abs(time_diff):.1f}秒就结束了!"
                except:
                    end_time_str = str(bt_session_ends[client_id])
                    status = "❓ 时间计算异常"
            else:
                end_time_str = "未知"
                status = "❓ 未找到会话结束时间"
            
            print(f"     客户端{client_id}: chunk_9生成{gen_time_str}, 会话结束{end_time_str} - {status}")
    
    # 4. 总结和建议
    print(f"\n🎯 结论和建议")
    print("-" * 60)
    
    print("🔍 发现的问题:")
    print("1. ⏱️ 时间窗口问题：chunk_9可能在BitTorrent会话结束后才生成")
    print("2. 🔄 轮次边界问题：新轮次开始时，上一轮次的BitTorrent交换可能已停止")
    print("3. 📡 交换优先级：BitTorrent可能优先交换早期生成的chunk")
    
    print("\n🔧 解决建议:")
    print("1. 📏 延长BitTorrent会话时间，确保所有chunk都有足够的交换窗口")
    print("2. 🎯 实现chunk_9的优先交换或专门处理逻辑")
    print("3. ⏰ 调整chunk生成时机，避免在会话结束前生成关键chunk")
    print("4. 🔄 增加轮次间的缓冲时间，让BitTorrent交换完成")

if __name__ == "__main__":
    analyze_bittorrent_timing()