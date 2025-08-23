#!/usr/bin/env python3
"""
调试轮次不匹配问题：BitTorrent请求round X但数据保存在round Y
"""

import sqlite3
import os

def debug_round_mismatch():
    """调试轮次不匹配问题"""
    print("🐛 轮次不匹配调试工具")
    print("分析BitTorrent请求轮次与数据保存轮次的不一致")
    print("=" * 60)
    
    for client_id in [1, 2, 3]:
        db_path = f"tmp/client_{client_id}/client_{client_id}_chunks.db"
        
        if not os.path.exists(db_path):
            print(f"❌ Client {client_id}: 数据库不存在")
            continue
            
        print(f"🔍 Client {client_id} 数据库分析:")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # 查看所有轮次的数据
            cursor.execute("SELECT DISTINCT round_num FROM chunk_metadata ORDER BY round_num")
            rounds = [row[0] for row in cursor.fetchall()]
            print(f"   💾 数据库中存在的轮次: {rounds}")
            
            # 检查每个轮次的chunk数量
            for round_num in rounds:
                cursor.execute("SELECT COUNT(*) FROM chunk_metadata WHERE round_num = ?", (round_num,))
                count = cursor.fetchone()[0]
                print(f"   📊 Round {round_num}: {count} chunks")
                
                # 显示前3个chunk的详细信息
                cursor.execute("SELECT chunk_id, chunk_hash FROM chunk_metadata WHERE round_num = ? LIMIT 3", (round_num,))
                chunks = cursor.fetchall()
                for chunk_id, chunk_hash in chunks:
                    print(f"      - Chunk {chunk_id}: {chunk_hash[:8]}...")
            
            print()
            
        except Exception as e:
            print(f"   ❌ 查询失败: {e}")
        finally:
            conn.close()
    
    print("🔍 日志中BitTorrent请求的轮次分析:")
    
    # 从日志中提取BitTorrent轮次信息
    log_files = [
        "multi_process_test_v2/logs/client_1.log",
        "multi_process_test_v2/logs/client_2.log", 
        "multi_process_test_v2/logs/client_3.log"
    ]
    
    for log_file in log_files:
        if os.path.exists(log_file):
            client_id = log_file.split('_')[-1].split('.')[0]
            print(f"📋 Client {client_id} 日志分析:")
            
            try:
                with open(log_file, 'r') as f:
                    content = f.read()
                
                # 查找BitTorrent轮次信息
                import re
                
                # 查找BitTorrent开始的轮次
                bt_start_matches = re.findall(r'BitTorrent will exchange chunks from round (\d+)', content)
                if bt_start_matches:
                    print(f"   🎯 BitTorrent交换轮次: {bt_start_matches}")
                
                # 查找处理请求时的轮次
                request_matches = re.findall(r'Handling request.*chunk \d+:\d+ \(round (\d+)\)', content)
                if request_matches:
                    unique_rounds = list(set(request_matches))
                    print(f"   📨 处理请求的轮次: {unique_rounds}")
                
                # 查找发送bitfield的轮次
                bitfield_matches = re.findall(r'My bitfield for round (\d+)', content)
                if bitfield_matches:
                    unique_rounds = list(set(bitfield_matches))
                    print(f"   📡 发送bitfield的轮次: {unique_rounds}")
                
                print()
                
            except Exception as e:
                print(f"   ❌ 日志分析失败: {e}")
    
    print("=" * 60)
    print("🎯 结论分析:")
    print("如果BitTorrent请求round 0，但数据库只有round 1，说明存在轮次不匹配问题")
    print("这可能是因为:")
    print("1. 数据保存时使用了错误的轮次号")
    print("2. BitTorrent触发时使用了错误的轮次号") 
    print("3. 轮次更新时机不一致")

if __name__ == "__main__":
    debug_round_mismatch()