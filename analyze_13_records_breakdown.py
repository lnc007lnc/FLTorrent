#!/usr/bin/env python3
"""
详细分析13条chunk_data记录的组成
解释为什么修复后每个客户端都有13条记录
"""

import sqlite3
import os
import hashlib
import pickle
from datetime import datetime

def analyze_13_records_breakdown():
    """详细分析13条记录的组成"""
    print("🔍 详细分析：为什么修复后每个客户端有13条chunk_data记录？")
    print("=" * 80)
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    for client_id, db_path in client_dbs.items():
        print(f"\n{'='*70}")
        print(f"🔍 客户端 {client_id} - 13条记录详细分解")
        print("=" * 70)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # 1. 获取所有chunk_data记录，按创建时间排序
            cursor.execute("""
                SELECT chunk_hash, created_at, length(data) as data_size
                FROM chunk_data 
                ORDER BY created_at
            """)
            
            chunk_data_records = cursor.fetchall()
            print(f"📊 总计chunk_data记录: {len(chunk_data_records)}条")
            
            # 2. 分析每条记录的来源
            local_count = 0
            bt_count = 0
            empty_count = 0
            
            print(f"\n📋 逐条记录分析:")
            
            for i, (chunk_hash, created_at, data_size) in enumerate(chunk_data_records, 1):
                # 检查是否是空chunk
                if chunk_hash == empty_hash:
                    chunk_type = "🗂️ 空chunk"
                    empty_count += 1
                else:
                    chunk_type = "📦 非空chunk"
                
                # 检查数据来源：本地 vs BitTorrent
                cursor.execute("""
                    SELECT COUNT(*) FROM chunk_metadata WHERE chunk_hash = ?
                """, (chunk_hash,))
                local_refs = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT COUNT(*) FROM bt_chunks WHERE chunk_hash = ?
                """, (chunk_hash,))
                bt_refs = cursor.fetchone()[0]
                
                if local_refs > 0 and bt_refs == 0:
                    source = f"🏠 纯本地生成 (local:{local_refs})"
                    local_count += 1
                elif bt_refs > 0 and local_refs == 0:
                    source = f"📥 纯BitTorrent交换 (bt:{bt_refs})"
                    bt_count += 1
                elif local_refs > 0 and bt_refs > 0:
                    source = f"🔄 本地+BitTorrent (local:{local_refs}, bt:{bt_refs})"
                    if local_refs >= bt_refs:
                        local_count += 1
                    else:
                        bt_count += 1
                else:
                    source = "❓ 未知来源"
                
                # 转换时间戳
                try:
                    created_time = datetime.fromtimestamp(created_at).strftime('%H:%M:%S')
                except:
                    created_time = str(created_at)
                
                print(f"   记录{i:2d}: {chunk_type} | {source}")
                print(f"          哈希: {chunk_hash[:16]}... | 大小: {data_size}字节 | 时间: {created_time}")
            
            print(f"\n📊 记录来源统计:")
            print(f"   🏠 本地生成: {local_count}条")
            print(f"   📥 BitTorrent交换: {bt_count}条") 
            print(f"   🗂️ 空chunk: {empty_count}条")
            print(f"   ✅ 总计: {local_count + bt_count + empty_count}条")
            
            # 3. 分析轮次信息
            print(f"\n🔄 轮次数据分析:")
            cursor.execute("""
                SELECT DISTINCT round_num FROM chunk_metadata 
                ORDER BY round_num
            """)
            local_rounds = [row[0] for row in cursor.fetchall()]
            
            cursor.execute("""
                SELECT DISTINCT round_num FROM bt_chunks 
                ORDER BY round_num
            """)
            bt_rounds = [row[0] for row in cursor.fetchall()]
            
            print(f"   📈 本地生成轮次: {local_rounds}")
            print(f"   📈 BitTorrent交换轮次: {bt_rounds}")
            
            # 4. 详细轮次分解
            print(f"\n🎯 轮次详细分解:")
            all_rounds = sorted(set(local_rounds + bt_rounds))
            for round_num in all_rounds:
                cursor.execute("""
                    SELECT COUNT(DISTINCT chunk_hash) 
                    FROM chunk_metadata 
                    WHERE round_num = ? AND chunk_hash != ?
                """, (round_num, empty_hash))
                local_chunks = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT COUNT(DISTINCT chunk_hash)
                    FROM bt_chunks 
                    WHERE round_num = ? AND chunk_hash != ?
                """, (round_num, empty_hash))
                bt_chunks = cursor.fetchone()[0]
                
                print(f"   轮次{round_num}: 本地{local_chunks}个chunk, BitTorrent{bt_chunks}个chunk")
                
        finally:
            conn.close()

if __name__ == "__main__":
    analyze_13_records_breakdown()