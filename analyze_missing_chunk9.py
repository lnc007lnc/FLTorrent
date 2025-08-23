#!/usr/bin/env python3
"""
分析为什么chunk_9无法跨节点提取
深入调查BitTorrent交换机制的问题
"""

import sqlite3
import os
import hashlib

def analyze_missing_chunk9():
    """分析chunk_9缺失的原因"""
    print("🔍 分析：为什么每个节点都无法提取其他节点的chunk_9？")
    print("=" * 80)
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    # 1. 分析每个节点的chunk_9生成和交换情况
    print("📊 第一阶段：chunk_9的生成和交换分析")
    print("-" * 60)
    
    chunk9_info = {}  # {client: {round: {'local_hash': xxx, 'bt_received': []}}}
    
    for client_id, db_path in client_dbs.items():
        print(f"\n🔍 客户端 {client_id} 的chunk_9分析:")
        chunk9_info[client_id] = {}
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # 分析本地生成的chunk_9
            cursor.execute("""
                SELECT round_num, chunk_hash 
                FROM chunk_metadata 
                WHERE chunk_id = 9 AND round_num IN (3, 4)
                ORDER BY round_num
            """)
            
            local_chunk9 = cursor.fetchall()
            
            for round_num, chunk_hash in local_chunk9:
                chunk9_info[client_id][round_num] = {'local_hash': chunk_hash, 'bt_received': []}
                chunk_type = "🗂️空" if chunk_hash == empty_hash else "📦非空"
                print(f"   轮次{round_num}: 本地生成 {chunk_type} {chunk_hash[:16]}...")
            
            # 分析BitTorrent接收的chunk_9
            cursor.execute("""
                SELECT round_num, source_client_id, chunk_hash
                FROM bt_chunks 
                WHERE chunk_id = 9 AND round_num IN (3, 4)
                ORDER BY round_num, source_client_id
            """)
            
            bt_chunk9 = cursor.fetchall()
            
            for round_num, source_client, chunk_hash in bt_chunk9:
                if round_num in chunk9_info[client_id]:
                    chunk9_info[client_id][round_num]['bt_received'].append({
                        'source': source_client, 
                        'hash': chunk_hash
                    })
                chunk_type = "🗂️空" if chunk_hash == empty_hash else "📦非空"
                print(f"   轮次{round_num}: BitTorrent接收 来自客户端{source_client} {chunk_type} {chunk_hash[:16]}...")
            
            # 检查chunk_data中是否有这些hash的数据
            all_chunk9_hashes = set()
            for round_info in chunk9_info[client_id].values():
                all_chunk9_hashes.add(round_info['local_hash'])
                for bt_info in round_info['bt_received']:
                    all_chunk9_hashes.add(bt_info['hash'])
            
            cursor.execute(f"""
                SELECT chunk_hash FROM chunk_data 
                WHERE chunk_hash IN ({','.join(['?' for _ in all_chunk9_hashes])})
            """, list(all_chunk9_hashes))
            
            available_hashes = {row[0] for row in cursor.fetchall()}
            
            print(f"   💾 chunk_data中可用的chunk_9数据:")
            for chunk_hash in all_chunk9_hashes:
                status = "✅ 存在" if chunk_hash in available_hashes else "❌ 缺失"
                chunk_type = "🗂️空" if chunk_hash == empty_hash else "📦非空"
                print(f"     {chunk_type} {chunk_hash[:16]}... {status}")
                
        finally:
            conn.close()
    
    # 2. 分析chunk_9的交叉验证
    print(f"\n📊 第二阶段：chunk_9交叉验证分析")
    print("-" * 60)
    
    for round_num in [3, 4]:
        print(f"\n🔄 轮次 {round_num} chunk_9交叉验证:")
        
        print("   📋 各客户端的chunk_9情况:")
        for client_id in sorted(chunk9_info.keys()):
            if round_num in chunk9_info[client_id]:
                local_hash = chunk9_info[client_id][round_num]['local_hash']
                bt_received = chunk9_info[client_id][round_num]['bt_received']
                
                chunk_type = "🗂️空" if local_hash == empty_hash else "📦非空"
                print(f"     客户端{client_id}: 本地{chunk_type} {local_hash[:16]}...")
                
                for bt_info in bt_received:
                    bt_type = "🗂️空" if bt_info['hash'] == empty_hash else "📦非空"
                    print(f"       收到: 来自客户端{bt_info['source']} {bt_type} {bt_info['hash'][:16]}...")
        
        print("   🎯 应该交换但可能缺失的情况:")
        for target_client in sorted(chunk9_info.keys()):
            if round_num not in chunk9_info[target_client]:
                continue
                
            should_have = []  # 应该从其他节点收到的chunk_9
            actually_have = []  # 实际收到的chunk_9
            
            for source_client in sorted(chunk9_info.keys()):
                if source_client != target_client and round_num in chunk9_info[source_client]:
                    source_hash = chunk9_info[source_client][round_num]['local_hash']
                    should_have.append((source_client, source_hash))
            
            # 检查是否实际收到了
            bt_received_hashes = {bt['hash'] for bt in chunk9_info[target_client][round_num]['bt_received']}
            
            for source_client, source_hash in should_have:
                if source_hash in bt_received_hashes:
                    status = "✅ 已收到"
                    actually_have.append((source_client, source_hash))
                else:
                    status = "❌ 未收到"
                
                chunk_type = "🗂️空" if source_hash == empty_hash else "📦非空"
                print(f"     客户端{target_client} 应从客户端{source_client}收到: {chunk_type} {source_hash[:16]}... {status}")
    
    # 3. 分析原因和建议
    print(f"\n📊 第三阶段：原因分析和建议")
    print("-" * 60)
    
    print("🔍 可能的原因:")
    print("1. 📡 BitTorrent交换不完整:")
    print("   - chunk_9可能没有被完整请求或发送")
    print("   - BitTorrent协议可能优先交换某些chunk")
    print("   - 网络延迟导致chunk_9交换超时")
    
    print("2. 💾 数据保存策略问题:")
    print("   - chunk_9可能被特殊处理或优先清理") 
    print("   - 保存时机与其他chunk不同")
    print("   - 存储策略可能对最后的chunk有特殊规则")
    
    print("3. 🔄 轮次边界问题:")
    print("   - chunk_9可能在轮次结束时生成，来不及交换")
    print("   - 下一轮次开始前，BitTorrent会话可能已经结束")
    print("   - 时间窗口问题导致部分chunk未完成交换")
    
    print("\n🔧 建议解决方案:")
    print("1. 🔍 检查BitTorrent请求日志，确认chunk_9是否被正确请求")
    print("2. ⏱️ 分析chunk生成和交换的时间序列，找出时间窗口问题") 
    print("3. 📡 验证BitTorrent协议是否对所有chunk_id都进行了完整交换")
    print("4. 💾 检查chunk_9的保存逻辑是否与其他chunk一致")

if __name__ == "__main__":
    analyze_missing_chunk9()