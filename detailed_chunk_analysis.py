#!/usr/bin/env python3
"""
详细的chunk数据分析 - 验证数据内容和哈希一致性
"""

import sqlite3
import os
import hashlib
import json
from collections import defaultdict

def analyze_detailed_chunk_content(conn, client_id):
    """详细分析chunk内容和一致性"""
    try:
        cursor = conn.cursor()
        
        print(f"\n📊 客户端 {client_id} 详细数据内容分析:")
        
        # 1. 分析bt_chunks表中的具体数据
        cursor.execute("""
            SELECT round_num, source_client_id, chunk_id, chunk_hash, 
                   holder_client_id, received_time, is_verified
            FROM bt_chunks 
            ORDER BY round_num, source_client_id, chunk_id
            LIMIT 10
        """)
        
        bt_samples = cursor.fetchall()
        print(f"   BitTorrent交换记录样本 (前10条):")
        for i, record in enumerate(bt_samples, 1):
            round_num, source_client_id, chunk_id, chunk_hash, holder_client_id, received_time, is_verified = record
            print(f"     {i}. 轮次={round_num}, 源={source_client_id}, 块ID={chunk_id}")
            print(f"        哈希={chunk_hash[:16] if chunk_hash else 'None'}..., 持有者={holder_client_id}, 验证={is_verified}")
        
        # 2. 分析chunk_data中的实际数据
        cursor.execute("""
            SELECT chunk_hash, LENGTH(data) as data_length, 
                   SUBSTR(data, 1, 50) as data_preview
            FROM chunk_data 
            LIMIT 5
        """)
        
        data_samples = cursor.fetchall()
        print(f"\n   存储数据样本 (前5条):")
        for i, record in enumerate(data_samples, 1):
            chunk_hash, data_length, data_preview = record
            print(f"     {i}. 哈希={chunk_hash[:16]}..., 长度={data_length}bytes")
            print(f"        数据预览={data_preview[:30] if data_preview else 'None'}...")
        
        # 3. 验证哈希一致性
        print(f"\n   哈希一致性验证:")
        cursor.execute("""
            SELECT cd.chunk_hash, cd.data
            FROM chunk_data cd
            LIMIT 3
        """)
        
        hash_samples = cursor.fetchall()
        for i, (stored_hash, data) in enumerate(hash_samples, 1):
            if data:
                # 计算实际数据的哈希
                actual_hash = hashlib.sha256(data).hexdigest()
                matches = stored_hash == actual_hash
                print(f"     样本{i}: 存储哈希vs实际哈希 = {'✅匹配' if matches else '❌不匹配'}")
                if not matches:
                    print(f"       存储: {stored_hash[:16]}...")
                    print(f"       实际: {actual_hash[:16]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ 详细内容分析失败: {e}")
        return False

def analyze_round_distribution(conn, client_id):
    """分析每轮的数据分布详情"""
    try:
        cursor = conn.cursor()
        
        print(f"\n🔍 客户端 {client_id} 轮次分布详细分析:")
        
        # 获取每轮的详细统计
        cursor.execute("""
            SELECT round_num, source_client_id, 
                   COUNT(*) as chunk_count,
                   COUNT(DISTINCT chunk_id) as unique_chunks,
                   COUNT(DISTINCT chunk_hash) as unique_hashes
            FROM bt_chunks 
            GROUP BY round_num, source_client_id
            ORDER BY round_num, source_client_id
        """)
        
        round_details = cursor.fetchall()
        
        current_round = None
        for record in round_details:
            round_num, source_client_id, chunk_count, unique_chunks, unique_hashes = record
            
            if round_num != current_round:
                print(f"\n   轮次 {round_num}:")
                current_round = round_num
            
            print(f"     来源客户端 {source_client_id}: {chunk_count}条记录, {unique_chunks}个唯一chunk, {unique_hashes}个唯一哈希")
        
        return True
        
    except Exception as e:
        print(f"❌ 轮次分布分析失败: {e}")
        return False

def verify_cross_client_consistency():
    """跨客户端数据一致性验证"""
    print(f"\n🌐 跨客户端数据一致性验证:")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    # 收集每个客户端的chunk哈希
    all_hashes = {}
    
    for client_id, db_path in client_dbs.items():
        if not os.path.exists(db_path):
            continue
            
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 获取该客户端的所有chunk哈希
        cursor.execute("""
            SELECT round_num, source_client_id, chunk_id, chunk_hash
            FROM bt_chunks
            ORDER BY round_num, source_client_id, chunk_id
        """)
        
        client_hashes = defaultdict(lambda: defaultdict(dict))
        for record in cursor.fetchall():
            round_num, source_client_id, chunk_id, chunk_hash = record
            client_hashes[round_num][source_client_id][chunk_id] = chunk_hash
        
        all_hashes[client_id] = client_hashes
        conn.close()
    
    # 验证一致性
    if len(all_hashes) < 2:
        print("   ⚠️ 数据不足，无法进行跨客户端验证")
        return
    
    # 检查同一源和chunk的哈希是否一致
    all_rounds = set()
    for client_hashes in all_hashes.values():
        all_rounds.update(client_hashes.keys())
    
    consistency_issues = 0
    total_checks = 0
    
    for round_num in sorted(all_rounds):
        print(f"\n   验证轮次 {round_num}:")
        
        # 获取这轮所有可能的源客户端
        all_sources = set()
        for client_hashes in all_hashes.values():
            if round_num in client_hashes:
                all_sources.update(client_hashes[round_num].keys())
        
        for source_client in sorted(all_sources):
            # 检查每个源客户端的chunks是否在所有目标客户端中一致
            source_chunks = None
            for client_id, client_hashes in all_hashes.items():
                if round_num in client_hashes and source_client in client_hashes[round_num]:
                    current_chunks = client_hashes[round_num][source_client]
                    
                    if source_chunks is None:
                        source_chunks = current_chunks
                        reference_client = client_id
                    else:
                        # 比较chunk哈希
                        for chunk_id, chunk_hash in current_chunks.items():
                            total_checks += 1
                            if chunk_id in source_chunks:
                                if source_chunks[chunk_id] != chunk_hash:
                                    consistency_issues += 1
                                    print(f"     ❌ 不一致: 源{source_client}的chunk{chunk_id}")
                                    print(f"        客户端{reference_client}: {source_chunks[chunk_id][:16]}...")
                                    print(f"        客户端{client_id}: {chunk_hash[:16]}...")
        
        # 统计这轮的一致性
        round_chunks = 0
        for client_hashes in all_hashes.values():
            if round_num in client_hashes:
                for source_chunks in client_hashes[round_num].values():
                    round_chunks += len(source_chunks)
        
        if round_chunks > 0:
            print(f"     该轮次总chunk数: {round_chunks}")
    
    print(f"\n   总体一致性检查: {total_checks - consistency_issues}/{total_checks} 通过")
    if consistency_issues == 0:
        print("   ✅ 所有跨客户端数据哈希完全一致！")
    else:
        print(f"   ⚠️ 发现 {consistency_issues} 个不一致的哈希")

def test_data_recovery():
    """测试数据恢复能力"""
    print(f"\n🔧 数据恢复能力测试:")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    
    # 随机选择一个客户端测试数据恢复
    client_1_db = os.path.join(base_path, "client_1", "client_1_chunks.db")
    
    if not os.path.exists(client_1_db):
        print("   ❌ 测试数据库不存在")
        return
    
    conn = sqlite3.connect(client_1_db)
    cursor = conn.cursor()
    
    try:
        # 1. 测试能否重构完整的chunk数据
        cursor.execute("""
            SELECT DISTINCT round_num FROM bt_chunks ORDER BY round_num LIMIT 1
        """)
        test_round = cursor.fetchone()
        
        if not test_round:
            print("   ⚠️ 没有数据可供测试")
            return
        
        test_round_num = test_round[0]
        print(f"   测试轮次 {test_round_num} 的数据恢复能力:")
        
        # 获取这轮的所有chunks
        cursor.execute("""
            SELECT source_client_id, chunk_id, chunk_hash
            FROM bt_chunks 
            WHERE round_num = ?
            ORDER BY source_client_id, chunk_id
        """, (test_round_num,))
        
        test_chunks = cursor.fetchall()
        print(f"     该轮次总chunk数: {len(test_chunks)}")
        
        # 统计每个源的chunks
        source_counts = defaultdict(int)
        for source_client_id, chunk_id, chunk_hash in test_chunks:
            source_counts[source_client_id] += 1
        
        print(f"     各源chunk分布: {dict(source_counts)}")
        
        # 2. 测试能否访问chunk的实际数据
        cursor.execute("""
            SELECT bc.chunk_hash, cd.data
            FROM bt_chunks bc
            LEFT JOIN chunk_data cd ON bc.chunk_hash = cd.chunk_hash
            WHERE bc.round_num = ?
            LIMIT 5
        """, (test_round_num,))
        
        data_samples = cursor.fetchall()
        accessible_data = sum(1 for _, data in data_samples if data is not None)
        
        print(f"     数据可访问性: {accessible_data}/{len(data_samples)} chunk有实际数据")
        
        if accessible_data > 0:
            print("   ✅ 数据恢复测试通过 - 可以访问chunk的实际内容")
        else:
            print("   ⚠️ 数据恢复测试部分通过 - 有元数据但缺少实际数据")
        
    finally:
        conn.close()

def main():
    """主函数"""
    print("🔍 开始详细的chunk数据分析")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    # 详细分析每个客户端
    for client_id, db_path in client_dbs.items():
        if not os.path.exists(db_path):
            continue
        
        print(f"\n{'='*60}")
        print(f"🔍 客户端 {client_id} 详细分析")
        
        conn = sqlite3.connect(db_path)
        try:
            analyze_detailed_chunk_content(conn, client_id)
            analyze_round_distribution(conn, client_id)
        finally:
            conn.close()
    
    # 跨客户端一致性验证
    verify_cross_client_consistency()
    
    # 数据恢复测试
    test_data_recovery()
    
    print(f"\n{'='*60}")
    print("✅ 详细chunk数据分析完成!")

if __name__ == "__main__":
    main()