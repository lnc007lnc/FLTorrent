#!/usr/bin/env python3
"""
分析实际存储的chunk数据 vs 预期的chunk数据
找出chunk_data表中的数据到底是什么
"""

import sqlite3
import os
import hashlib
import pickle
import numpy as np
from collections import defaultdict

def analyze_stored_chunks():
    """分析实际存储的chunk数据"""
    print("🔍 分析chunk_data表中实际存储的数据")
    print("=" * 80)
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    
    stored_data_analysis = {}
    
    for client_id in [1, 2, 3]:
        db_path = os.path.join(base_path, f"client_{client_id}", f"client_{client_id}_chunks.db")
        print(f"\n📊 客户端 {client_id} 详细分析:")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 获取chunk_data详细信息
        cursor.execute("""
            SELECT chunk_hash, data, created_at
            FROM chunk_data
            ORDER BY created_at
        """)
        
        stored_records = cursor.fetchall()
        client_analysis = []
        
        for i, (chunk_hash, data_blob, created_at) in enumerate(stored_records, 1):
            print(f"\n   📋 记录 {i}:")
            print(f"      哈希: {chunk_hash}")
            print(f"      创建时间: {created_at}")
            print(f"      数据大小: {len(data_blob)} bytes")
            
            try:
                # 反序列化数据
                unpickled_data = pickle.loads(data_blob)
                print(f"      数据类型: {type(unpickled_data)}")
                
                if isinstance(unpickled_data, np.ndarray):
                    print(f"      numpy形状: {unpickled_data.shape}")
                    print(f"      numpy dtype: {unpickled_data.dtype}")
                    print(f"      元素数量: {unpickled_data.size}")
                    if unpickled_data.size > 0:
                        print(f"      数据范围: [{unpickled_data.min():.6f}, {unpickled_data.max():.6f}]")
                        print(f"      数据样本: {unpickled_data.flatten()[:5]}...")
                    else:
                        print(f"      空numpy数组")
                elif hasattr(unpickled_data, '__len__'):
                    print(f"      数据长度: {len(unpickled_data)}")
                    if len(unpickled_data) > 0:
                        print(f"      数据内容: {str(unpickled_data)[:100]}...")
                
                client_analysis.append({
                    'hash': chunk_hash,
                    'data': unpickled_data,
                    'type': type(unpickled_data),
                    'created_at': created_at
                })
                
            except Exception as e:
                print(f"      ❌ 反序列化失败: {e}")
                client_analysis.append({
                    'hash': chunk_hash,
                    'error': str(e),
                    'created_at': created_at
                })
        
        stored_data_analysis[client_id] = client_analysis
        
        # 检查这些chunk_hash是否在bt_chunks或chunk_metadata中
        print(f"\n   🔗 数据来源分析:")
        for record in client_analysis:
            chunk_hash = record['hash']
            
            # 检查是否在bt_chunks中（来自BitTorrent交换）
            cursor.execute("SELECT COUNT(*) FROM bt_chunks WHERE chunk_hash = ?", (chunk_hash,))
            bt_count = cursor.fetchone()[0]
            
            # 检查是否在chunk_metadata中（本地生成）
            cursor.execute("SELECT COUNT(*) FROM chunk_metadata WHERE chunk_hash = ?", (chunk_hash,))
            meta_count = cursor.fetchone()[0]
            
            print(f"      哈希 {chunk_hash[:16]}...: bt_chunks={bt_count}, chunk_metadata={meta_count}")
            
            if bt_count > 0 and meta_count == 0:
                print(f"        📥 来源：BitTorrent交换")
            elif bt_count == 0 and meta_count > 0:
                print(f"        🏠 来源：本地生成")
            elif bt_count > 0 and meta_count > 0:
                print(f"        🔄 来源：本地生成且通过BitTorrent交换")
            else:
                print(f"        ❓ 来源：未知")
        
        conn.close()
    
    return stored_data_analysis

def compare_across_clients(stored_data_analysis):
    """比较客户端间存储的数据"""
    print(f"\n🔍 跨客户端数据比较")
    print("=" * 60)
    
    # 收集所有哈希
    all_hashes = set()
    for client_data in stored_data_analysis.values():
        for record in client_data:
            if 'hash' in record:
                all_hashes.add(record['hash'])
    
    print(f"总共发现 {len(all_hashes)} 个不同的哈希")
    
    # 分析每个哈希在各客户端的情况
    for chunk_hash in sorted(all_hashes):
        print(f"\n📊 哈希 {chunk_hash[:16]}...:")
        
        hash_data = {}
        for client_id, client_data in stored_data_analysis.items():
            for record in client_data:
                if record.get('hash') == chunk_hash:
                    hash_data[client_id] = record
                    break
        
        if len(hash_data) > 1:
            # 多个客户端有这个哈希，检查数据是否相同
            print(f"   出现在客户端: {list(hash_data.keys())}")
            
            # 比较数据一致性
            first_client_data = None
            data_consistent = True
            
            for client_id, record in hash_data.items():
                if 'data' in record:
                    if first_client_data is None:
                        first_client_data = record['data']
                    else:
                        try:
                            if isinstance(first_client_data, np.ndarray) and isinstance(record['data'], np.ndarray):
                                if not np.array_equal(first_client_data, record['data']):
                                    data_consistent = False
                            elif first_client_data != record['data']:
                                data_consistent = False
                        except:
                            data_consistent = False
                
                print(f"   客户端 {client_id}: {record.get('type', '未知类型')}")
            
            print(f"   数据一致性: {'✅一致' if data_consistent else '❌不一致'}")
        else:
            # 只有一个客户端有这个哈希
            client_id, record = list(hash_data.items())[0]
            print(f"   仅在客户端 {client_id}: {record.get('type', '未知类型')}")

def investigate_chunk_generation_pattern():
    """调查chunk生成模式"""
    print(f"\n🔍 调查chunk生成模式")
    print("=" * 60)
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    
    for client_id in [1, 2, 3]:
        db_path = os.path.join(base_path, f"client_{client_id}", f"client_{client_id}_chunks.db")
        print(f"\n📋 客户端 {client_id} chunk生成分析:")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 查看chunk_metadata（本地生成的chunk）
        cursor.execute("""
            SELECT round_num, chunk_id, chunk_hash, parts_info, flat_size
            FROM chunk_metadata
            ORDER BY round_num, chunk_id
        """)
        
        local_chunks = cursor.fetchall()
        print(f"   本地生成的chunk数量: {len(local_chunks)}")
        
        if local_chunks:
            print("   本地chunk详情:")
            for round_num, chunk_id, chunk_hash, parts_info, flat_size in local_chunks[:5]:  # 只显示前5个
                print(f"     轮次{round_num}, 块{chunk_id}: 哈希={chunk_hash[:16]}..., 大小={flat_size}")
                
                # 检查这个哈希是否在chunk_data中有数据
                cursor.execute("SELECT COUNT(*) FROM chunk_data WHERE chunk_hash = ?", (chunk_hash,))
                data_exists = cursor.fetchone()[0] > 0
                print(f"       数据存在: {'✅是' if data_exists else '❌否'}")
        
        # 统计BitTorrent chunk中哪些应该有数据但没有
        empty_hash = hashlib.sha256(b'').hexdigest()
        cursor.execute("""
            SELECT COUNT(*) FROM bt_chunks bc
            LEFT JOIN chunk_data cd ON bc.chunk_hash = cd.chunk_hash
            WHERE bc.chunk_hash != ? AND cd.chunk_hash IS NULL
        """, (empty_hash,))
        
        missing_bt_data = cursor.fetchone()[0]
        print(f"   BitTorrent缺失数据的非空chunk: {missing_bt_data}")
        
        conn.close()

def main():
    """主函数"""
    # 分析存储的chunk数据
    stored_data_analysis = analyze_stored_chunks()
    
    # 比较客户端间的数据
    compare_across_clients(stored_data_analysis)
    
    # 调查chunk生成模式
    investigate_chunk_generation_pattern()
    
    print(f"\n{'='*80}")
    print("🎯 最终分析结论:")
    print("1. chunk_data表中存储的是什么类型的数据")
    print("2. 这些数据是本地生成的还是BitTorrent交换的")
    print("3. BitTorrent接收到的chunk数据是否被正确保存")

if __name__ == "__main__":
    main()