#!/usr/bin/env python3
"""
交叉验证3个客户端的chunk数据
区分合法的空chunk vs 丢失的数据
"""

import sqlite3
import os
import hashlib
import pickle
from collections import defaultdict, Counter

def collect_all_chunk_info():
    """收集所有客户端的chunk信息"""
    print("🔍 收集所有客户端的chunk信息")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    # 数据结构：{(round_num, source_client_id, chunk_id): {client_id: chunk_info}}
    all_chunks = defaultdict(dict)
    all_chunk_data = {}  # {client_id: {chunk_hash: data_info}}
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    for client_id, db_path in client_dbs.items():
        if not os.path.exists(db_path):
            continue
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"📋 客户端 {client_id}:")
        
        # 收集bt_chunks信息
        cursor.execute("""
            SELECT round_num, source_client_id, chunk_id, chunk_hash, is_verified
            FROM bt_chunks 
            ORDER BY round_num, source_client_id, chunk_id
        """)
        
        bt_records = cursor.fetchall()
        print(f"   bt_chunks记录数: {len(bt_records)}")
        
        for round_num, source_client_id, chunk_id, chunk_hash, is_verified in bt_records:
            chunk_key = (round_num, source_client_id, chunk_id)
            all_chunks[chunk_key][client_id] = {
                'hash': chunk_hash,
                'verified': is_verified,
                'is_empty': chunk_hash == empty_hash
            }
        
        # 收集chunk_data信息
        cursor.execute("""
            SELECT chunk_hash, LENGTH(data), data
            FROM chunk_data
        """)
        
        data_records = cursor.fetchall()
        print(f"   chunk_data记录数: {len(data_records)}")
        
        client_data = {}
        for chunk_hash, data_length, data_blob in data_records:
            try:
                unpickled_data = pickle.loads(data_blob)
                client_data[chunk_hash] = {
                    'length': data_length,
                    'type': str(type(unpickled_data)),
                    'shape': getattr(unpickled_data, 'shape', None),
                    'size': getattr(unpickled_data, 'size', None) if hasattr(unpickled_data, 'size') else len(unpickled_data) if hasattr(unpickled_data, '__len__') else None
                }
            except Exception as e:
                client_data[chunk_hash] = {
                    'length': data_length,
                    'error': str(e)
                }
        
        all_chunk_data[client_id] = client_data
        conn.close()
    
    return all_chunks, all_chunk_data

def analyze_chunk_consistency(all_chunks):
    """分析chunk一致性"""
    print(f"\n🔍 分析chunk一致性")
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    # 统计信息
    total_chunks = len(all_chunks)
    consistent_chunks = 0
    empty_chunks = 0
    non_empty_chunks = 0
    hash_mismatches = 0
    
    print(f"总chunk数量: {total_chunks}")
    
    # 按轮次分析
    rounds_analysis = defaultdict(lambda: {'total': 0, 'empty': 0, 'non_empty': 0, 'consistent': 0})
    
    for chunk_key, client_data in all_chunks.items():
        round_num, source_client_id, chunk_id = chunk_key
        rounds_analysis[round_num]['total'] += 1
        
        # 检查所有客户端是否对这个chunk有一致的哈希
        hashes = set(info['hash'] for info in client_data.values())
        
        if len(hashes) == 1:
            # 哈希一致
            consistent_chunks += 1
            rounds_analysis[round_num]['consistent'] += 1
            
            chunk_hash = list(hashes)[0]
            if chunk_hash == empty_hash:
                empty_chunks += 1
                rounds_analysis[round_num]['empty'] += 1
            else:
                non_empty_chunks += 1
                rounds_analysis[round_num]['non_empty'] += 1
        else:
            # 哈希不一致
            hash_mismatches += 1
            print(f"   ❌ 哈希不一致: {chunk_key}")
            for client_id, info in client_data.items():
                print(f"      客户端{client_id}: {info['hash'][:16]}...")
    
    print(f"\n📊 总体统计:")
    print(f"   一致的chunk: {consistent_chunks}/{total_chunks} ({consistent_chunks/total_chunks*100:.1f}%)")
    print(f"   空chunk: {empty_chunks} ({empty_chunks/total_chunks*100:.1f}%)")
    print(f"   非空chunk: {non_empty_chunks} ({non_empty_chunks/total_chunks*100:.1f}%)")
    print(f"   哈希不一致: {hash_mismatches}")
    
    print(f"\n📈 按轮次分析:")
    for round_num in sorted(rounds_analysis.keys()):
        stats = rounds_analysis[round_num]
        print(f"   轮次 {round_num}: 总计={stats['total']}, 空={stats['empty']}, 非空={stats['non_empty']}, 一致={stats['consistent']}")

def analyze_data_availability(all_chunks, all_chunk_data):
    """分析数据可用性"""
    print(f"\n🔍 分析数据可用性")
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    # 统计每个客户端的数据覆盖率
    for client_id, client_data in all_chunk_data.items():
        print(f"\n📊 客户端 {client_id} 数据分析:")
        print(f"   chunk_data表记录数: {len(client_data)}")
        
        # 分析每个存储的chunk
        for chunk_hash, data_info in client_data.items():
            if chunk_hash == empty_hash:
                print(f"   🔴 空chunk数据: 长度={data_info['length']}")
            else:
                print(f"   🟢 非空chunk数据: 哈希={chunk_hash[:16]}..., 长度={data_info['length']}")
                if 'shape' in data_info and data_info['shape'] is not None:
                    print(f"      形状: {data_info['shape']}")
                if 'size' in data_info and data_info['size'] is not None:
                    print(f"      元素数: {data_info['size']}")
                if 'error' in data_info:
                    print(f"      ❌ 反序列化错误: {data_info['error']}")
    
    # 交叉验证：检查哪些非空chunk在所有客户端都缺失数据
    print(f"\n🔗 交叉验证数据可用性:")
    
    # 收集所有非空chunk的哈希
    all_non_empty_hashes = set()
    for chunk_key, client_data in all_chunks.items():
        for client_id, info in client_data.items():
            if not info['is_empty']:
                all_non_empty_hashes.add(info['hash'])
    
    print(f"   发现 {len(all_non_empty_hashes)} 个唯一的非空chunk哈希")
    
    # 检查每个非空哈希在chunk_data中的可用性
    available_hashes = set()
    missing_hashes = set()
    
    for chunk_hash in all_non_empty_hashes:
        found_in_any_client = False
        found_in_clients = []
        
        for client_id, client_data in all_chunk_data.items():
            if chunk_hash in client_data:
                found_in_any_client = True
                found_in_clients.append(client_id)
        
        if found_in_any_client:
            available_hashes.add(chunk_hash)
            print(f"   ✅ 哈希 {chunk_hash[:16]}... 在客户端 {found_in_clients} 中有数据")
        else:
            missing_hashes.add(chunk_hash)
            print(f"   ❌ 哈希 {chunk_hash[:16]}... 在所有客户端中都缺失数据")
    
    print(f"\n📋 数据可用性总结:")
    print(f"   有数据的非空chunk: {len(available_hashes)}/{len(all_non_empty_hashes)} ({len(available_hashes)/len(all_non_empty_hashes)*100:.1f}%)")
    print(f"   缺失数据的非空chunk: {len(missing_hashes)}")

def analyze_chunk_patterns(all_chunks):
    """分析chunk模式"""
    print(f"\n🔍 分析chunk模式")
    
    # 按源客户端和chunk_id分析
    source_patterns = defaultdict(lambda: defaultdict(list))
    
    for chunk_key, client_data in all_chunks.items():
        round_num, source_client_id, chunk_id = chunk_key
        
        # 获取这个chunk的哈希（假设一致）
        hashes = set(info['hash'] for info in client_data.values())
        if len(hashes) == 1:
            chunk_hash = list(hashes)[0]
            is_empty = list(client_data.values())[0]['is_empty']
            source_patterns[source_client_id][chunk_id].append({
                'round': round_num,
                'hash': chunk_hash,
                'is_empty': is_empty
            })
    
    print(f"\n📊 每个源客户端的chunk模式:")
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    for source_client_id in sorted(source_patterns.keys()):
        print(f"\n   源客户端 {source_client_id}:")
        chunk_patterns = source_patterns[source_client_id]
        
        for chunk_id in sorted(chunk_patterns.keys()):
            chunk_history = chunk_patterns[chunk_id]
            empty_count = sum(1 for c in chunk_history if c['is_empty'])
            non_empty_count = len(chunk_history) - empty_count
            
            print(f"     Chunk {chunk_id}: {len(chunk_history)}轮, 空={empty_count}, 非空={non_empty_count}")
            
            if non_empty_count > 0:
                # 显示非空chunk的详情
                non_empty_chunks = [c for c in chunk_history if not c['is_empty']]
                unique_hashes = set(c['hash'] for c in non_empty_chunks)
                print(f"       非空轮次: {[c['round'] for c in non_empty_chunks]}")
                print(f"       唯一哈希数: {len(unique_hashes)}")

def main():
    """主函数"""
    print("🔍 交叉验证BitTorrent chunk数据")
    print("=" * 80)
    
    # 收集所有数据
    all_chunks, all_chunk_data = collect_all_chunk_info()
    
    # 分析一致性
    analyze_chunk_consistency(all_chunks)
    
    # 分析数据可用性
    analyze_data_availability(all_chunks, all_chunk_data)
    
    # 分析chunk模式
    analyze_chunk_patterns(all_chunks)
    
    print(f"\n{'='*80}")
    print("🎯 交叉验证结论:")
    print("1. 验证chunk哈希在所有客户端间的一致性")
    print("2. 区分合法的空chunk vs 应该有数据但丢失的chunk")
    print("3. 确定BitTorrent数据存储的真实状况")

if __name__ == "__main__":
    main()