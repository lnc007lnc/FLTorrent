#!/usr/bin/env python3
"""
跨节点chunk提取测试 - 轮次3、4
验证每个节点能否提取到网络中全部chunk数据
"""

import sqlite3
import os
import hashlib
import pickle
from collections import defaultdict

def cross_node_chunk_extraction_test():
    """跨节点chunk提取测试"""
    print("🔍 跨节点chunk提取测试 - 轮次3、4")
    print("=" * 80)
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    # 1. 收集所有节点在轮次3、4的chunk信息
    print("📊 第一阶段：收集所有节点的chunk信息")
    print("-" * 60)
    
    all_chunks = {}  # {round: {client: {chunk_id: chunk_hash}}}
    all_chunk_data = {}  # {client: {chunk_hash: data}}
    
    for client_id, db_path in client_dbs.items():
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            print(f"\n🔍 分析客户端 {client_id}:")
            
            # 收集轮次3、4的chunk信息
            cursor.execute("""
                SELECT round_num, chunk_id, chunk_hash 
                FROM chunk_metadata 
                WHERE round_num IN (3, 4)
                ORDER BY round_num, chunk_id
            """)
            
            local_chunks = cursor.fetchall()
            
            for round_num, chunk_id, chunk_hash in local_chunks:
                if round_num not in all_chunks:
                    all_chunks[round_num] = {}
                if client_id not in all_chunks[round_num]:
                    all_chunks[round_num][client_id] = {}
                all_chunks[round_num][client_id][chunk_id] = chunk_hash
            
            # 收集该节点可访问的所有chunk_data
            cursor.execute("""
                SELECT chunk_hash, data FROM chunk_data
            """)
            
            chunk_data_records = cursor.fetchall()
            all_chunk_data[client_id] = {}
            
            for chunk_hash, data in chunk_data_records:
                all_chunk_data[client_id][chunk_hash] = pickle.loads(data)
            
            print(f"   本地chunk记录: 轮次3({len(all_chunks.get(3, {}).get(client_id, {}))}) + 轮次4({len(all_chunks.get(4, {}).get(client_id, {}))})")
            print(f"   可访问chunk_data: {len(all_chunk_data[client_id])} 条记录")
            
        finally:
            conn.close()
    
    # 2. 分析网络中chunk的全局分布
    print(f"\n📊 第二阶段：网络chunk全局分布分析")
    print("-" * 60)
    
    for round_num in sorted(all_chunks.keys()):
        print(f"\n🔄 轮次 {round_num} 的chunk分布:")
        
        # 统计每个chunk_id在所有客户端的hash分布
        chunk_distribution = defaultdict(lambda: defaultdict(list))  # {chunk_id: {hash: [client_list]}}
        
        for client_id in all_chunks[round_num]:
            for chunk_id, chunk_hash in all_chunks[round_num][client_id].items():
                chunk_distribution[chunk_id][chunk_hash].append(client_id)
        
        for chunk_id in sorted(chunk_distribution.keys()):
            hash_info = chunk_distribution[chunk_id]
            print(f"   chunk_{chunk_id}:")
            for chunk_hash, clients in hash_info.items():
                chunk_type = "🗂️空" if chunk_hash == empty_hash else "📦非空"
                print(f"     {chunk_type} {chunk_hash[:16]}... → 客户端{clients}")
    
    # 3. 模拟跨节点提取测试
    print(f"\n🎯 第三阶段：跨节点chunk提取模拟测试")
    print("-" * 60)
    
    test_results = {}
    
    for round_num in sorted(all_chunks.keys()):
        print(f"\n🔄 轮次 {round_num} 提取测试:")
        test_results[round_num] = {}
        
        # 收集该轮次网络中的所有unique chunks
        network_chunks = set()  # {(source_client, chunk_id, chunk_hash)}
        for client_id in all_chunks[round_num]:
            for chunk_id, chunk_hash in all_chunks[round_num][client_id].items():
                network_chunks.add((client_id, chunk_id, chunk_hash))
        
        print(f"   网络中总chunk数: {len(network_chunks)} 个")
        
        # 对每个客户端进行提取测试
        for target_client in client_dbs.keys():
            print(f"\n   🏠 客户端 {target_client} 的提取测试:")
            
            extractable_count = 0
            missing_chunks = []
            extraction_details = []
            
            for source_client, chunk_id, chunk_hash in network_chunks:
                # 模拟提取：检查target_client是否有该chunk的数据
                if chunk_hash in all_chunk_data[target_client]:
                    extractable_count += 1
                    chunk_type = "🗂️空" if chunk_hash == empty_hash else "📦非空"
                    extraction_details.append(f"     ✅ 客户端{source_client}_chunk{chunk_id}: {chunk_type} 可提取")
                else:
                    missing_chunks.append((source_client, chunk_id, chunk_hash))
                    chunk_type = "🗂️空" if chunk_hash == empty_hash else "📦非空"
                    extraction_details.append(f"     ❌ 客户端{source_client}_chunk{chunk_id}: {chunk_type} 缺失")
            
            success_rate = (extractable_count / len(network_chunks)) * 100
            
            print(f"     📊 提取统计: {extractable_count}/{len(network_chunks)} ({success_rate:.1f}%)")
            
            if success_rate == 100:
                print(f"     🎉 完美！可提取网络中所有chunk数据")
            else:
                print(f"     ⚠️ 缺失 {len(missing_chunks)} 个chunk:")
                for source_client, chunk_id, chunk_hash in missing_chunks:
                    chunk_type = "🗂️空" if chunk_hash == empty_hash else "📦非空"
                    print(f"       - 客户端{source_client}_chunk{chunk_id}: {chunk_type} {chunk_hash[:16]}...")
            
            # 显示详细提取信息（只显示前10个）
            print(f"     📋 详细提取信息 (显示前10个):")
            for detail in extraction_details[:10]:
                print(detail)
            if len(extraction_details) > 10:
                print(f"       ... 省略其余 {len(extraction_details) - 10} 条记录")
            
            test_results[round_num][target_client] = {
                'success_rate': success_rate,
                'extractable': extractable_count,
                'total': len(network_chunks),
                'missing': len(missing_chunks)
            }
    
    # 4. 生成测试总结报告
    print(f"\n📋 第四阶段：测试总结报告")
    print("=" * 60)
    
    overall_success = True
    
    for round_num in sorted(test_results.keys()):
        print(f"\n🔄 轮次 {round_num} 总结:")
        round_perfect = True
        
        for client_id in sorted(test_results[round_num].keys()):
            result = test_results[round_num][client_id]
            status = "✅ 完美" if result['success_rate'] == 100 else "⚠️ 不完整"
            print(f"   客户端 {client_id}: {result['extractable']}/{result['total']} ({result['success_rate']:.1f}%) {status}")
            
            if result['success_rate'] != 100:
                round_perfect = False
                overall_success = False
        
        print(f"   轮次{round_num}状态: {'🎉 所有客户端都能完整提取' if round_perfect else '⚠️ 存在提取不完整的客户端'}")
    
    print(f"\n🎯 最终结论:")
    if overall_success:
        print("🎉 跨节点chunk提取测试 - 完全成功！")
        print("   ✅ 所有客户端都能提取到网络中的全部chunk数据")
        print("   ✅ BitTorrent chunk交换系统运行完美")
    else:
        print("⚠️ 跨节点chunk提取测试 - 发现问题")
        print("   ❌ 部分客户端无法提取完整的网络chunk数据")
        print("   🔧 建议检查BitTorrent交换和数据保存机制")

if __name__ == "__main__":
    cross_node_chunk_extraction_test()