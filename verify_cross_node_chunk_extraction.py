#!/usr/bin/env python3
"""
验证跨节点chunk数据提取
假设一个节点要提取其他节点的chunk数据（考虑生存期限制，只有3/4轮可用）
"""

import sqlite3
import os
import hashlib
import pickle
import numpy as np
from collections import defaultdict

def simulate_chunk_extraction():
    """模拟跨节点chunk数据提取过程"""
    print("🔍 模拟跨节点chunk数据提取（考虑生存期限制）")
    print("=" * 80)
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    # 针对每个客户端，模拟提取其他节点的chunk数据
    for target_client in [1, 2, 3]:
        print(f"\n{'='*60}")
        print(f"🎯 客户端 {target_client} 提取其他节点的chunk数据")
        print(f"   约束条件：只能访问轮次3-4的数据（生存期限制）")
        
        target_db_path = client_dbs[target_client]
        if not os.path.exists(target_db_path):
            print(f"❌ 数据库文件不存在")
            continue
        
        conn = sqlite3.connect(target_db_path)
        cursor = conn.cursor()
        
        try:
            # 获取轮次3-4中其他节点的非空chunk记录
            other_clients = [c for c in [1, 2, 3] if c != target_client]
            
            print(f"\n📋 目标：提取来自客户端 {other_clients} 在轮次3-4的chunk数据")
            
            extraction_results = {}
            
            for round_num in [3, 4]:
                print(f"\n🔄 轮次 {round_num}:")
                round_results = {}
                
                for source_client in other_clients:
                    # 查找该源客户端在该轮次的所有非空chunk
                    cursor.execute("""
                        SELECT chunk_id, chunk_hash, is_verified
                        FROM bt_chunks 
                        WHERE round_num = ? AND source_client_id = ? AND chunk_hash != ?
                        ORDER BY chunk_id
                    """, (round_num, source_client, empty_hash))
                    
                    non_empty_chunks = cursor.fetchall()
                    print(f"   📊 源客户端{source_client}: 发现{len(non_empty_chunks)}个非空chunk")
                    
                    client_extraction = {
                        'expected': len(non_empty_chunks),
                        'extracted': 0,
                        'failed': 0,
                        'chunks': []
                    }
                    
                    for chunk_id, chunk_hash, is_verified in non_empty_chunks:
                        # 尝试从chunk_data表中提取实际数据
                        cursor.execute("SELECT data FROM chunk_data WHERE chunk_hash = ?", (chunk_hash,))
                        data_result = cursor.fetchone()
                        
                        chunk_info = {
                            'chunk_id': chunk_id,
                            'hash': chunk_hash[:16] + '...',
                            'verified': bool(is_verified),
                            'data_available': data_result is not None
                        }
                        
                        if data_result:
                            try:
                                # 尝试反序列化数据
                                unpickled_data = pickle.loads(data_result[0])
                                chunk_info['data_type'] = str(type(unpickled_data))
                                chunk_info['extraction_success'] = True
                                
                                if hasattr(unpickled_data, 'shape'):
                                    chunk_info['data_shape'] = unpickled_data.shape
                                elif hasattr(unpickled_data, '__len__'):
                                    chunk_info['data_size'] = len(unpickled_data)
                                
                                client_extraction['extracted'] += 1
                                print(f"      ✅ 块{chunk_id}: 数据提取成功 ({chunk_info['data_type']})")
                                
                            except Exception as e:
                                chunk_info['extraction_success'] = False
                                chunk_info['error'] = str(e)
                                client_extraction['failed'] += 1
                                print(f"      ❌ 块{chunk_id}: 反序列化失败 - {e}")
                        else:
                            chunk_info['extraction_success'] = False
                            chunk_info['error'] = "数据不存在"
                            client_extraction['failed'] += 1
                            print(f"      ❌ 块{chunk_id}: chunk_data中无对应数据")
                        
                        client_extraction['chunks'].append(chunk_info)
                    
                    round_results[source_client] = client_extraction
                
                extraction_results[round_num] = round_results
            
            # 统计提取成功率
            print(f"\n📈 客户端{target_client}的提取统计:")
            total_expected = 0
            total_extracted = 0
            total_failed = 0
            
            for round_num, round_data in extraction_results.items():
                round_expected = sum(client_data['expected'] for client_data in round_data.values())
                round_extracted = sum(client_data['extracted'] for client_data in round_data.values())
                round_failed = sum(client_data['failed'] for client_data in round_data.values())
                
                success_rate = (round_extracted / round_expected * 100) if round_expected > 0 else 0
                print(f"   轮次{round_num}: 期望{round_expected}, 成功{round_extracted}, 失败{round_failed} (成功率: {success_rate:.1f}%)")
                
                total_expected += round_expected
                total_extracted += round_extracted
                total_failed += round_failed
            
            overall_success_rate = (total_extracted / total_expected * 100) if total_expected > 0 else 0
            print(f"   🎯 总体: 期望{total_expected}, 成功{total_extracted}, 失败{total_failed} (成功率: {overall_success_rate:.1f}%)")
            
            # 分析提取失败的原因
            if total_failed > 0:
                print(f"\n🔍 提取失败原因分析:")
                failure_reasons = defaultdict(int)
                
                for round_data in extraction_results.values():
                    for client_data in round_data.values():
                        for chunk in client_data['chunks']:
                            if not chunk['extraction_success']:
                                failure_reasons[chunk.get('error', '未知错误')] += 1
                
                for reason, count in failure_reasons.items():
                    print(f"   - {reason}: {count} 次")
        
        finally:
            conn.close()

def analyze_data_availability_pattern():
    """分析数据可用性模式"""
    print(f"\n{'='*80}")
    print("🔍 分析chunk数据可用性模式")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    
    # 收集所有chunk_data记录的创建时间模式
    print(f"\n📊 chunk_data创建时间模式:")
    
    for client_id in [1, 2, 3]:
        db_path = os.path.join(base_path, f"client_{client_id}", f"client_{client_id}_chunks.db")
        print(f"\n   客户端{client_id}:")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 按创建时间排序查看chunk_data
        cursor.execute("""
            SELECT chunk_hash, created_at, LENGTH(data)
            FROM chunk_data
            ORDER BY created_at
        """)
        
        data_records = cursor.fetchall()
        empty_hash = hashlib.sha256(b'').hexdigest()
        
        for i, (chunk_hash, created_at, data_length) in enumerate(data_records, 1):
            chunk_type = "空chunk" if chunk_hash == empty_hash else "非空chunk"
            print(f"      {i}. {chunk_type}: {created_at} (大小: {data_length})")
        
        conn.close()

def simulate_chunk_request_scenario():
    """模拟具体的chunk请求场景"""
    print(f"\n{'='*80}")
    print("🎯 模拟具体的chunk请求场景")
    
    # 场景：客户端1需要客户端2在轮次4的所有模型参数chunk
    print(f"\n📋 场景描述：")
    print(f"   - 请求方：客户端1")
    print(f"   - 目标：获取客户端2在轮次4的所有非空chunk数据")
    print(f"   - 约束：只能从客户端1的数据库中查找")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_1_db = os.path.join(base_path, "client_1", "client_1_chunks.db")
    
    conn = sqlite3.connect(client_1_db)
    cursor = conn.cursor()
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    try:
        # 查找客户端2在轮次4的所有chunk记录
        cursor.execute("""
            SELECT chunk_id, chunk_hash, is_verified
            FROM bt_chunks 
            WHERE round_num = 4 AND source_client_id = 2
            ORDER BY chunk_id
        """, )
        
        all_chunks = cursor.fetchall()
        non_empty_chunks = [(cid, hash, verified) for cid, hash, verified in all_chunks if hash != empty_hash]
        
        print(f"\n📊 查询结果:")
        print(f"   - 客户端2轮次4总chunk数: {len(all_chunks)}")
        print(f"   - 非空chunk数: {len(non_empty_chunks)}")
        print(f"   - 空chunk数: {len(all_chunks) - len(non_empty_chunks)}")
        
        print(f"\n🔍 非空chunk提取详情:")
        extraction_success = 0
        
        for chunk_id, chunk_hash, is_verified in non_empty_chunks:
            print(f"   Chunk {chunk_id} (哈希: {chunk_hash[:16]}...):")
            print(f"     验证状态: {'✅已验证' if is_verified else '❌未验证'}")
            
            # 尝试获取数据
            cursor.execute("SELECT data, created_at FROM chunk_data WHERE chunk_hash = ?", (chunk_hash,))
            data_result = cursor.fetchone()
            
            if data_result:
                data_blob, created_at = data_result
                print(f"     数据状态: ✅可用 (创建时间: {created_at})")
                print(f"     数据大小: {len(data_blob)} bytes")
                
                try:
                    unpickled_data = pickle.loads(data_blob)
                    print(f"     数据类型: {type(unpickled_data)}")
                    if hasattr(unpickled_data, 'shape'):
                        print(f"     数据形状: {unpickled_data.shape}")
                    extraction_success += 1
                    print(f"     提取结果: ✅成功")
                except Exception as e:
                    print(f"     提取结果: ❌失败 ({e})")
            else:
                print(f"     数据状态: ❌不可用")
                print(f"     提取结果: ❌失败 (数据不存在)")
        
        success_rate = (extraction_success / len(non_empty_chunks) * 100) if non_empty_chunks else 0
        print(f"\n🎯 场景提取结果:")
        print(f"   成功提取: {extraction_success}/{len(non_empty_chunks)} ({success_rate:.1f}%)")
        
        if success_rate == 100.0:
            print(f"   ✅ 完全成功！客户端1可以完整获取客户端2轮次4的所有模型参数")
        elif success_rate > 0:
            print(f"   ⚠️ 部分成功，存在一些数据缺失")
        else:
            print(f"   ❌ 完全失败，无法获取任何数据")
    
    finally:
        conn.close()

def main():
    """主函数"""
    print("🔍 验证跨节点chunk数据提取能力")
    print("考虑数据生存期限制（轮次3-4可用）")
    
    # 模拟跨节点提取
    simulate_chunk_extraction()
    
    # 分析数据可用性模式
    analyze_data_availability_pattern()
    
    # 模拟具体场景
    simulate_chunk_request_scenario()
    
    print(f"\n{'='*80}")
    print("🎯 验证结论:")
    print("1. 跨节点chunk数据提取的可行性")
    print("2. 数据生存期限制对提取成功率的影响")
    print("3. BitTorrent chunk exchange系统的实际效果")
    print("4. 在保留期内，其他节点的chunk数据是否可完整提取")

if __name__ == "__main__":
    main()