#!/usr/bin/env python3
"""
测试客户端1能否从自己的数据库中提取出客户端2、3的所有chunk数据
"""

import sqlite3
import os
import pickle
import hashlib

def test_client1_extraction():
    """测试客户端1的跨节点chunk数据提取能力"""
    print("🔍 测试：客户端1能否提取出客户端2、3的所有chunk数据")
    print("=" * 80)
    
    client1_db = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_1/client_1_chunks.db"
    
    if not os.path.exists(client1_db):
        print("❌ 客户端1数据库文件不存在")
        return
    
    conn = sqlite3.connect(client1_db)
    cursor = conn.cursor()
    
    try:
        # 1. 分析客户端1数据库中其他节点的chunk信息
        print("📊 第一步：分析客户端1数据库中来自其他节点的chunk信息")
        print("-" * 60)
        
        # 获取来自客户端2、3的BitTorrent chunk记录
        for target_client in [2, 3]:
            print(f"\n🎯 来自客户端{target_client}的chunk记录:")
            
            cursor.execute("""
                SELECT round_num, chunk_id, chunk_hash, 
                       datetime(received_time,'unixepoch') as received_time
                FROM bt_chunks 
                WHERE source_client_id = ?
                ORDER BY round_num, chunk_id
            """, (target_client,))
            
            bt_records = cursor.fetchall()
            
            print(f"   总记录数: {len(bt_records)} 条")
            
            # 按轮次分组显示
            current_round = None
            for round_num, chunk_id, chunk_hash, received_time in bt_records:
                if round_num != current_round:
                    print(f"\n   【轮次 {round_num}】")
                    current_round = round_num
                
                chunk_type = "🗂️空" if chunk_hash == hashlib.sha256(b'').hexdigest() else "📦非空"
                print(f"     chunk{chunk_id:2d}: {chunk_type} {chunk_hash[:16]}... 时间:{received_time}")
        
        # 2. 测试数据提取功能
        print(f"\n📊 第二步：测试实际数据提取功能")
        print("-" * 60)
        
        for round_num in [3, 4]:
            print(f"\n🔄 轮次 {round_num} 数据提取测试:")
            
            for target_client in [2, 3]:
                print(f"\n   🎯 尝试提取客户端{target_client}的数据:")
                
                # 获取该客户端在该轮次的所有chunk
                cursor.execute("""
                    SELECT chunk_id, chunk_hash
                    FROM bt_chunks 
                    WHERE source_client_id = ? AND round_num = ?
                    ORDER BY chunk_id
                """, (target_client, round_num))
                
                target_chunks = cursor.fetchall()
                
                if not target_chunks:
                    print(f"     ❌ 没有找到客户端{target_client}在轮次{round_num}的chunk记录")
                    continue
                
                extractable_count = 0
                missing_count = 0
                
                for chunk_id, chunk_hash in target_chunks:
                    # 检查是否能从chunk_data中提取到实际数据
                    cursor.execute("""
                        SELECT data, length(data) as data_len
                        FROM chunk_data 
                        WHERE chunk_hash = ?
                    """, (chunk_hash,))
                    
                    data_result = cursor.fetchone()
                    
                    chunk_type = "🗂️空" if chunk_hash == hashlib.sha256(b'').hexdigest() else "📦非空"
                    
                    if data_result:
                        data_blob, data_len = data_result
                        try:
                            # 尝试反序列化数据
                            chunk_data = pickle.loads(data_blob)
                            extractable_count += 1
                            print(f"     ✅ chunk{chunk_id:2d}: {chunk_type} 成功提取 ({data_len}字节)")
                        except Exception as e:
                            print(f"     ⚠️ chunk{chunk_id:2d}: {chunk_type} 数据损坏 ({e})")
                            missing_count += 1
                    else:
                        missing_count += 1
                        print(f"     ❌ chunk{chunk_id:2d}: {chunk_type} 数据缺失")
                
                success_rate = (extractable_count / len(target_chunks)) * 100 if target_chunks else 0
                print(f"     📊 提取结果: {extractable_count}/{len(target_chunks)} ({success_rate:.1f}%)")
        
        # 3. 详细分析chunk_data表中的数据来源
        print(f"\n📊 第三步：分析chunk_data表中的数据来源")
        print("-" * 60)
        
        cursor.execute("""
            SELECT chunk_hash, length(data) as data_len,
                   datetime(created_at,'unixepoch') as created_time
            FROM chunk_data 
            ORDER BY created_at
        """)
        
        all_data = cursor.fetchall()
        
        print(f"客户端1 chunk_data表总共有 {len(all_data)} 条记录:")
        
        local_count = 0
        bt_count = 0
        
        for i, (chunk_hash, data_len, created_time) in enumerate(all_data, 1):
            # 检查这个hash是否来自本地生成
            cursor.execute("""
                SELECT COUNT(*) FROM chunk_metadata WHERE chunk_hash = ?
            """, (chunk_hash,))
            local_refs = cursor.fetchone()[0]
            
            # 检查这个hash是否来自BitTorrent
            cursor.execute("""
                SELECT COUNT(*) FROM bt_chunks WHERE chunk_hash = ?
            """, (chunk_hash,))
            bt_refs = cursor.fetchone()[0]
            
            chunk_type = "🗂️空" if chunk_hash == hashlib.sha256(b'').hexdigest() else "📦非空"
            
            if local_refs > 0 and bt_refs == 0:
                source = f"🏠 纯本地"
                local_count += 1
            elif bt_refs > 0 and local_refs == 0:
                source = f"📥 纯BitTorrent"
                bt_count += 1
            elif local_refs > 0 and bt_refs > 0:
                source = f"🔄 本地+BitTorrent"
                local_count += 1  # 归类为本地
            else:
                source = "❓ 未知"
            
            print(f"   记录{i:2d}: {chunk_type} {chunk_hash[:16]}... {data_len:3d}字节 {created_time} {source}")
        
        print(f"\n📋 数据来源统计:")
        print(f"   🏠 本地生成数据: {local_count} 条")
        print(f"   📥 BitTorrent数据: {bt_count} 条") 
        print(f"   📊 总计: {local_count + bt_count} 条")
        
        # 4. 模拟完整的跨节点提取测试
        print(f"\n📊 第四步：模拟完整的跨节点提取测试")
        print("-" * 60)
        
        print("🎯 假设场景：客户端1需要重建客户端2、3在轮次3、4的完整模型")
        
        for round_num in [3, 4]:
            print(f"\n🔄 轮次 {round_num} 完整性测试:")
            
            for target_client in [2, 3]:
                # 应该有的chunk数量 (每轮次10个chunk)
                expected_chunks = list(range(10))
                
                # 实际能提取到的chunk
                cursor.execute("""
                    SELECT chunk_id, chunk_hash
                    FROM bt_chunks 
                    WHERE source_client_id = ? AND round_num = ?
                    ORDER BY chunk_id
                """, (target_client, round_num))
                
                available_chunks = cursor.fetchall()
                available_chunk_ids = [chunk_id for chunk_id, _ in available_chunks]
                
                # 检查每个chunk的数据可用性
                extractable_chunks = []
                for chunk_id, chunk_hash in available_chunks:
                    cursor.execute("""
                        SELECT COUNT(*) FROM chunk_data WHERE chunk_hash = ?
                    """, (chunk_hash,))
                    
                    if cursor.fetchone()[0] > 0:
                        extractable_chunks.append(chunk_id)
                
                missing_chunks = [cid for cid in expected_chunks if cid not in available_chunk_ids]
                unavailable_chunks = [cid for cid in available_chunk_ids if cid not in extractable_chunks]
                
                print(f"   客户端{target_client}:")
                print(f"     📋 应有chunk: {expected_chunks}")
                print(f"     📥 已接收chunk: {available_chunk_ids}")
                print(f"     ✅ 可提取chunk: {extractable_chunks}")
                print(f"     ❌ 缺失chunk: {missing_chunks}")
                print(f"     ⚠️ 不可用chunk: {unavailable_chunks}")
                
                completeness = len(extractable_chunks) / len(expected_chunks) * 100
                print(f"     🎯 完整性: {completeness:.1f}% ({len(extractable_chunks)}/10)")
                
                if completeness == 100:
                    print(f"     🎉 完美！可以完整重建客户端{target_client}在轮次{round_num}的模型")
                else:
                    print(f"     ⚠️ 不完整，缺少 {10-len(extractable_chunks)} 个chunk")
        
    finally:
        conn.close()

if __name__ == "__main__":
    test_client1_extraction()