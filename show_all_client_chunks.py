#!/usr/bin/env python3
"""
直接显示每个客户端中的所有chunk数据
清晰展示bt_chunks和chunk_data的具体内容
"""

import sqlite3
import os
import hashlib
import pickle
import numpy as np

def show_client_chunks(client_id, db_path):
    """显示单个客户端的所有chunk数据"""
    print(f"\n{'='*80}")
    print(f"🔍 客户端 {client_id} - 完整chunk数据")
    print(f"数据库路径: {db_path}")
    print('='*80)
    
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件不存在")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    try:
        # 1. 显示bt_chunks表（BitTorrent交换记录）
        print(f"\n📊 BitTorrent交换记录 (bt_chunks表)")
        print("-" * 80)
        
        cursor.execute("""
            SELECT round_num, source_client_id, chunk_id, chunk_hash, 
                   holder_client_id, received_time, is_verified
            FROM bt_chunks 
            ORDER BY round_num, source_client_id, chunk_id
        """)
        
        bt_records = cursor.fetchall()
        print(f"总记录数: {len(bt_records)}")
        
        # 按轮次组织显示
        current_round = None
        for round_num, source_client_id, chunk_id, chunk_hash, holder_client_id, received_time, is_verified in bt_records:
            
            if round_num != current_round:
                print(f"\n🔄 轮次 {round_num}:")
                current_round = round_num
            
            # 判断是否为空chunk
            is_empty = chunk_hash == empty_hash
            status_icon = "🔴" if is_empty else "🟢"
            
            print(f"  {status_icon} 源客户端{source_client_id}, 块{chunk_id}: {chunk_hash[:16]}... "
                  f"({'空' if is_empty else '非空'}, 验证={'✅' if is_verified else '❌'})")
        
        # 2. 显示chunk_data表（实际存储的数据）
        print(f"\n💾 实际存储的chunk数据 (chunk_data表)")
        print("-" * 80)
        
        cursor.execute("""
            SELECT chunk_hash, LENGTH(data), data, created_at
            FROM chunk_data
            ORDER BY created_at
        """)
        
        data_records = cursor.fetchall()
        print(f"存储的chunk数据条数: {len(data_records)}")
        
        if data_records:
            for i, (chunk_hash, data_length, data_blob, created_at) in enumerate(data_records, 1):
                print(f"\n  📋 数据记录 {i}:")
                print(f"     哈希: {chunk_hash}")
                print(f"     创建时间: {created_at}")
                print(f"     数据大小: {data_length} bytes")
                
                try:
                    # 反序列化并显示数据内容
                    unpickled_data = pickle.loads(data_blob)
                    print(f"     数据类型: {type(unpickled_data)}")
                    
                    if isinstance(unpickled_data, np.ndarray):
                        print(f"     numpy形状: {unpickled_data.shape}")
                        print(f"     numpy dtype: {unpickled_data.dtype}")
                        if unpickled_data.size > 0:
                            print(f"     数据范围: [{unpickled_data.min():.6f}, {unpickled_data.max():.6f}]")
                            print(f"     数据内容: {unpickled_data.flatten()[:10]}...")
                        else:
                            print(f"     内容: 空数组")
                    elif hasattr(unpickled_data, '__len__'):
                        print(f"     数据长度: {len(unpickled_data)}")
                        print(f"     数据内容: {str(unpickled_data)[:100]}...")
                    else:
                        print(f"     数据内容: {unpickled_data}")
                    
                    # 检查这个哈希是否被BitTorrent使用
                    cursor.execute("SELECT COUNT(*) FROM bt_chunks WHERE chunk_hash = ?", (chunk_hash,))
                    bt_usage = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM chunk_metadata WHERE chunk_hash = ?", (chunk_hash,))
                    local_usage = cursor.fetchone()[0]
                    
                    print(f"     数据来源: ", end="")
                    if bt_usage > 0 and local_usage > 0:
                        print("🔄 本地生成 + BitTorrent交换")
                    elif bt_usage > 0:
                        print("📥 BitTorrent交换")
                    elif local_usage > 0:
                        print("🏠 本地生成")
                    else:
                        print("❓ 未知")
                    
                except Exception as e:
                    print(f"     ❌ 反序列化失败: {e}")
        else:
            print("  ⚠️ 没有存储任何chunk数据")
        
        # 3. 显示chunk_metadata表（本地生成的chunk）
        print(f"\n🏠 本地生成的chunk (chunk_metadata表)")
        print("-" * 80)
        
        cursor.execute("""
            SELECT round_num, chunk_id, chunk_hash, parts_info, flat_size, created_at
            FROM chunk_metadata
            ORDER BY round_num, chunk_id
        """)
        
        meta_records = cursor.fetchall()
        print(f"本地生成的chunk数量: {len(meta_records)}")
        
        if meta_records:
            current_round = None
            for round_num, chunk_id, chunk_hash, parts_info, flat_size, created_at in meta_records:
                
                if round_num != current_round:
                    print(f"\n  🔄 轮次 {round_num} 本地chunks:")
                    current_round = round_num
                
                # 检查是否有对应的实际数据
                cursor.execute("SELECT COUNT(*) FROM chunk_data WHERE chunk_hash = ?", (chunk_hash,))
                has_data = cursor.fetchone()[0] > 0
                
                is_empty = chunk_hash == empty_hash
                status_icon = "🔴" if is_empty else "🟢"
                data_icon = "💾" if has_data else "❌"
                
                print(f"    {status_icon} 块{chunk_id}: {chunk_hash[:16]}... "
                      f"(大小={flat_size}, 数据={data_icon}, 时间={created_at})")
        else:
            print("  ⚠️ 没有本地生成的chunk")
        
        # 4. 统计总结
        print(f"\n📈 数据统计总结")
        print("-" * 80)
        
        # BitTorrent统计
        cursor.execute(f"SELECT COUNT(*) FROM bt_chunks WHERE chunk_hash = '{empty_hash}'")
        bt_empty_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM bt_chunks WHERE chunk_hash != '{empty_hash}'")
        bt_non_empty_count = cursor.fetchone()[0]
        
        print(f"BitTorrent chunk记录: {len(bt_records)} 条")
        print(f"  └─ 空chunk: {bt_empty_count} 条")
        print(f"  └─ 非空chunk: {bt_non_empty_count} 条")
        
        # chunk_data统计
        cursor.execute("SELECT COUNT(*) FROM chunk_data")
        stored_data_count = cursor.fetchone()[0]
        
        print(f"实际存储数据: {stored_data_count} 条")
        
        # 本地chunk统计
        cursor.execute(f"SELECT COUNT(*) FROM chunk_metadata WHERE chunk_hash = '{empty_hash}'")
        local_empty_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM chunk_metadata WHERE chunk_hash != '{empty_hash}'")
        local_non_empty_count = cursor.fetchone()[0]
        
        print(f"本地生成chunk: {len(meta_records)} 条")
        print(f"  └─ 空chunk: {local_empty_count} 条")
        print(f"  └─ 非空chunk: {local_non_empty_count} 条")
        
        # 数据覆盖率
        cursor.execute("""
            SELECT COUNT(*) FROM bt_chunks bc
            LEFT JOIN chunk_data cd ON bc.chunk_hash = cd.chunk_hash
            WHERE bc.chunk_hash != ? AND cd.chunk_hash IS NULL
        """, (empty_hash,))
        missing_bt_data = cursor.fetchone()[0]
        
        print(f"BitTorrent数据缺失: {missing_bt_data} 条非空chunk无实际数据")
        
        if bt_non_empty_count > 0:
            coverage_rate = (bt_non_empty_count - missing_bt_data) / bt_non_empty_count * 100
            print(f"BitTorrent数据完整性: {coverage_rate:.1f}%")
    
    finally:
        conn.close()

def main():
    """主函数"""
    print("🔍 显示所有客户端的chunk数据")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    for client_id, db_path in client_dbs.items():
        show_client_chunks(client_id, db_path)
    
    print(f"\n{'='*80}")
    print("✅ 所有客户端chunk数据显示完成")

if __name__ == "__main__":
    main()