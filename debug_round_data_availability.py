#!/usr/bin/env python3
"""
调试轮次数据可用性
检查实际存在哪些轮次的数据
"""

import sqlite3
import os
import hashlib

def check_actual_round_data():
    """检查实际存在的轮次数据"""
    print("🔍 检查实际轮次数据可用性")
    print("=" * 80)
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    for client_id, db_path in client_dbs.items():
        print(f"\n📊 客户端 {client_id}:")
        
        if not os.path.exists(db_path):
            print(f"❌ 数据库文件不存在")
            continue
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # 查看所有轮次的数据分布
            cursor.execute("""
                SELECT round_num, source_client_id, 
                       COUNT(*) as total_chunks,
                       SUM(CASE WHEN chunk_hash = ? THEN 1 ELSE 0 END) as empty_chunks,
                       SUM(CASE WHEN chunk_hash != ? THEN 1 ELSE 0 END) as non_empty_chunks
                FROM bt_chunks 
                GROUP BY round_num, source_client_id
                ORDER BY round_num, source_client_id
            """, (empty_hash, empty_hash))
            
            round_data = cursor.fetchall()
            print(f"   bt_chunks表轮次分布:")
            
            current_round = None
            for round_num, source_client_id, total, empty, non_empty in round_data:
                if round_num != current_round:
                    print(f"\n     🔄 轮次 {round_num}:")
                    current_round = round_num
                
                print(f"       源客户端{source_client_id}: 总计={total}, 空={empty}, 非空={non_empty}")
            
            # 查看chunk_data与轮次的关系
            print(f"\n   chunk_data与轮次关联:")
            cursor.execute("""
                SELECT cd.chunk_hash, cd.created_at, COUNT(DISTINCT bc.round_num) as round_count
                FROM chunk_data cd
                LEFT JOIN bt_chunks bc ON cd.chunk_hash = bc.chunk_hash
                GROUP BY cd.chunk_hash, cd.created_at
                ORDER BY cd.created_at
            """)
            
            chunk_data_info = cursor.fetchall()
            for i, (chunk_hash, created_at, round_count) in enumerate(chunk_data_info, 1):
                chunk_type = "空chunk" if chunk_hash == empty_hash else "非空chunk"
                print(f"     {i}. {chunk_type}: 在{round_count}个轮次中出现 (创建: {created_at})")
                
                # 显示具体在哪些轮次出现
                cursor.execute("""
                    SELECT DISTINCT round_num, source_client_id
                    FROM bt_chunks
                    WHERE chunk_hash = ?
                    ORDER BY round_num, source_client_id
                """, (chunk_hash,))
                
                appearances = cursor.fetchall()
                if appearances:
                    rounds_info = []
                    for round_num, source_client_id in appearances:
                        rounds_info.append(f"轮次{round_num}源{source_client_id}")
                    print(f"        出现位置: {', '.join(rounds_info[:5])}...")  # 只显示前5个
        
        finally:
            conn.close()

def find_available_cross_node_data():
    """寻找可用的跨节点数据"""
    print(f"\n{'='*80}")
    print("🔍 寻找实际可用的跨节点chunk数据")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    for target_client in [1, 2, 3]:
        print(f"\n📋 客户端 {target_client} 可提取的其他节点数据:")
        
        conn = sqlite3.connect(client_dbs[target_client])
        cursor = conn.cursor()
        
        try:
            other_clients = [c for c in [1, 2, 3] if c != target_client]
            
            # 查找所有轮次中其他客户端的非空chunk，并检查数据可用性
            for source_client in other_clients:
                print(f"\n   📊 来自客户端{source_client}的数据:")
                
                cursor.execute("""
                    SELECT bc.round_num, bc.chunk_id, bc.chunk_hash, bc.is_verified,
                           CASE WHEN cd.chunk_hash IS NOT NULL THEN 1 ELSE 0 END as has_data
                    FROM bt_chunks bc
                    LEFT JOIN chunk_data cd ON bc.chunk_hash = cd.chunk_hash
                    WHERE bc.source_client_id = ? AND bc.chunk_hash != ?
                    ORDER BY bc.round_num, bc.chunk_id
                """, (source_client, empty_hash))
                
                available_chunks = cursor.fetchall()
                
                if available_chunks:
                    # 按轮次分组
                    rounds = {}
                    for round_num, chunk_id, chunk_hash, is_verified, has_data in available_chunks:
                        if round_num not in rounds:
                            rounds[round_num] = []
                        rounds[round_num].append({
                            'chunk_id': chunk_id,
                            'hash': chunk_hash,
                            'verified': is_verified,
                            'has_data': has_data
                        })
                    
                    # 显示每个轮次的情况
                    for round_num in sorted(rounds.keys()):
                        chunks = rounds[round_num]
                        available_count = sum(1 for c in chunks if c['has_data'])
                        print(f"     轮次{round_num}: {len(chunks)}个非空chunk, {available_count}个有数据可提取")
                        
                        # 显示详细信息
                        for chunk in chunks:
                            status = "✅可提取" if chunk['has_data'] else "❌无数据"
                            verified = "✅已验证" if chunk['verified'] else "❌未验证"
                            print(f"       块{chunk['chunk_id']}: {status} ({verified})")
                else:
                    print(f"     ❌ 没有来自客户端{source_client}的非空chunk")
        
        finally:
            conn.close()

def test_successful_extraction_scenario():
    """测试成功提取场景"""
    print(f"\n{'='*80}")
    print("🧪 测试实际可成功提取的数据")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_1_db = os.path.join(base_path, "client_1", "client_1_chunks.db")
    
    conn = sqlite3.connect(client_1_db)
    cursor = conn.cursor()
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    try:
        # 查找客户端1中存在数据的所有非空chunk
        cursor.execute("""
            SELECT bc.round_num, bc.source_client_id, bc.chunk_id, bc.chunk_hash, cd.created_at
            FROM bt_chunks bc
            INNER JOIN chunk_data cd ON bc.chunk_hash = cd.chunk_hash
            WHERE bc.chunk_hash != ?
            ORDER BY bc.round_num, bc.source_client_id, bc.chunk_id
        """, (empty_hash,))
        
        extractable_chunks = cursor.fetchall()
        
        print(f"\n📊 客户端1中可提取的非空chunk数据:")
        print(f"   总数: {len(extractable_chunks)} 条")
        
        if extractable_chunks:
            # 按来源分组
            by_source = {}
            for round_num, source_client_id, chunk_id, chunk_hash, created_at in extractable_chunks:
                key = f"客户端{source_client_id}"
                if key not in by_source:
                    by_source[key] = []
                by_source[key].append({
                    'round': round_num,
                    'chunk_id': chunk_id,
                    'hash': chunk_hash[:16] + '...',
                    'created_at': created_at
                })
            
            for source, chunks in by_source.items():
                print(f"\n   📋 {source}的数据:")
                for chunk in chunks:
                    print(f"     轮次{chunk['round']}, 块{chunk['chunk_id']}: {chunk['hash']} (创建: {chunk['created_at']})")
                    
                    # 尝试实际提取数据验证
                    full_hash = chunk['hash'].replace('...', '')  # 需要完整哈希
                    cursor.execute("SELECT data FROM chunk_data WHERE chunk_hash LIKE ?", (chunk['hash'].replace('...', '%'),))
                    data_result = cursor.fetchone()
                    
                    if data_result:
                        try:
                            import pickle
                            unpickled_data = pickle.loads(data_result[0])
                            print(f"       ✅ 数据提取成功: {type(unpickled_data)}")
                        except Exception as e:
                            print(f"       ❌ 数据提取失败: {e}")
    
    finally:
        conn.close()

def main():
    """主函数"""
    check_actual_round_data()
    find_available_cross_node_data()
    test_successful_extraction_scenario()
    
    print(f"\n{'='*80}")
    print("🎯 调试结论:")
    print("1. 实际可用的轮次数据分布")
    print("2. 跨节点chunk数据的真实可提取性")
    print("3. 数据生存期策略的实际影响")

if __name__ == "__main__":
    main()