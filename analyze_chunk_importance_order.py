#!/usr/bin/env python3
"""
分析chunk接收顺序与重要性分数的关系
检查BitTorrent是否按照重要性优先级接收chunk
"""
import sqlite3
import os
from typing import Dict, List, Tuple, Any
from datetime import datetime

def analyze_chunk_importance_ordering(db_path: str, client_name: str):
    """分析单个客户端的chunk重要性排序"""
    print(f"\n{'='*60}")
    print(f"📊 分析 {client_name} 的chunk重要性排序")
    print(f"{'='*60}")
    
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件不存在: {db_path}")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查表结构
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        print(f"📋 可用表: {tables}")
        
        # 分析chunk_metadata表
        if 'chunk_metadata' in tables:
            print(f"\n🔍 chunk_metadata表结构:")
            cursor.execute("PRAGMA table_info(chunk_metadata)")
            columns = cursor.fetchall()
            for col in columns:
                print(f"   {col[1]}: {col[2]}")
            
            # 获取重要性分数数据
            print(f"\n📊 重要性分数统计:")
            cursor.execute("""
                SELECT round_num, chunk_id, importance_score, pruning_method, created_at 
                FROM chunk_metadata 
                ORDER BY round_num, chunk_id
            """)
            importance_data = cursor.fetchall()
            
            if importance_data:
                print(f"   找到 {len(importance_data)} 条重要性记录")
                
                # 按轮次分组
                rounds = {}
                for row in importance_data:
                    round_num, chunk_id, importance_score, pruning_method, created_at = row
                    if round_num not in rounds:
                        rounds[round_num] = []
                    rounds[round_num].append({
                        'chunk_id': chunk_id,
                        'importance_score': float(importance_score) if importance_score else 0.0,
                        'pruning_method': pruning_method,
                        'created_at': created_at
                    })
                
                # 分析每一轮
                for round_num in sorted(rounds.keys()):
                    print(f"\n📈 Round {round_num} 重要性分析:")
                    round_data = rounds[round_num]
                    
                    # 按重要性排序
                    sorted_by_importance = sorted(round_data, 
                                                key=lambda x: x['importance_score'], 
                                                reverse=True)
                    
                    print(f"   Chunk数量: {len(round_data)}")
                    print(f"   重要性分数范围: {min(d['importance_score'] for d in round_data):.4f} - {max(d['importance_score'] for d in round_data):.4f}")
                    print(f"   按重要性排序 (前5个):")
                    for i, chunk in enumerate(sorted_by_importance[:5]):
                        print(f"     {i+1}. Chunk {chunk['chunk_id']}: {chunk['importance_score']:.4f} ({chunk['pruning_method']})")
            else:
                print("   ❌ 未找到重要性数据")
        
        # 分析bt_chunks表 - BitTorrent接收记录
        if 'bt_chunks' in tables:
            print(f"\n🔍 bt_chunks表结构:")
            cursor.execute("PRAGMA table_info(bt_chunks)")
            columns = cursor.fetchall()
            for col in columns:
                print(f"   {col[1]}: {col[2]}")
            
            # 获取BitTorrent接收数据
            print(f"\n📡 BitTorrent接收记录:")
            cursor.execute("""
                SELECT chunk_id, round_num, received_time, source_client_id, is_verified 
                FROM bt_chunks 
                ORDER BY round_num, received_time
            """)
            bt_data = cursor.fetchall()
            
            if bt_data:
                print(f"   找到 {len(bt_data)} 条接收记录")
                
                # 按轮次分组分析接收顺序
                bt_rounds = {}
                for row in bt_data:
                    chunk_id, round_num, received_time, source_client_id, is_verified = row
                    if round_num not in bt_rounds:
                        bt_rounds[round_num] = []
                    bt_rounds[round_num].append({
                        'chunk_id': chunk_id,
                        'received_time': received_time,
                        'source_client_id': source_client_id,
                        'is_verified': is_verified
                    })
                
                # 对比重要性顺序与接收顺序
                if 'chunk_metadata' in tables and importance_data:
                    print(f"\n🔍 重要性排序 vs 接收顺序对比:")
                    
                    for round_num in sorted(bt_rounds.keys()):
                        if round_num in rounds:
                            print(f"\n📈 Round {round_num}:")
                            
                            # 获取该轮次的重要性排序
                            importance_order = sorted(rounds[round_num], 
                                                    key=lambda x: x['importance_score'], 
                                                    reverse=True)
                            
                            # 获取该轮次的接收顺序
                            reception_order = sorted(bt_rounds[round_num], 
                                                   key=lambda x: x['received_time'])
                            
                            print(f"   重要性顺序 (前5个):")
                            for i, chunk in enumerate(importance_order[:5]):
                                print(f"     {i+1}. Chunk {chunk['chunk_id']}: {chunk['importance_score']:.4f}")
                            
                            print(f"   接收顺序 (前5个):")
                            for i, chunk in enumerate(reception_order[:5]):
                                # 查找对应的重要性分数
                                importance_score = next(
                                    (c['importance_score'] for c in importance_order 
                                     if c['chunk_id'] == chunk['chunk_id']), 
                                    0.0
                                )
                                print(f"     {i+1}. Chunk {chunk['chunk_id']}: 重要性{importance_score:.4f}, 时间{chunk['received_time']}")
                            
                            # 计算顺序相关性
                            if len(importance_order) > 1 and len(reception_order) > 1:
                                # Spearman rank correlation approximation
                                importance_ranks = {chunk['chunk_id']: i for i, chunk in enumerate(importance_order)}
                                reception_ranks = {chunk['chunk_id']: i for i, chunk in enumerate(reception_order)}
                                
                                common_chunks = set(importance_ranks.keys()) & set(reception_ranks.keys())
                                if len(common_chunks) > 1:
                                    rank_diffs = []
                                    for chunk_id in common_chunks:
                                        diff = importance_ranks[chunk_id] - reception_ranks[chunk_id]
                                        rank_diffs.append(diff * diff)
                                    
                                    n = len(common_chunks)
                                    spearman_approx = 1 - (6 * sum(rank_diffs)) / (n * (n * n - 1))
                                    print(f"   📊 排序相关性估计: {spearman_approx:.3f} (1.0=完全正相关, -1.0=完全负相关)")
            else:
                print("   ❌ 未找到BitTorrent接收记录")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 分析数据库时出错: {e}")
        import traceback
        traceback.print_exc()

def main():
    """主函数：分析所有客户端的chunk重要性排序"""
    print("🔍 分析Chunk重要性排序与BitTorrent接收顺序")
    print("目的：验证是否按照SNIP重要性分数优先接收chunk")
    
    tmp_dir = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    
    if not os.path.exists(tmp_dir):
        print(f"❌ tmp目录不存在: {tmp_dir}")
        return
    
    # 分析每个客户端的数据库
    for client_dir in sorted(os.listdir(tmp_dir)):
        client_path = os.path.join(tmp_dir, client_dir)
        if os.path.isdir(client_path) and client_dir.startswith('client_'):
            db_file = f"{client_dir}_chunks.db"
            db_path = os.path.join(client_path, db_file)
            if os.path.exists(db_path):
                analyze_chunk_importance_ordering(db_path, client_dir)
            else:
                print(f"\n⚠️ {client_dir}: 数据库文件不存在 ({db_file})")
    
    print(f"\n{'='*70}")
    print("✅ Chunk重要性排序分析完成")

if __name__ == "__main__":
    main()