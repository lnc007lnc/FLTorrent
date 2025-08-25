#!/usr/bin/env python3
"""
分析接收到的chunks，查看是否优先接收了高重要度chunks
"""

import sqlite3
import pandas as pd
import numpy as np
import os

def analyze_received_chunks():
    """分析接收到的chunks和重要度分数"""
    
    print("🔍 分析接收到的chunks和重要度分布")
    print("=" * 60)
    
    # 查找chunk数据库文件
    db_files = []
    for i in range(1, 4):  # 3个客户端
        db_path = f"tmp/client_{i}/client_{i}_chunks.db"
        if os.path.exists(db_path):
            db_files.append((i, db_path))
            print(f"✅ 找到客户端{i}数据库: {db_path}")
        else:
            print(f"❌ 未找到客户端{i}数据库: {db_path}")
    
    if not db_files:
        print("❌ 未找到任何chunk数据库文件")
        return
    
    print(f"\n📊 分析{len(db_files)}个客户端的数据...")
    
    for client_id, db_path in db_files:
        print(f"\n--- 客户端 {client_id} ---")
        
        try:
            conn = sqlite3.connect(db_path)
            
            # 获取本地生成的chunks及其重要度分数
            local_chunks = pd.read_sql_query("""
                SELECT round_num, chunk_id, importance_score, 
                       pruning_method, created_at
                FROM chunk_metadata
                ORDER BY round_num, importance_score DESC
            """, conn)
            
            # 获取接收到的chunks（通过BitTorrent）
            received_chunks = pd.read_sql_query("""
                SELECT round_num, source_client_id, chunk_id, 
                       holder_client_id, received_time
                FROM bt_chunks
                ORDER BY round_num, received_time
            """, conn)
            
            conn.close()
            
            print(f"  📦 本地chunks: {len(local_chunks)}")
            print(f"  📨 接收chunks: {len(received_chunks)}")
            
            if len(local_chunks) == 0:
                print("  ⚠️  无本地chunk数据")
                continue
                
            # 按轮次分析
            for round_num in sorted(local_chunks['round_num'].unique()):
                round_local = local_chunks[local_chunks['round_num'] == round_num]
                round_received = received_chunks[received_chunks['round_num'] == round_num]
                
                print(f"\n  🔄 轮次 {round_num}:")
                print(f"     本地chunks: {len(round_local)}")
                print(f"     接收chunks: {len(round_received)}")
                
                # 显示本地chunks的重要度分布
                if len(round_local) > 0:
                    local_importance = round_local['importance_score'].values
                    print(f"     本地重要度: 最高={max(local_importance):.6f}, 最低={min(local_importance):.6f}, 平均={np.mean(local_importance):.6f}")
                    
                    # 显示重要度排序的本地chunks
                    print(f"     本地chunks（按重要度排序）:")
                    for idx, row in round_local.head(10).iterrows():
                        print(f"       chunk{row['chunk_id']}: 重要度={row['importance_score']:.6f}")
                
                # 分析接收到的chunks
                if len(round_received) > 0:
                    print(f"     接收到的chunks:")
                    for idx, row in round_received.iterrows():
                        # 尝试找到对应的重要度分数
                        source_id = row['source_client_id']
                        chunk_id = row['chunk_id']
                        print(f"       从客户端{source_id}接收chunk{chunk_id} (holder={row['holder_client_id']})")
                    
                    # 如果接收的chunks来自其他客户端，我们无法直接比较重要度
                    # 但可以分析接收的chunks数量和模式
                    unique_sources = round_received['source_client_id'].unique()
                    print(f"     接收来源: {list(unique_sources)}")
                    
                    for source in unique_sources:
                        source_chunks = round_received[round_received['source_client_id'] == source]
                        print(f"       从客户端{source}: {len(source_chunks)}个chunks")
                
        except Exception as e:
            print(f"  ❌ 分析客户端{client_id}数据库时出错: {e}")
    
    print(f"\n{'='*60}")
    print("💡 重要度验证说明")
    print("="*60)
    print("由于BitTorrent exchange的工作机制：")
    print("1. 每个客户端只能看到自己的chunk重要度分数")
    print("2. 接收其他客户端chunks时，无法直接获得对方的重要度评估")
    print("3. 我们需要通过以下方式验证重要度优先选择：")
    print("   - 查看每个客户端本地生成的chunks重要度分布")
    print("   - 分析接收chunks的时间顺序和来源模式")
    print("   - 验证算法是否按预期工作（重要度高的先交换）")

if __name__ == "__main__":
    analyze_received_chunks()