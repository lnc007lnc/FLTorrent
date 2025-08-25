#!/usr/bin/env python3
"""
分析chunk数据库，验证在4秒超时限制下节点是否优先接收高重要度的chunks
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from typing import Dict, List, Tuple

def analyze_chunk_importance_selection():
    """分析chunk选择是否基于重要度优先"""
    
    print("🔍 分析chunk重要度选择验证")
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
    
    print(f"\n📊 分析{len(db_files)}个客户端的chunk接收数据...")
    
    all_results = {}
    
    for client_id, db_path in db_files:
        print(f"\n--- 客户端 {client_id} ---")
        
        try:
            conn = sqlite3.connect(db_path)
            
            # 获取所有接收到的chunks（从bt_chunks表）
            chunks_df = pd.read_sql_query("""
                SELECT round_num, source_id, chunk_id, importance_score
                FROM bt_chunks 
                ORDER BY round_num, created_at
            """, conn)
            
            # 获取所有chunk重要度分数（从chunk_metadata表 - 包含本地生成的chunks）
            importance_df = pd.read_sql_query("""
                SELECT round_num, chunk_id, importance_score
                FROM chunk_metadata
                ORDER BY round_num, importance_score DESC
            """, conn)
            
            conn.close()
            
            if len(chunks_df) == 0:
                print(f"  ⚠️  客户端{client_id}未接收到任何chunks")
                continue
                
            print(f"  📦 总接收chunks: {len(chunks_df)}")
            print(f"  📊 重要度分数记录: {len(importance_df)}")
            
            # 按轮次分析
            for round_num in sorted(chunks_df['round_num'].unique()):
                round_chunks = chunks_df[chunks_df['round_num'] == round_num]
                round_importance = importance_df[importance_df['round_num'] == round_num]
                
                print(f"\n  🔄 轮次 {round_num}:")
                print(f"     接收chunks: {len(round_chunks)}")
                
                if len(round_chunks) > 0:
                    # 分析接收到的chunks的重要度分布
                    received_importance = round_chunks['importance_score'].values
                    avg_importance = np.mean(received_importance)
                    max_importance = np.max(received_importance)
                    min_importance = np.min(received_importance)
                    
                    print(f"     重要度统计: 平均={avg_importance:.6f}, 最大={max_importance:.6f}, 最小={min_importance:.6f}")
                    
                    # 如果有重要度分数记录，对比全局分布
                    if len(round_importance) > 0:
                        all_importance = round_importance['importance_score'].values
                        global_avg = np.mean(all_importance)
                        global_max = np.max(all_importance)
                        
                        # 计算接收chunks在全局重要度排名中的位置
                        received_ranks = []
                        for imp_score in received_importance:
                            rank = np.sum(all_importance >= imp_score) / len(all_importance) * 100
                            received_ranks.append(rank)
                        
                        avg_rank_percentile = np.mean(received_ranks)
                        
                        print(f"     全局重要度对比: 全局平均={global_avg:.6f}, 全局最大={global_max:.6f}")
                        print(f"     接收chunks排名: 平均排名百分位={avg_rank_percentile:.1f}%")
                        
                        # 判断是否优先选择了高重要度chunks
                        if avg_rank_percentile <= 30:  # 前30%认为是高重要度优先
                            print(f"     ✅ 优先选择高重要度chunks (平均排名在前{avg_rank_percentile:.1f}%)")
                        elif avg_rank_percentile <= 60:
                            print(f"     ⚠️  中等重要度选择 (平均排名在前{avg_rank_percentile:.1f}%)")
                        else:
                            print(f"     ❌ 未优先选择高重要度chunks (平均排名仅在前{avg_rank_percentile:.1f}%)")
                        
                        # 存储结果供后续分析
                        if client_id not in all_results:
                            all_results[client_id] = {}
                        
                        all_results[client_id][round_num] = {
                            'received_count': len(round_chunks),
                            'avg_importance': avg_importance,
                            'avg_rank_percentile': avg_rank_percentile,
                            'total_available': len(round_importance)
                        }
                
                # 显示接收的chunks详情（前10个）
                if len(round_chunks) > 0:
                    print(f"     接收详情 (前10个):")
                    for idx, row in round_chunks.head(10).iterrows():
                        print(f"       源{row['source_id']}-chunk{row['chunk_id']}: 重要度={row['importance_score']:.6f}")
        
        except Exception as e:
            print(f"  ❌ 分析客户端{client_id}数据库时出错: {e}")
    
    # 总结分析
    print(f"\n{'='*60}")
    print("📈 总结分析")
    print("="*60)
    
    if all_results:
        total_high_priority = 0
        total_evaluations = 0
        
        for client_id, rounds in all_results.items():
            print(f"\n客户端 {client_id}:")
            for round_num, stats in rounds.items():
                percentile = stats['avg_rank_percentile']
                count = stats['received_count']
                total = stats['total_available']
                
                if percentile <= 30:
                    priority_level = "高重要度优先 ✅"
                    total_high_priority += 1
                elif percentile <= 60:
                    priority_level = "中等重要度 ⚠️"
                else:
                    priority_level = "低重要度选择 ❌"
                
                print(f"  轮次{round_num}: 接收{count}/{total} chunks, 平均排名前{percentile:.1f}% - {priority_level}")
                total_evaluations += 1
        
        if total_evaluations > 0:
            success_rate = (total_high_priority / total_evaluations) * 100
            print(f"\n🎯 重要度优先选择成功率: {total_high_priority}/{total_evaluations} = {success_rate:.1f}%")
            
            if success_rate >= 70:
                print("✅ 验证成功：节点在超时限制下成功优先选择高重要度chunks")
            elif success_rate >= 40:
                print("⚠️  部分成功：节点部分优先选择高重要度chunks")
            else:
                print("❌ 验证失败：节点未能优先选择高重要度chunks")
    else:
        print("❌ 无法进行总结分析，缺少有效数据")

if __name__ == "__main__":
    analyze_chunk_importance_selection()