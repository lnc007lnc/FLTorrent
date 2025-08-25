#!/usr/bin/env python3
"""
跨客户端重要度分析：对比各客户端的本地chunk重要度与其他客户端接收这些chunks的顺序
"""

import sqlite3
import pandas as pd
import numpy as np
import os

def cross_client_importance_analysis():
    """跨客户端重要度分析"""
    
    print("🔍 跨客户端重要度分析")
    print("=" * 60)
    
    # 收集所有客户端的本地chunk重要度
    all_client_importance = {}  # {client_id: {round: [(chunk_id, importance)]}}
    
    print("📊 收集各客户端本地chunk重要度...")
    
    for client_id in range(1, 4):
        db_path = f"tmp/client_{client_id}/client_{client_id}_chunks.db"
        if not os.path.exists(db_path):
            continue
            
        print(f"\n--- 客户端 {client_id} 本地chunks ---")
        
        conn = sqlite3.connect(db_path)
        local_chunks = pd.read_sql_query("""
            SELECT round_num, chunk_id, importance_score
            FROM chunk_metadata
            ORDER BY round_num, importance_score DESC
        """, conn)
        conn.close()
        
        all_client_importance[client_id] = {}
        
        for round_num in sorted(local_chunks['round_num'].unique()):
            round_chunks = local_chunks[local_chunks['round_num'] == round_num]
            
            # 按重要度排序
            chunks_by_importance = []
            for _, row in round_chunks.iterrows():
                chunks_by_importance.append((int(row['chunk_id']), row['importance_score']))
            
            all_client_importance[client_id][round_num] = chunks_by_importance
            
            print(f"  轮次 {round_num}: {len(chunks_by_importance)}个chunks")
            print(f"    重要度排序 (chunk_id: importance):")
            for i, (chunk_id, importance) in enumerate(chunks_by_importance[:5]):
                print(f"      {i+1}. chunk{chunk_id}: {importance:.6f}")
            if len(chunks_by_importance) > 5:
                print(f"      ... 还有{len(chunks_by_importance)-5}个")
    
    # 分析接收情况
    print(f"\n{'='*60}")
    print("📨 分析各客户端接收其他客户端chunks的情况")
    print("="*60)
    
    for receiver_id in range(1, 4):
        db_path = f"tmp/client_{receiver_id}/client_{receiver_id}_chunks.db"
        if not os.path.exists(db_path):
            continue
            
        print(f"\n--- 客户端 {receiver_id} 接收分析 ---")
        
        conn = sqlite3.connect(db_path)
        received_chunks = pd.read_sql_query("""
            SELECT round_num, source_client_id, chunk_id, received_time
            FROM bt_chunks
            ORDER BY round_num, source_client_id, received_time
        """, conn)
        conn.close()
        
        if len(received_chunks) == 0:
            print(f"  ⚠️  客户端{receiver_id}未接收任何chunks")
            continue
        
        # 按源客户端和轮次分组
        for round_num in sorted(received_chunks['round_num'].unique()):
            round_received = received_chunks[received_chunks['round_num'] == round_num]
            
            print(f"\n  🔄 轮次 {round_num}:")
            
            for source_id in sorted(round_received['source_client_id'].unique()):
                source_chunks = round_received[round_received['source_client_id'] == source_id].copy()
                source_chunks = source_chunks.sort_values('received_time')
                
                source_id_int = int(source_id)
                print(f"\n    📡 从客户端{source_id_int}接收:")
                
                # 获取接收的chunk顺序
                received_order = [int(row['chunk_id']) for _, row in source_chunks.iterrows()]
                print(f"       接收顺序: {received_order}")
                
                # 对比源客户端的重要度排序
                if source_id_int in all_client_importance and round_num in all_client_importance[source_id_int]:
                    source_importance = all_client_importance[source_id_int][round_num]
                    
                    # 创建重要度字典
                    importance_dict = {chunk_id: importance for chunk_id, importance in source_importance}
                    
                    # 获取接收chunks的重要度
                    received_importance = []
                    for chunk_id in received_order:
                        if chunk_id in importance_dict:
                            received_importance.append(importance_dict[chunk_id])
                        else:
                            received_importance.append(0.0)
                    
                    print(f"       接收重要度: {[f'{score:.6f}' for score in received_importance]}")
                    
                    # 理想顺序（按重要度排序）
                    ideal_order = [chunk_id for chunk_id, importance in source_importance]
                    ideal_importance = [importance for chunk_id, importance in source_importance]
                    
                    print(f"       理想顺序: {ideal_order}")
                    print(f"       理想重要度: {[f'{score:.6f}' for score in ideal_importance]}")
                    
                    # 分析匹配度
                    if len(received_order) > 0:
                        # 检查前N个chunks的匹配度
                        n = min(3, len(received_order), len(ideal_order))
                        top_n_received = set(received_order[:n])
                        top_n_ideal = set(ideal_order[:n])
                        
                        match_count = len(top_n_received & top_n_ideal)
                        match_rate = match_count / n * 100
                        
                        print(f"       前{n}个匹配度: {match_count}/{n} ({match_rate:.1f}%)")
                        
                        # 检查是否按重要度递减
                        is_decreasing = True
                        if len(received_importance) > 1:
                            for i in range(len(received_importance)-1):
                                if received_importance[i] < received_importance[i+1]:
                                    is_decreasing = False
                                    break
                        
                        # 计算重要度相关性
                        if len(received_importance) > 1 and len(set(received_importance)) > 1:
                            # 计算与理想排序的相关性
                            received_ranks = []
                            for chunk_id in received_order:
                                try:
                                    rank = ideal_order.index(chunk_id)
                                    received_ranks.append(rank)
                                except ValueError:
                                    received_ranks.append(len(ideal_order))  # 未找到的排在最后
                            
                            # 越小的rank越好（rank从0开始）
                            avg_rank = np.mean(received_ranks)
                            best_possible_rank = np.mean(range(len(received_order)))
                            
                            print(f"       平均排名: {avg_rank:.1f} (理想: {best_possible_rank:.1f})")
                        
                        # 总体评估
                        if match_rate >= 80:
                            print(f"       ✅ 优秀：重要度优先选择效果很好")
                        elif match_rate >= 50:
                            print(f"       ⚠️  良好：重要度优先选择效果中等")
                        elif is_decreasing and len(received_importance) > 1:
                            print(f"       ⚠️  可接受：接收顺序按重要度递减")
                        else:
                            print(f"       ❌ 较差：重要度优先选择效果不明显")
                else:
                    print(f"       ❌ 无法获取客户端{source_id_int}轮次{round_num}的重要度数据")
    
    # 总结
    print(f"\n{'='*60}")
    print("🎯 跨客户端重要度分析总结")
    print("="*60)
    print("分析完成！通过对比：")
    print("1. 各客户端本地chunk的重要度排序")  
    print("2. 其他客户端实际接收这些chunk的顺序")
    print("3. 可以验证重要度优先算法是否正确工作")
    print("4. 匹配度高说明算法工作良好，匹配度低可能需要调试")

if __name__ == "__main__":
    cross_client_importance_analysis()