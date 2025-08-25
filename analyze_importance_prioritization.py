#!/usr/bin/env python3

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from collections import defaultdict
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_cross_client_importance_prioritization():
    """分析所有客户端的重要度优先处理情况"""
    
    # 数据库路径
    db_paths = {
        'client_1': 'multi_process_test_v2/client_1_output/chunk_storage.db',
        'client_2': 'multi_process_test_v2/client_2_output/chunk_storage.db', 
        'client_3': 'multi_process_test_v2/client_3_output/chunk_storage.db'
    }
    
    logger.info("🔍 开始跨客户端重要度优先处理分析...")
    
    all_client_data = {}
    
    for client_name, db_path in db_paths.items():
        if not Path(db_path).exists():
            logger.warning(f"❌ 数据库不存在: {db_path}")
            continue
            
        logger.info(f"📊 分析 {client_name} 数据库...")
        
        try:
            conn = sqlite3.connect(db_path)
            
            # 获取chunk重要度分数
            importance_query = """
            SELECT round_num, source_id, chunk_id, importance_score, timestamp
            FROM chunk_importance 
            ORDER BY round_num, timestamp
            """
            importance_df = pd.read_sql_query(importance_query, conn)
            
            # 获取接收到的chunk数据
            received_query = """
            SELECT round_num, source_id, chunk_id, timestamp, data_size
            FROM chunk_data
            WHERE source_id != ?
            ORDER BY round_num, timestamp
            """
            # 获取当前客户端ID（假设client_1的ID是1，client_2的ID是2，etc）
            client_id = int(client_name.split('_')[1])
            received_df = pd.read_sql_query(received_query, conn, params=(client_id,))
            
            conn.close()
            
            # 合并重要度和接收数据
            if not received_df.empty and not importance_df.empty:
                merged_df = received_df.merge(
                    importance_df, 
                    on=['round_num', 'source_id', 'chunk_id'], 
                    how='left'
                )
                
                # 按时间戳排序，获取接收顺序
                merged_df = merged_df.sort_values('timestamp_x')  # timestamp_x来自received_df
                
                all_client_data[client_name] = {
                    'importance_df': importance_df,
                    'received_df': received_df,
                    'merged_df': merged_df,
                    'client_id': client_id
                }
                
                logger.info(f"✅ {client_name}: 找到 {len(received_df)} 个接收到的chunk，{len(importance_df)} 个重要度分数")
            else:
                logger.warning(f"⚠️ {client_name}: 没有足够的数据进行分析")
                
        except Exception as e:
            logger.error(f"❌ 分析 {client_name} 时出错: {e}")
    
    if not all_client_data:
        logger.error("❌ 没有找到任何有效的客户端数据")
        return
        
    logger.info("\n" + "="*80)
    logger.info("📈 重要度优先处理分析结果")
    logger.info("="*80)
    
    total_importance_correlation = []
    total_perfect_order_ratio = []
    
    for client_name, data in all_client_data.items():
        merged_df = data['merged_df']
        
        if merged_df.empty or merged_df['importance_score'].isna().all():
            logger.warning(f"⚠️ {client_name}: 没有有效的重要度-接收数据")
            continue
            
        logger.info(f"\n🎯 {client_name.upper()} 分析:")
        
        # 1. 按重要度排序的理想顺序 vs 实际接收顺序
        valid_chunks = merged_df.dropna(subset=['importance_score'])
        
        if len(valid_chunks) == 0:
            logger.warning(f"⚠️ {client_name}: 没有有效的重要度分数")
            continue
            
        # 理想顺序（按重要度降序）
        ideal_order = valid_chunks.sort_values('importance_score', ascending=False)
        
        # 实际接收顺序（按时间戳）
        actual_order = valid_chunks.sort_values('timestamp_x')
        
        logger.info(f"📊 总共处理了 {len(valid_chunks)} 个chunk")
        
        # 2. 计算Spearman相关系数（重要度与接收顺序的相关性）
        if len(valid_chunks) > 1:
            # 为每个chunk分配接收顺序号（1表示最早接收）
            actual_order_rank = actual_order.reset_index(drop=True)
            actual_order_rank['reception_order'] = range(1, len(actual_order_rank) + 1)
            
            # 计算重要度与接收顺序的相关性（期望负相关：重要度高的应该先接收）
            correlation = np.corrcoef(
                actual_order_rank['importance_score'], 
                actual_order_rank['reception_order']
            )[0, 1]
            
            total_importance_correlation.append(correlation)
            
            logger.info(f"📈 重要度-接收顺序相关系数: {correlation:.4f}")
            logger.info(f"   (期望负值：重要度越高，接收顺序越靠前)")
        
        # 3. 分析前N个chunk的重要度匹配情况
        top_n_values = [3, 5, min(10, len(valid_chunks))]
        
        for n in top_n_values:
            if n > len(valid_chunks):
                continue
                
            # 理想前N个（重要度最高）
            ideal_top_n = set(ideal_order.head(n).index)
            
            # 实际前N个（最早接收）
            actual_top_n = set(actual_order.head(n).index)
            
            # 计算匹配率
            match_count = len(ideal_top_n.intersection(actual_top_n))
            match_ratio = match_count / n
            
            if n == 3:
                total_perfect_order_ratio.append(match_ratio)
            
            logger.info(f"🎯 前{n}个chunk匹配率: {match_ratio:.2%} ({match_count}/{n})")
        
        # 4. 显示具体的接收顺序vs重要度
        logger.info(f"\n📋 {client_name} 详细接收顺序 (前10个):")
        display_df = actual_order.head(10)[['round_num', 'source_id', 'chunk_id', 'importance_score', 'timestamp_x']]
        display_df['reception_rank'] = range(1, len(display_df) + 1)
        
        for _, row in display_df.iterrows():
            logger.info(f"   #{row['reception_rank']:2d}: Round {row['round_num']}, "
                       f"Source {row['source_id']}, Chunk {row['chunk_id']}, "
                       f"重要度: {row['importance_score']:.6f}")
        
        # 5. 重要度分布统计
        logger.info(f"\n📊 {client_name} 重要度分数统计:")
        importance_stats = valid_chunks['importance_score'].describe()
        logger.info(f"   平均值: {importance_stats['mean']:.6f}")
        logger.info(f"   标准差: {importance_stats['std']:.6f}") 
        logger.info(f"   最大值: {importance_stats['max']:.6f}")
        logger.info(f"   最小值: {importance_stats['min']:.6f}")
    
    # 6. 总体分析
    if total_importance_correlation:
        avg_correlation = np.mean(total_importance_correlation)
        logger.info(f"\n🌟 总体分析:")
        logger.info(f"📈 平均重要度-接收顺序相关系数: {avg_correlation:.4f}")
        
        if avg_correlation < -0.3:
            logger.info("✅ 优秀！算法显著优先处理高重要度chunk")
        elif avg_correlation < -0.1:
            logger.info("✅ 良好！算法倾向于优先处理高重要度chunk")
        elif avg_correlation < 0.1:
            logger.info("⚠️ 一般：算法对重要度的优先处理不明显")
        else:
            logger.info("❌ 较差：算法没有按重要度优先处理")
    
    if total_perfect_order_ratio:
        avg_top3_match = np.mean(total_perfect_order_ratio)
        logger.info(f"🎯 平均前3个chunk匹配率: {avg_top3_match:.2%}")
        
        if avg_top3_match > 0.8:
            logger.info("✅ 优秀！前3个chunk高度匹配重要度排序")
        elif avg_top3_match > 0.5:
            logger.info("✅ 良好！前3个chunk较好匹配重要度排序")
        elif avg_top3_match > 0.3:
            logger.info("⚠️ 一般：前3个chunk部分匹配重要度排序")
        else:
            logger.info("❌ 较差：前3个chunk很少匹配重要度排序")
    
    logger.info("\n" + "="*80)
    logger.info("✅ 跨客户端重要度优先处理分析完成")
    logger.info("="*80)

if __name__ == "__main__":
    analyze_cross_client_importance_prioritization()