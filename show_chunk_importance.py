#!/usr/bin/env python3
"""
显示chunk重要度评分结果的工具脚本
"""

import os
import sys
import sqlite3
import argparse
from pathlib import Path

def show_chunk_importance(client_id=None, round_num=None, top_k=None):
    """显示chunk重要度分数"""
    
    if client_id:
        client_dirs = [f'tmp/client_{client_id}']
    else:
        # 查找所有客户端目录
        client_dirs = []
        for i in range(1, 6):  # 检查client_1到client_5
            if os.path.exists(f'tmp/client_{i}'):
                client_dirs.append(f'tmp/client_{i}')
    
    if not client_dirs:
        print("❌ 未找到任何客户端数据库")
        return
    
    print(f"🧠 Chunk重要度分析报告")
    print("=" * 60)
    
    for client_dir in client_dirs:
        client_num = client_dir.split('_')[-1]
        db_path = f"{client_dir}/client_{client_num}_chunks.db"
        
        if not os.path.exists(db_path):
            print(f"⚠️  客户端 {client_num}: 数据库不存在")
            continue
        
        print(f"\n📊 客户端 {client_num} ({db_path})")
        print("-" * 40)
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # 检查数据库结构
            cursor.execute("PRAGMA table_info(chunk_metadata)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'importance_score' not in columns:
                print("   ⚠️  数据库尚未升级，缺少importance_score字段")
                conn.close()
                continue
            
            # 获取可用轮次
            cursor.execute("SELECT DISTINCT round_num FROM chunk_metadata ORDER BY round_num")
            available_rounds = [row[0] for row in cursor.fetchall()]
            
            if not available_rounds:
                print("   ❌ 没有找到任何chunk数据")
                conn.close()
                continue
            
            # 确定要显示的轮次
            if round_num is not None:
                if round_num in available_rounds:
                    target_rounds = [round_num]
                else:
                    print(f"   ❌ 指定的轮次 {round_num} 不存在，可用轮次: {available_rounds}")
                    conn.close()
                    continue
            else:
                # 显示最后2轮
                target_rounds = available_rounds[-2:] if len(available_rounds) >= 2 else available_rounds
            
            print(f"   📈 可用轮次: {available_rounds}")
            print(f"   🔍 显示轮次: {target_rounds}")
            
            for r in target_rounds:
                print(f"\n   🏆 轮次 {r} - Top chunks (按重要度排序)")
                
                query = '''
                    SELECT chunk_id, importance_score, pruning_method, flat_size, 
                           substr(chunk_hash, 1, 8) as short_hash
                    FROM chunk_metadata 
                    WHERE round_num = ?
                    ORDER BY importance_score DESC
                '''
                
                if top_k:
                    query += f' LIMIT {top_k}'
                
                cursor.execute(query, (r,))
                chunk_data = cursor.fetchall()
                
                if not chunk_data:
                    print(f"      ❌ 轮次 {r} 没有chunk数据")
                    continue
                
                print(f"      {'排名':<4} {'Chunk':<6} {'重要度':<12} {'方法':<10} {'大小':<8} {'Hash'}")
                print(f"      {'-' * 4} {'-' * 6} {'-' * 12} {'-' * 10} {'-' * 8} {'-' * 8}")
                
                for rank, (chunk_id, importance_score, pruning_method, flat_size, short_hash) in enumerate(chunk_data, 1):
                    importance_score = importance_score if importance_score is not None else 0.0
                    pruning_method = pruning_method or 'unknown'
                    
                    # 添加星级标记
                    stars = ""
                    if importance_score >= 0.8:
                        stars = "⭐⭐⭐"
                    elif importance_score >= 0.6:
                        stars = "⭐⭐"
                    elif importance_score >= 0.3:
                        stars = "⭐"
                    
                    print(f"      {rank:<4} {chunk_id:<6} {importance_score:<12.6f} {pruning_method:<10} {flat_size:<8} {short_hash}... {stars}")
                
                # 显示统计信息
                scores = [row[1] for row in chunk_data if row[1] is not None]
                if scores:
                    avg_score = sum(scores) / len(scores)
                    max_score = max(scores)
                    min_score = min(scores)
                    std_score = (sum((s - avg_score) ** 2 for s in scores) / len(scores)) ** 0.5
                    
                    print(f"\n      📊 统计信息:")
                    print(f"         平均重要度: {avg_score:.6f}")
                    print(f"         最高重要度: {max_score:.6f}")
                    print(f"         最低重要度: {min_score:.6f}")
                    print(f"         标准差:     {std_score:.6f}")
                    print(f"         高重要度chunk (>0.7): {len([s for s in scores if s > 0.7])}")
                    print(f"         中重要度chunk (0.3-0.7): {len([s for s in scores if 0.3 <= s <= 0.7])}")
                    print(f"         低重要度chunk (<0.3): {len([s for s in scores if s < 0.3])}")
                
            conn.close()
            
        except Exception as e:
            print(f"   ❌ 分析失败: {e}")

def main():
    parser = argparse.ArgumentParser(description="显示chunk重要度评分结果")
    parser.add_argument('--client', '-c', type=int, help='指定客户端ID (1, 2, 3, ...)')
    parser.add_argument('--round', '-r', type=int, help='指定轮次')
    parser.add_argument('--top', '-t', type=int, help='只显示前K个重要度最高的chunk')
    
    args = parser.parse_args()
    
    show_chunk_importance(client_id=args.client, round_num=args.round, top_k=args.top)

if __name__ == "__main__":
    main()