#!/usr/bin/env python3
"""
测试chunk重要度评分系统
"""

import os
import sys
import sqlite3
from pathlib import Path

# 设置路径
sys.path.append('.')

def test_chunk_importance_scores():
    """测试chunk重要度分数"""
    print("🧠 测试chunk重要度评分系统")
    print("=" * 50)
    
    # 查找最新的chunk数据库
    client_dirs = ['tmp/client_1', 'tmp/client_2', 'tmp/client_3']
    
    for i, client_dir in enumerate(client_dirs, 1):
        db_path = f"{client_dir}/client_{i}_chunks.db"
        
        if not os.path.exists(db_path):
            print(f"❌ 客户端 {i}: 数据库不存在 - {db_path}")
            continue
            
        print(f"\n📊 客户端 {i} chunk重要度分析:")
        print(f"   数据库: {db_path}")
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # 检查是否有重要度字段
            cursor.execute("PRAGMA table_info(chunk_metadata)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'importance_score' not in columns:
                print(f"   ⚠️  数据库尚未升级，缺少importance_score字段")
                conn.close()
                continue
            
            # 获取所有轮次
            cursor.execute("SELECT DISTINCT round_num FROM chunk_metadata ORDER BY round_num")
            rounds = [row[0] for row in cursor.fetchall()]
            
            if not rounds:
                print(f"   ❌ 没有找到chunk数据")
                conn.close()
                continue
            
            print(f"   📈 发现 {len(rounds)} 个训练轮次: {rounds}")
            
            for round_num in rounds[-2:]:  # 只显示最后2轮
                print(f"\n   🔍 轮次 {round_num} chunk重要度分数:")
                
                cursor.execute('''
                    SELECT chunk_id, importance_score, pruning_method, flat_size, 
                           substr(chunk_hash, 1, 8) as short_hash
                    FROM chunk_metadata 
                    WHERE round_num = ?
                    ORDER BY importance_score DESC
                ''', (round_num,))
                
                chunk_data = cursor.fetchall()
                
                if not chunk_data:
                    print(f"      ❌ 轮次 {round_num} 没有chunk数据")
                    continue
                
                print(f"      {'Chunk':<8} {'重要度':<10} {'方法':<12} {'大小':<10} {'Hash':<10}")
                print(f"      {'-' * 8} {'-' * 10} {'-' * 12} {'-' * 10} {'-' * 10}")
                
                for chunk_id, importance_score, pruning_method, flat_size, short_hash in chunk_data:
                    importance_score = importance_score if importance_score is not None else 0.0
                    pruning_method = pruning_method or 'unknown'
                    
                    print(f"      {chunk_id:<8} {importance_score:<10.6f} {pruning_method:<12} {flat_size:<10} {short_hash}...")
                
                # 统计信息
                scores = [row[1] for row in chunk_data if row[1] is not None]
                if scores:
                    avg_score = sum(scores) / len(scores)
                    max_score = max(scores)
                    min_score = min(scores)
                    
                    print(f"      📊 统计: 平均={avg_score:.6f}, 最高={max_score:.6f}, 最低={min_score:.6f}")
                else:
                    print(f"      ⚠️  没有有效的重要度分数")
            
            conn.close()
            
        except Exception as e:
            print(f"   ❌ 分析失败: {e}")

def test_importance_methods():
    """测试不同重要度计算方法"""
    print(f"\n🧠 测试不同重要度计算方法")
    print("=" * 50)
    
    methods = ['magnitude', 'l2_norm', 'snip', 'fisher']
    
    try:
        from federatedscope.core.chunk_manager import ChunkManager
        import torch
        import torch.nn as nn
        import numpy as np
        
        # 创建简单测试模型
        class SimpleModel(nn.Module):
            def __init__(self):
                super().__init__()
                self.linear1 = nn.Linear(10, 5)
                self.linear2 = nn.Linear(5, 2)
                
            def forward(self, x):
                x = torch.relu(self.linear1(x))
                return self.linear2(x)
        
        # 初始化模型
        model = SimpleModel()
        
        # 创建临时chunk manager
        chunk_manager = ChunkManager(client_id=999)
        
        # 转换模型参数
        params = chunk_manager.model_to_params(model)
        
        # 分割模型
        chunks_info = chunk_manager.split_model(params, num_chunks=4)
        
        print(f"📊 测试模型: {len(params)} 个参数层, 分割为 {len(chunks_info)} 个chunk")
        
        # 测试每种方法
        for method in methods:
            print(f"\n🔬 方法: {method}")
            try:
                scores = chunk_manager.compute_chunk_importance(params, chunks_info, method)
                print(f"   重要度分数: {[f'{s:.4f}' for s in scores]}")
                print(f"   分数范围: [{min(scores):.4f}, {max(scores):.4f}]")
            except Exception as e:
                print(f"   ❌ 计算失败: {e}")
        
        # 清理临时文件
        if os.path.exists("tmp/client_999/client_999_chunks.db"):
            os.remove("tmp/client_999/client_999_chunks.db")
            import shutil
            if os.path.exists("tmp/client_999"):
                shutil.rmtree("tmp/client_999")
            
        print(f"\n✅ 重要度计算方法测试完成")
        
    except Exception as e:
        print(f"❌ 重要度方法测试失败: {e}")

if __name__ == "__main__":
    print("🧠 Chunk重要度评分系统测试")
    print("=" * 60)
    
    # 测试现有数据库中的重要度分数
    test_chunk_importance_scores()
    
    # 测试不同重要度计算方法
    test_importance_methods()
    
    print(f"\n🎉 测试完成!")