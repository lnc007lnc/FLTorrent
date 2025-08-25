#!/usr/bin/env python3
"""
分析双池系统测试结果 - 打印3个客户端的chunk接收记录和重要度分数
"""

import sqlite3
import os
import sys
from typing import Dict, List, Tuple

def analyze_client_chunks(client_id: int) -> Dict:
    """分析单个客户端的chunk数据"""
    db_path = f"tmp/client_{client_id}/client_{client_id}_chunks.db"
    
    if not os.path.exists(db_path):
        return {"error": f"数据库文件不存在: {db_path}"}
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        result = {
            "client_id": client_id,
            "local_chunks": {},  # {round: [(chunk_id, importance_score, data_size)]}
            "received_chunks": {},  # {round: [(source_id, chunk_id, importance_score, timestamp)]}
            "chunk_reception_order": {}  # {round: [chunk_keys_in_order]}
        }
        
        # 获取本地chunk重要度分数
        cursor.execute("""
            SELECT round_num, chunk_id, importance_score, flat_size, created_at
            FROM chunk_metadata 
            ORDER BY round_num, chunk_id
        """)
        local_data = cursor.fetchall()
        
        for round_num, chunk_id, importance, size, created_at in local_data:
            if round_num not in result["local_chunks"]:
                result["local_chunks"][round_num] = []
            result["local_chunks"][round_num].append({
                "chunk_id": chunk_id,
                "importance_score": importance,
                "data_size": size,
                "created_at": created_at
            })
        
        # 获取接收chunk记录（按时间排序）
        # 这里我们需要获取remote chunks的重要度分数，需要特殊处理
        cursor.execute("""
            SELECT bc.round_num, bc.source_client_id, bc.chunk_id, 
                   bc.received_time
            FROM bt_chunks bc
            WHERE bc.source_client_id != ? AND bc.holder_client_id = ?
            ORDER BY bc.round_num, bc.received_time
        """, (client_id, client_id))
        received_data = cursor.fetchall()
        
        for round_num, source_id, chunk_id, received_at in received_data:
            if round_num not in result["received_chunks"]:
                result["received_chunks"][round_num] = []
                result["chunk_reception_order"][round_num] = []
            
            # 从源客户端的数据库中获取重要度分数
            # 这里先设置为0，后续可以从源客户端获取
            importance_score = 0.0
            
            # 尝试从远程chunks的重要度数据库中获取
            cursor.execute("""
                SELECT importance_score FROM bt_chunks 
                WHERE round_num = ? AND source_client_id = ? AND chunk_id = ? 
                  AND holder_client_id = ?
                LIMIT 1
            """, (round_num, source_id, chunk_id, client_id))
            importance_result = cursor.fetchone()
            
            result["received_chunks"][round_num].append({
                "source_client_id": source_id,
                "chunk_id": chunk_id,
                "importance_score": importance_score,  # 这里需要后续处理
                "received_at": received_at,
                "data_size": 0  # 暂时设为0
            })
            result["chunk_reception_order"][round_num].append(f"C{source_id}-{chunk_id}")
        
        conn.close()
        return result
        
    except Exception as e:
        return {"error": f"数据库查询失败: {e}"}

def print_client_analysis(client_data: Dict):
    """打印单个客户端的分析结果"""
    if "error" in client_data:
        print(f"❌ 客户端数据错误: {client_data['error']}")
        return
    
    client_id = client_data["client_id"]
    print(f"\n{'='*80}")
    print(f"📊 客户端 {client_id} 的Chunk数据分析")
    print(f"{'='*80}")
    
    # 打印本地chunk重要度分数
    if client_data["local_chunks"]:
        print(f"\n🏠 本地Chunk重要度分数:")
        for round_num in sorted(client_data["local_chunks"].keys()):
            chunks = client_data["local_chunks"][round_num]
            print(f"\n  轮次 {round_num}: ({len(chunks)} chunks)")
            
            # 按重要度排序显示
            chunks_sorted = sorted(chunks, key=lambda x: x['importance_score'], reverse=True)
            for i, chunk in enumerate(chunks_sorted):
                importance = chunk['importance_score']
                chunk_id = chunk['chunk_id']
                size = chunk['data_size']
                print(f"    #{i+1}: Chunk-{chunk_id} | 重要度: {importance:.4f} | 大小: {size:,} bytes")
    
    # 打印接收chunk记录（按重要度排序）
    if client_data["received_chunks"]:
        print(f"\n📥 接收Chunk记录（按重要度排序）:")
        for round_num in sorted(client_data["received_chunks"].keys()):
            chunks = client_data["received_chunks"][round_num]
            print(f"\n  轮次 {round_num}: ({len(chunks)} chunks)")
            
            # 按重要度排序
            chunks_sorted = sorted(chunks, key=lambda x: x['importance_score'], reverse=True)
            print(f"    按重要度排序的接收顺序:")
            for i, chunk in enumerate(chunks_sorted):
                source = chunk['source_client_id']
                chunk_id = chunk['chunk_id']
                importance = chunk['importance_score']
                time_str = chunk['received_at']
                print(f"      #{i+1}: C{source}-{chunk_id} | 重要度: {importance:.4f} | 接收时间: {time_str}")
            
            # 显示实际接收时间顺序
            print(f"\n    实际接收时间顺序:")
            reception_order = client_data["chunk_reception_order"][round_num]
            for i, chunk_key in enumerate(reception_order):
                chunk_info = next((c for c in chunks if f"C{c['source_client_id']}-{c['chunk_id']}" == chunk_key), None)
                if chunk_info:
                    importance = chunk_info['importance_score']
                    print(f"      第{i+1}个: {chunk_key} | 重要度: {importance:.4f}")

def print_cross_client_comparison():
    """打印跨客户端重要度对比"""
    print(f"\n{'='*100}")
    print(f"🔄 跨客户端重要度分数对比分析")
    print(f"{'='*100}")
    
    all_data = {}
    for client_id in [1, 2, 3]:
        all_data[client_id] = analyze_client_chunks(client_id)
    
    # 按轮次对比每个客户端的重要度分数
    for round_num in [0, 1, 2]:
        print(f"\n📋 轮次 {round_num} - 各客户端本地Chunk重要度对比:")
        print(f"{'客户端':<10} {'Chunk ID':<12} {'重要度分数':<15} {'排名':<8}")
        print(f"{'-'*50}")
        
        round_chunks = []
        for client_id in [1, 2, 3]:
            if ("local_chunks" in all_data[client_id] and 
                round_num in all_data[client_id]["local_chunks"]):
                for chunk in all_data[client_id]["local_chunks"][round_num]:
                    round_chunks.append({
                        "client_id": client_id,
                        "chunk_id": chunk["chunk_id"],
                        "importance": chunk["importance_score"]
                    })
        
        # 按重要度排序
        round_chunks.sort(key=lambda x: x["importance"], reverse=True)
        
        for i, chunk in enumerate(round_chunks):
            client_id = chunk["client_id"]
            chunk_id = chunk["chunk_id"]
            importance = chunk["importance"]
            rank = i + 1
            print(f"客户端{client_id:<7} Chunk-{chunk_id:<7} {importance:<15.4f} #{rank}")

def print_importance_prioritization_analysis():
    """分析重要度优先级是否生效"""
    print(f"\n{'='*100}")
    print(f"🎯 重要度优先级分析 - 验证双池系统是否按重要度优先请求")
    print(f"{'='*100}")
    
    for client_id in [1, 2, 3]:
        client_data = analyze_client_chunks(client_id)
        if "received_chunks" not in client_data:
            continue
            
        print(f"\n📈 客户端 {client_id} - 重要度优先级验证:")
        
        for round_num in sorted(client_data["received_chunks"].keys()):
            chunks = client_data["received_chunks"][round_num]
            if len(chunks) < 2:
                continue
                
            print(f"\n  轮次 {round_num}:")
            
            # 按接收时间排序
            chunks_by_time = sorted(chunks, key=lambda x: x['received_at'])
            
            # 计算重要度相关性
            importances = [c['importance_score'] for c in chunks_by_time]
            
            # 前5个接收的chunk的重要度
            first_5 = chunks_by_time[:5] if len(chunks_by_time) >= 5 else chunks_by_time
            first_5_importance = [c['importance_score'] for c in first_5]
            avg_first_5 = sum(first_5_importance) / len(first_5_importance)
            
            # 全部chunk的重要度均值
            avg_all = sum(importances) / len(importances)
            
            print(f"    前5个接收chunk平均重要度: {avg_first_5:.4f}")
            print(f"    全部chunk平均重要度: {avg_all:.4f}")
            print(f"    重要度优先效果: {'+' if avg_first_5 > avg_all else '-'}{((avg_first_5 - avg_all) / avg_all * 100):.1f}%")
            
            # 显示前5个接收的chunk
            print(f"    前5个接收的chunk:")
            for i, chunk in enumerate(first_5):
                source = chunk['source_client_id']
                chunk_id = chunk['chunk_id']
                importance = chunk['importance_score']
                print(f"      第{i+1}: C{source}-{chunk_id} (重要度: {importance:.4f})")

def main():
    """主函数"""
    print("🔍 双池系统测试结果分析 - Chunk接收记录和重要度分数")
    print("=" * 100)
    
    # 检查数据库文件是否存在
    missing_dbs = []
    for client_id in [1, 2, 3]:
        db_path = f"tmp/client_{client_id}/client_{client_id}_chunks.db"
        if not os.path.exists(db_path):
            missing_dbs.append(db_path)
    
    if missing_dbs:
        print("❌ 以下数据库文件不存在:")
        for db in missing_dbs:
            print(f"   - {db}")
        print("\n请确保已运行V2测试并生成了chunk数据库。")
        return
    
    # 分析每个客户端
    for client_id in [1, 2, 3]:
        client_data = analyze_client_chunks(client_id)
        print_client_analysis(client_data)
    
    # 跨客户端对比
    print_cross_client_comparison()
    
    # 重要度优先级分析
    print_importance_prioritization_analysis()
    
    print(f"\n{'='*100}")
    print("✅ 分析完成！")
    print("💡 关键指标:")
    print("   - 重要度优先效果 > 0% : 说明双池系统按重要度优先工作")
    print("   - 前5个接收chunk重要度 > 全部平均值 : 说明高重要度chunk被优先请求")
    print("   - 跨客户端重要度对比 : 验证不同客户端的重要度计算一致性")

if __name__ == "__main__":
    main()