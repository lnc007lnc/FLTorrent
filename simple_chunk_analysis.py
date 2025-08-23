#!/usr/bin/env python3
"""
简化版Chunk数据分析 - 查看3个客户端的chunk记录和重要度分数
"""

import sqlite3
import os
from datetime import datetime

def analyze_client_simple(client_id: int):
    """简单分析单个客户端"""
    db_path = f"tmp/client_{client_id}/client_{client_id}_chunks.db"
    
    if not os.path.exists(db_path):
        print(f"❌ 客户端{client_id}数据库不存在: {db_path}")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"\n{'='*80}")
        print(f"📊 客户端 {client_id} 数据分析")
        print(f"{'='*80}")
        
        # 1. 本地chunk重要度分数
        print(f"\n🏠 本地Chunk重要度分数:")
        cursor.execute("""
            SELECT round_num, chunk_id, importance_score, flat_size
            FROM chunk_metadata 
            ORDER BY round_num, importance_score DESC
        """)
        local_chunks = cursor.fetchall()
        
        current_round = -1
        for round_num, chunk_id, importance, size in local_chunks:
            if round_num != current_round:
                current_round = round_num
                print(f"\n  🔄 轮次 {round_num}:")
            print(f"     Chunk-{chunk_id}: 重要度 {importance:.4f} | 大小 {size:,} bytes")
        
        # 2. 接收的远程chunk记录
        print(f"\n📥 接收的远程Chunk记录:")
        cursor.execute("""
            SELECT round_num, source_client_id, chunk_id, received_time
            FROM bt_chunks 
            WHERE source_client_id != ? AND holder_client_id = ?
            ORDER BY round_num, received_time
        """, (client_id, client_id))
        received_chunks = cursor.fetchall()
        
        current_round = -1
        chunk_count = 0
        for round_num, source_id, chunk_id, received_time in received_chunks:
            if round_num != current_round:
                current_round = round_num
                chunk_count = 0
                print(f"\n  🔄 轮次 {round_num} 接收顺序:")
            chunk_count += 1
            # 转换时间戳
            time_str = datetime.fromtimestamp(received_time).strftime("%H:%M:%S.%f")[:-3]
            print(f"     #{chunk_count:2}: C{source_id}-Chunk{chunk_id} | {time_str}")
        
        # 3. 统计信息
        cursor.execute("SELECT COUNT(*) FROM chunk_metadata")
        local_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM bt_chunks WHERE source_client_id != ? AND holder_client_id = ?", (client_id, client_id))
        received_count = cursor.fetchone()[0]
        
        print(f"\n📈 统计摘要:")
        print(f"   - 本地生成chunks: {local_count}")
        print(f"   - 接收远程chunks: {received_count}")
        print(f"   - 总计chunks: {local_count + received_count}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 数据库查询失败: {e}")

def cross_client_importance_comparison():
    """跨客户端重要度分数对比"""
    print(f"\n{'='*100}")
    print(f"🔄 跨客户端重要度分数对比")
    print(f"{'='*100}")
    
    all_chunks = {}  # {round: {client: [(chunk_id, importance)]}}
    
    # 收集所有客户端的数据
    for client_id in [1, 2, 3]:
        db_path = f"tmp/client_{client_id}/client_{client_id}_chunks.db"
        if not os.path.exists(db_path):
            continue
            
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT round_num, chunk_id, importance_score
                FROM chunk_metadata 
                ORDER BY round_num, chunk_id
            """)
            chunks = cursor.fetchall()
            
            for round_num, chunk_id, importance in chunks:
                if round_num not in all_chunks:
                    all_chunks[round_num] = {}
                if client_id not in all_chunks[round_num]:
                    all_chunks[round_num][client_id] = []
                all_chunks[round_num][client_id].append((chunk_id, importance))
            
            conn.close()
            
        except Exception as e:
            print(f"❌ 客户端{client_id}查询失败: {e}")
    
    # 按轮次显示对比
    for round_num in sorted(all_chunks.keys()):
        print(f"\n📋 轮次 {round_num} - 各客户端chunk重要度排名:")
        
        # 收集所有chunks并排序
        all_round_chunks = []
        for client_id, chunks in all_chunks[round_num].items():
            for chunk_id, importance in chunks:
                all_round_chunks.append((client_id, chunk_id, importance))
        
        # 按重要度排序
        all_round_chunks.sort(key=lambda x: x[2], reverse=True)
        
        print(f"{'排名':<6} {'客户端':<8} {'Chunk':<10} {'重要度':<12} {'重要度排名'}")
        print("-" * 60)
        
        for i, (client_id, chunk_id, importance) in enumerate(all_round_chunks):
            rank = i + 1
            print(f"#{rank:<5} 客户端{client_id:<4} Chunk-{chunk_id:<4} {importance:<12.4f} {'🥇' if rank == 1 else '🥈' if rank == 2 else '🥉' if rank == 3 else ''}")

def analyze_reception_patterns():
    """分析接收模式 - 验证重要度优先级"""
    print(f"\n{'='*100}")
    print(f"🎯 接收模式分析 - 验证双池系统重要度优先级")
    print(f"{'='*100}")
    
    for client_id in [1, 2, 3]:
        db_path = f"tmp/client_{client_id}/client_{client_id}_chunks.db"
        if not os.path.exists(db_path):
            continue
            
        print(f"\n📈 客户端 {client_id} 接收模式:")
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # 获取接收时间顺序
            cursor.execute("""
                SELECT round_num, source_client_id, chunk_id, received_time
                FROM bt_chunks 
                WHERE source_client_id != ? AND holder_client_id = ?
                ORDER BY round_num, received_time
            """, (client_id, client_id))
            received = cursor.fetchall()
            
            # 按轮次分组
            by_round = {}
            for round_num, source_id, chunk_id, received_time in received:
                if round_num not in by_round:
                    by_round[round_num] = []
                by_round[round_num].append((source_id, chunk_id, received_time))
            
            # 分析每轮的接收模式
            for round_num in sorted(by_round.keys()):
                chunks = by_round[round_num]
                print(f"\n  轮次 {round_num}: 接收了{len(chunks)}个chunks")
                
                if len(chunks) >= 5:
                    print(f"    前5个接收: ", end="")
                    for i in range(5):
                        source_id, chunk_id, _ = chunks[i]
                        print(f"C{source_id}-{chunk_id}", end=" ")
                    print()
                    
                    print(f"    最后5个: ", end="")
                    for i in range(max(0, len(chunks)-5), len(chunks)):
                        source_id, chunk_id, _ = chunks[i]
                        print(f"C{source_id}-{chunk_id}", end=" ")
                    print()
                
                # 计算接收时间范围
                if len(chunks) > 1:
                    first_time = chunks[0][2]
                    last_time = chunks[-1][2]
                    duration = last_time - first_time
                    print(f"    接收用时: {duration:.2f}秒")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ 客户端{client_id}分析失败: {e}")

def main():
    print("🔍 双池系统Chunk数据分析")
    print("=" * 100)
    
    # 分析每个客户端
    for client_id in [1, 2, 3]:
        analyze_client_simple(client_id)
    
    # 跨客户端对比
    cross_client_importance_comparison()
    
    # 接收模式分析  
    analyze_reception_patterns()
    
    print(f"\n{'='*100}")
    print("✅ 分析完成！双池系统chunk接收和重要度分析完毕。")

if __name__ == "__main__":
    main()