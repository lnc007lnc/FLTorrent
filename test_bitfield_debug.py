#!/usr/bin/env python3
"""
单独测试bitfield生成函数，排查为什么BitTorrent发送0个chunks的问题
"""
import sqlite3
import os
import sys
import logging

# 配置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_chunk_database(db_path):
    """分析chunk数据库的内容"""
    logger.info(f"=== 分析数据库: {db_path} ===")
    
    if not os.path.exists(db_path):
        logger.error(f"数据库文件不存在: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 检查表结构
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='chunk_metadata'")
        table_schema = cursor.fetchone()
        if table_schema:
            logger.info(f"表结构: {table_schema[0]}")
        else:
            logger.error("chunk_metadata表不存在！")
            return
        
        # 查看所有数据
        cursor.execute("SELECT * FROM chunk_metadata")
        rows = cursor.fetchall()
        logger.info(f"总共有 {len(rows)} 条记录")
        
        # 按轮次分组统计
        cursor.execute("SELECT round_num, COUNT(*) FROM chunk_metadata GROUP BY round_num ORDER BY round_num")
        rounds = cursor.fetchall()
        logger.info("按轮次统计:")
        for round_num, count in rounds:
            logger.info(f"  轮次 {round_num}: {count} chunks")
        
        # 查看每条记录的详细信息 (注意：表中没有chunk_size字段，使用flat_size)
        logger.info("详细记录:")
        cursor.execute("SELECT round_num, chunk_id, flat_size, chunk_hash FROM chunk_metadata ORDER BY round_num, chunk_id")
        rows = cursor.fetchall()
        for row in rows:
            round_num, chunk_id, flat_size, chunk_hash = row
            logger.info(f"  Round={round_num}, Chunk={chunk_id}, Size={flat_size}, Hash={chunk_hash[:8]}...")
            
    except Exception as e:
        logger.error(f"查询数据库失败: {e}")
    finally:
        conn.close()

def test_get_global_bitfield():
    """测试get_global_bitfield函数"""
    logger.info("=== 测试get_global_bitfield函数 ===")
    
    # 添加项目路径
    sys.path.insert(0, '/mnt/g/FLtorrent_combine/FederatedScope-master')
    
    try:
        from federatedscope.core.chunk_manager import ChunkManager
        
        # 为客户端1创建ChunkManager实例
        chunk_manager = ChunkManager(client_id=1)
        
        # 测试轮次0的bitfield
        logger.info("测试轮次0的bitfield:")
        bitfield = chunk_manager.get_global_bitfield(round_num=0)
        logger.info(f"轮次0的bitfield: {bitfield}")
        logger.info(f"bitfield长度: {len(bitfield)}")
        logger.info(f"设置的位数: {sum(bitfield)}")
        
        # 测试轮次3的bitfield (数据库中存在的轮次)
        logger.info("测试轮次3的bitfield:")
        bitfield_round3 = chunk_manager.get_global_bitfield(round_num=3)
        logger.info(f"轮次3的bitfield: {bitfield_round3}")
        logger.info(f"bitfield长度: {len(bitfield_round3)}")
        logger.info(f"设置的位数: {sum(bitfield_round3)}")
        
        # 测试轮次4的bitfield (数据库中存在的轮次)
        logger.info("测试轮次4的bitfield:")
        bitfield_round4 = chunk_manager.get_global_bitfield(round_num=4)
        logger.info(f"轮次4的bitfield: {bitfield_round4}")
        logger.info(f"bitfield长度: {len(bitfield_round4)}")
        logger.info(f"设置的位数: {sum(bitfield_round4)}")
        
        # 测试轮次1的bitfield
        logger.info("测试轮次1的bitfield:")
        bitfield_round1 = chunk_manager.get_global_bitfield(round_num=1)
        logger.info(f"轮次1的bitfield: {bitfield_round1}")
        logger.info(f"bitfield长度: {len(bitfield_round1)}")
        logger.info(f"设置的位数: {sum(bitfield_round1)}")
        
        # 获取storage stats
        stats = chunk_manager.get_storage_stats()
        logger.info(f"存储统计: {stats}")
        
    except Exception as e:
        logger.error(f"测试get_global_bitfield失败: {e}")
        import traceback
        logger.error(traceback.format_exc())

def main():
    """主函数"""
    logger.info("开始调试bitfield生成问题")
    
    # 分析所有客户端的数据库
    for client_id in [1, 2, 3]:
        db_path = f"/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_{client_id}/client_{client_id}_chunks.db"
        analyze_chunk_database(db_path)
        print("-" * 50)
    
    # 测试get_global_bitfield函数
    test_get_global_bitfield()

if __name__ == "__main__":
    main()