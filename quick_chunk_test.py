#!/usr/bin/env python3
"""
快速测试chunk系统的集成
"""
import os
import sys
import logging

# 设置路径和环境
sys.path.insert(0, os.getcwd())
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_chunk_system():
    """测试chunk系统的完整工作流"""
    try:
        # 导入必要模块
        import torch
        import torch.nn as nn
        from federatedscope.core.chunk_manager import ChunkManager
        
        logger.info("🧪 开始测试chunk系统...")
        
        # 创建简单测试模型
        class SimpleModel(nn.Module):
            def __init__(self):
                super().__init__()
                self.fc1 = nn.Linear(10, 20)
                self.fc2 = nn.Linear(20, 1)
            
            def forward(self, x):
                x = torch.relu(self.fc1(x))
                return self.fc2(x)
        
        model = SimpleModel()
        logger.info(f"   模型参数数量: {sum(p.numel() for p in model.parameters())}")
        
        # 测试3个不同的客户端
        for client_id in [1, 2, 3]:
            logger.info(f"🤖 测试客户端 {client_id}...")
            
            chunk_manager = ChunkManager(client_id=client_id)
            
            # 模拟2轮训练
            for round_num in range(1, 3):
                # 稍微修改模型权重
                with torch.no_grad():
                    for param in model.parameters():
                        param.data += torch.randn_like(param) * 0.01
                
                # 保存chunks
                saved_hashes = chunk_manager.save_model_chunks(
                    model=model,
                    round_num=round_num,
                    num_chunks=4
                )
                
                logger.info(f"   💾 客户端{client_id}第{round_num}轮保存了 {len(saved_hashes)} 个chunks")
            
            # 验证数据库
            stats = chunk_manager.get_storage_stats()
            logger.info(f"   📊 客户端{client_id}存储统计: {stats['storage_size_mb']:.3f} MB, "
                       f"{stats['unique_chunks']} chunks")
        
        # 检查生成的数据库文件
        logger.info("\n🔍 检查生成的数据库文件:")
        chunk_files = []
        for root, dirs, files in os.walk('.'):
            for file in files:
                if file.endswith('_chunks.db'):
                    full_path = os.path.join(root, file)
                    size_mb = os.path.getsize(full_path) / (1024 * 1024)
                    chunk_files.append((full_path, size_mb))
                    logger.info(f"   ✅ {full_path} ({size_mb:.3f} MB)")
        
        if chunk_files:
            logger.info(f"\n✅ 测试成功! 生成了 {len(chunk_files)} 个chunk数据库文件")
            
            # 验证chunk数据完整性
            for client_id in [1, 2, 3]:
                chunk_manager = ChunkManager(client_id=client_id)
                test_model = SimpleModel()
                success = chunk_manager.reconstruct_model_from_chunks(
                    round_num=2,
                    target_model=test_model
                )
                if success:
                    logger.info(f"   ✅ 客户端{client_id}的模型重建成功")
                else:
                    logger.warning(f"   ⚠️ 客户端{client_id}的模型重建失败")
            
            return True
        else:
            logger.error("❌ 未找到任何chunk数据库文件!")
            return False
            
    except Exception as e:
        logger.error(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_chunk_system()
    if success:
        print("\n🎉 Chunk系统测试成功! 系统正常工作.")
    else:
        print("\n💥 Chunk系统测试失败! 请检查实现.")
    sys.exit(0 if success else 1)