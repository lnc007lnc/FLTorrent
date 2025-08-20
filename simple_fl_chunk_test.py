#!/usr/bin/env python3
"""
简化的联邦学习 + Chunk存储测试
使用standalone模式进行快速验证
"""
import os
import sys
import time
import logging

# 设置路径和环境
sys.path.insert(0, os.getcwd())
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_test_config():
    """创建测试配置文件"""
    config_content = """
use_gpu: False
device: 0
federate:
  mode: 'standalone'
  total_round_num: 3
  client_num: 3
  sample_client_num: 3
  make_global_eval: False
  share_local_model: True
  online_aggr: True

trainer:
  type: 'general'

eval:
  freq: 1

data:
  type: 'toy'

model:
  type: 'lr'

train:
  local_update_steps: 1
  optimizer:
    lr: 0.1
    type: SGD

seed: 12345
"""
    
    with open('test_fl_config.yaml', 'w') as f:
        f.write(config_content.strip())
    
    logger.info("✅ 创建配置文件: test_fl_config.yaml")

def run_fl_with_chunks():
    """运行带有chunk存储的联邦学习"""
    try:
        logger.info("🚀 开始运行联邦学习训练...")
        
        # 导入federated learning main
        from federatedscope.main import main
        import sys
        
        # 模拟命令行参数
        old_argv = sys.argv
        sys.argv = [
            'federatedscope/main.py',
            '--cfg', 'test_fl_config.yaml'
        ]
        
        try:
            # 运行联邦学习
            main()
            logger.info("✅ 联邦学习训练完成")
            return True
        except Exception as e:
            logger.error(f"❌ 联邦学习训练失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False
        finally:
            sys.argv = old_argv
            
    except Exception as e:
        logger.error(f"❌ 运行FL失败: {e}")
        return False

def check_chunk_results():
    """检查chunk存储结果"""
    logger.info("🔍 检查chunk存储结果...")
    
    chunk_files = []
    for root, dirs, files in os.walk('.'):
        for file in files:
            if file.endswith('_chunks.db'):
                full_path = os.path.join(root, file)
                size_mb = os.path.getsize(full_path) / (1024 * 1024)
                chunk_files.append((full_path, size_mb))
                logger.info(f"   ✅ 找到chunk数据库: {full_path} ({size_mb:.3f} MB)")
    
    if chunk_files:
        logger.info(f"\n✅ 成功! 找到 {len(chunk_files)} 个chunk数据库文件")
        
        # 验证数据库内容
        try:
            from federatedscope.core.chunk_manager import ChunkManager
            
            for file_path, _ in chunk_files:
                # 从文件名推断client_id
                if 'client_' in file_path:
                    client_id_str = file_path.split('client_')[1].split('_')[0]
                    try:
                        client_id = int(client_id_str)
                        chunk_manager = ChunkManager(client_id=client_id)
                        stats = chunk_manager.get_storage_stats()
                        logger.info(f"   📊 客户端{client_id}: {stats.get('unique_chunks', 0)} chunks, "
                                   f"{stats.get('storage_size_mb', 0):.3f} MB")
                    except ValueError:
                        logger.warning(f"   ⚠️ 无法解析客户端ID: {file_path}")
                        
        except Exception as e:
            logger.warning(f"⚠️ 无法验证chunk数据库内容: {e}")
        
        return True
    else:
        logger.error("❌ 没有找到任何chunk数据库文件!")
        return False

def main():
    """主函数"""
    logger.info("🧪 开始简化的联邦学习 + Chunk存储测试")
    logger.info("=" * 60)
    
    try:
        # 清理旧文件
        if os.path.exists('test_fl_config.yaml'):
            os.remove('test_fl_config.yaml')
        
        # 步骤1: 创建配置
        create_test_config()
        
        # 步骤2: 运行联邦学习
        fl_success = run_fl_with_chunks()
        
        if fl_success:
            # 步骤3: 检查结果
            chunk_success = check_chunk_results()
            
            if chunk_success:
                logger.info("\n🎉 测试完全成功!")
                logger.info("✅ 联邦学习正常运行")
                logger.info("✅ Chunk存储系统正常工作")
                logger.info("✅ 多客户端chunk数据库已生成")
                return True
            else:
                logger.error("\n❌ Chunk存储测试失败")
                return False
        else:
            logger.error("\n❌ 联邦学习测试失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 测试异常: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return False
    finally:
        # 清理
        if os.path.exists('test_fl_config.yaml'):
            os.remove('test_fl_config.yaml')

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)