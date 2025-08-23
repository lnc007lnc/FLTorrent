"""
Chunk配置模块
定义模型分块相关的配置参数
"""

import logging
from federatedscope.core.configs.config import CN
from federatedscope.register import register_config

logger = logging.getLogger(__name__)


def extend_chunk_cfg(cfg):
    """
    扩展配置系统，添加Chunk相关配置
    
    Args:
        cfg: 全局配置对象
    """
    # 🔧 Chunk配置section
    cfg.chunk = CN()
    
    # 基础chunk配置
    cfg.chunk.num_chunks = 10  # 每个客户端模型分割的chunk数量
    cfg.chunk.keep_rounds = 2  # 保留chunk的轮数
    cfg.chunk.importance_method = 'magnitude'  # chunk重要度计算方法
    
    # 兼容性配置：客户端使用的直接配置
    cfg.chunk_num = 10  # 客户端直接读取的chunk数量配置
    cfg.chunk_keep_rounds = 2  # 客户端直接读取的保留轮数配置
    cfg.chunk_importance_method = 'magnitude'  # 客户端直接读取的重要度方法配置


def assert_chunk_cfg(cfg):
    """验证Chunk配置参数的有效性"""
    
    # 验证chunk数量
    if hasattr(cfg, 'chunk') and hasattr(cfg.chunk, 'num_chunks'):
        assert cfg.chunk.num_chunks > 0, \
            f"cfg.chunk.num_chunks must be positive, but got {cfg.chunk.num_chunks}"
    
    if hasattr(cfg, 'chunk_num'):
        assert cfg.chunk_num > 0, \
            f"cfg.chunk_num must be positive, but got {cfg.chunk_num}"
    
    # 验证保留轮数
    if hasattr(cfg, 'chunk') and hasattr(cfg.chunk, 'keep_rounds'):
        assert cfg.chunk.keep_rounds >= 0, \
            f"cfg.chunk.keep_rounds must be non-negative, but got {cfg.chunk.keep_rounds}"
    
    # 验证重要度方法
    valid_methods = ['magnitude', 'l2_norm', 'gradient_norm', 'snip', 'fisher']
    if hasattr(cfg, 'chunk') and hasattr(cfg.chunk, 'importance_method'):
        assert cfg.chunk.importance_method in valid_methods, \
            f"cfg.chunk.importance_method must be one of {valid_methods}, but got {cfg.chunk.importance_method}"
    
    if hasattr(cfg, 'chunk_importance_method'):
        assert cfg.chunk_importance_method in valid_methods, \
            f"cfg.chunk_importance_method must be one of {valid_methods}, but got {cfg.chunk_importance_method}"
    
    # 记录配置信息
    if hasattr(cfg, 'chunk') and hasattr(cfg.chunk, 'num_chunks'):
        importance_method = getattr(cfg.chunk, 'importance_method', 'magnitude')
        logger.info(f"Chunk configuration: num_chunks={cfg.chunk.num_chunks}, keep_rounds={getattr(cfg.chunk, 'keep_rounds', 2)}, importance_method={importance_method}")


register_config("chunk", extend_chunk_cfg)