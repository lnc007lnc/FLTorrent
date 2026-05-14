"""
Chunk Configuration Module
Define configuration parameters related to model chunking
"""

import logging
from federatedscope.core.configs.config import CN
from federatedscope.register import register_config

logger = logging.getLogger(__name__)


def extend_chunk_cfg(cfg):
    """
    Extend configuration system, add Chunk-related configuration
    
    Args:
        cfg: Global configuration object
    """
    # 🔧 Chunk configuration section
    cfg.chunk = CN()
    
    # Basic chunk configuration
    cfg.chunk.num_chunks = 10  # Number of chunks each client model is split into
    cfg.chunk.keep_rounds = 2  # Number of rounds to keep chunks
    cfg.chunk.importance_method = 'magnitude'  # Chunk importance calculation method
    cfg.chunk.tmp_dir = '/tmp/fl_chunks'  # Temporary directory for chunk storage
    
    # Compatibility configuration: direct configuration used by clients
    cfg.chunk_num = 10  # Chunk count configuration read directly by clients
    cfg.chunk_keep_rounds = 2  # Keep rounds configuration read directly by clients
    cfg.chunk_importance_method = 'magnitude'  # Importance method configuration read directly by clients
    cfg.chunk_tmp_dir = '/tmp/fl_chunks'  # Temporary directory for chunk storage
    cfg.chunk_nfs_compatible = False  # Whether to use NFS-compatible storage


def assert_chunk_cfg(cfg):
    """Validate the validity of Chunk configuration parameters"""
    
    # Validate chunk count
    if hasattr(cfg, 'chunk') and hasattr(cfg.chunk, 'num_chunks'):
        assert cfg.chunk.num_chunks > 0, \
            f"cfg.chunk.num_chunks must be positive, but got {cfg.chunk.num_chunks}"
    
    if hasattr(cfg, 'chunk_num'):
        assert cfg.chunk_num > 0, \
            f"cfg.chunk_num must be positive, but got {cfg.chunk_num}"
    
    # Validate keep rounds
    if hasattr(cfg, 'chunk') and hasattr(cfg.chunk, 'keep_rounds'):
        assert cfg.chunk.keep_rounds >= 0, \
            f"cfg.chunk.keep_rounds must be non-negative, but got {cfg.chunk.keep_rounds}"
    
    # Validate importance method
    valid_methods = ['magnitude', 'l2_norm', 'gradient_norm', 'snip', 'fisher']
    if hasattr(cfg, 'chunk') and hasattr(cfg.chunk, 'importance_method'):
        assert cfg.chunk.importance_method in valid_methods, \
            f"cfg.chunk.importance_method must be one of {valid_methods}, but got {cfg.chunk.importance_method}"
    
    if hasattr(cfg, 'chunk_importance_method'):
        assert cfg.chunk_importance_method in valid_methods, \
            f"cfg.chunk_importance_method must be one of {valid_methods}, but got {cfg.chunk_importance_method}"
    
    # Log configuration information
    if hasattr(cfg, 'chunk') and hasattr(cfg.chunk, 'num_chunks'):
        importance_method = getattr(cfg.chunk, 'importance_method', 'magnitude')
        logger.info(f"Chunk configuration: num_chunks={cfg.chunk.num_chunks}, keep_rounds={getattr(cfg.chunk, 'keep_rounds', 2)}, importance_method={importance_method}")


register_config("chunk", extend_chunk_cfg)