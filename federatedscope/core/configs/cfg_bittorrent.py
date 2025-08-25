"""
BitTorrent Configuration Module
Define configuration parameters for BitTorrent chunk exchange system
"""

import logging
from federatedscope.core.configs.config import CN
from federatedscope.register import register_config

logger = logging.getLogger(__name__)


def extend_bittorrent_cfg(cfg):
    """
    Extend configuration system, add BitTorrent-related configuration
    
    Args:
        cfg: Global configuration object
    """
    # 🔧 BitTorrent configuration section
    cfg.bittorrent = CN()
    
    # Basic switch configuration
    cfg.bittorrent.enable = False  # Whether to enable BitTorrent chunk exchange
    
    # Timeout and retry configuration
    cfg.bittorrent.timeout = 60.0  # BitTorrent exchange timeout (seconds)
    cfg.bittorrent.max_retries = 3  # Maximum retry count for chunk requests
    cfg.bittorrent.request_timeout = 5.0  # Timeout for single chunk request (seconds)
    
    # Protocol parameter configuration
    cfg.bittorrent.max_upload_slots = 4  # Maximum upload slots
    cfg.bittorrent.optimistic_unchoke_interval = 30.0  # Optimistic unchoke interval (seconds)
    cfg.bittorrent.regular_unchoke_interval = 10.0  # Regular unchoke interval (seconds)
    cfg.bittorrent.end_game_threshold = 10  # Remaining chunk threshold for entering End Game mode
    
    # Completion strategy configuration
    cfg.bittorrent.min_completion_ratio = 0.8  # Minimum completion ratio allowed to continue
    cfg.bittorrent.require_full_completion = True  # Whether to require all clients to complete fully
    
    # Algorithm selection configuration
    cfg.bittorrent.chunk_selection = 'rarest_first'  # Chunk selection algorithm: 'rarest_first', 'random', 'sequential'
    
    # Optimization configuration
    cfg.bittorrent.enable_have_suppression = True  # Whether to enable have message suppression
    cfg.bittorrent.batch_have_interval = 1.0  # Batch have message send interval (seconds)
    
    # Storage configuration
    cfg.bittorrent.keep_remote_chunks = 5  # Number of rounds to keep remote chunks
    cfg.bittorrent.cleanup_policy = 'separate'  # Cleanup policy: 'separate', 'unified'
    
    # Logging and debug configuration
    cfg.bittorrent.verbose = False  # Whether to enable verbose logging
    cfg.bittorrent.save_statistics = True  # Whether to save exchange statistics
    cfg.bittorrent.log_dir = 'bittorrent_logs'  # BitTorrent log save directory



def assert_bittorrent_cfg(cfg):
    """Validate the validity of BitTorrent configuration parameters"""
    if not cfg.bittorrent.enable:
        return  # If BitTorrent is disabled, skip validation
    
    # Validate timeout parameters
    assert cfg.bittorrent.timeout > 0, \
        f"cfg.bittorrent.timeout must be positive, but got {cfg.bittorrent.timeout}"
    
    assert cfg.bittorrent.request_timeout > 0, \
        f"cfg.bittorrent.request_timeout must be positive, but got {cfg.bittorrent.request_timeout}"
    
    # Validate retry parameters
    assert cfg.bittorrent.max_retries >= 0, \
        f"cfg.bittorrent.max_retries must be non-negative, but got {cfg.bittorrent.max_retries}"
    
    # Validate protocol parameters
    assert cfg.bittorrent.max_upload_slots > 0, \
        f"cfg.bittorrent.max_upload_slots must be positive, but got {cfg.bittorrent.max_upload_slots}"
    
    assert 0.0 < cfg.bittorrent.min_completion_ratio <= 1.0, \
        f"cfg.bittorrent.min_completion_ratio must be in (0.0, 1.0], but got {cfg.bittorrent.min_completion_ratio}"
    
    # Validate algorithm selection
    valid_algorithms = ['rarest_first', 'random', 'sequential']
    assert cfg.bittorrent.chunk_selection in valid_algorithms, \
        f"cfg.bittorrent.chunk_selection must be one of {valid_algorithms}, but got {cfg.bittorrent.chunk_selection}"
    
    # Validate cleanup policy
    valid_policies = ['separate', 'unified']
    assert cfg.bittorrent.cleanup_policy in valid_policies, \
        f"cfg.bittorrent.cleanup_policy must be one of {valid_policies}, but got {cfg.bittorrent.cleanup_policy}"
    
    # Log configuration information
    if cfg.bittorrent.enable:
        logger.info(f"BitTorrent chunk exchange enabled with {cfg.bittorrent.chunk_selection} algorithm")
        if cfg.bittorrent.verbose:
            logger.info(f"BitTorrent configuration: timeout={cfg.bittorrent.timeout}s, "
                       f"upload_slots={cfg.bittorrent.max_upload_slots}, "
                       f"completion_ratio={cfg.bittorrent.min_completion_ratio}")


register_config("bittorrent", extend_bittorrent_cfg)