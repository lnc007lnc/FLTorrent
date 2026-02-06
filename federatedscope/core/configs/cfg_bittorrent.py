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
    
    # 🆕 Concurrent request management configuration
    # 🔧 FIX: Reduced from 20/40 to prevent excessive concurrent requests
    cfg.bittorrent.max_active_requests = 10  # Maximum concurrent active requests per client
    cfg.bittorrent.max_pending_queue = 20    # Maximum pending request queue size

    # Completion strategy configuration
    # 🔧 FIX: Increased from 0.8 to 0.9 to reduce premature endgame mode
    cfg.bittorrent.min_completion_ratio = 0.9  # Minimum completion ratio before endgame mode
    cfg.bittorrent.endgame_max_parallel_peers = 2  # Max parallel requests per chunk in endgame
    cfg.bittorrent.require_full_completion = True  # Whether to require all clients to complete fully
    
    # Algorithm selection configuration
    cfg.bittorrent.chunk_selection = 'rarest_first'  # Chunk selection algorithm: 'rarest_first', 'random', 'sequential'
    
    # Multi-factor chunk sorting parameters
    cfg.bittorrent.rarity_weight = 0.01      # tau: rarity weight
    cfg.bittorrent.rarity_adjustment = 1e-6  # eps: rarity adjustment parameter  
    cfg.bittorrent.random_noise = 1e-4       # gamma: random noise strength
    
    # Write queue configuration
    cfg.bittorrent.write_queue_size = 1000   # ChunkWriteQueue max capacity
    
    # Optimization configuration
    cfg.bittorrent.enable_have_suppression = True  # Whether to enable have message suppression
    cfg.bittorrent.batch_have_interval = 1.0  # Batch have message send interval (seconds)
    
    # Storage configuration
    cfg.bittorrent.keep_remote_chunks = 5  # Number of rounds to keep remote chunks
    cfg.bittorrent.cleanup_policy = 'separate'  # Cleanup policy: 'separate', 'unified'
    
    # Parameter completion strategy configuration
    cfg.bittorrent.parameter_completion = 'global_model'  # Strategy for missing parameter completion
                                                          # Options: 'global_model', 'zeros', 'local_model'

    # Compensation configuration
    cfg.bittorrent.enable_compensation = True  # Enable post-aggregation compensation for partial chunk collection
                                               # Set to False for complete collection scenarios to reduce CPU overhead

    # Logging and debug configuration
    cfg.bittorrent.verbose = False  # Whether to enable verbose logging
    cfg.bittorrent.save_statistics = True  # Whether to save exchange statistics
    cfg.bittorrent.log_dir = 'bittorrent_logs'  # BitTorrent log save directory

    # Gossip-style DFL: neighbor-only collection
    cfg.bittorrent.neighbor_only_collection = False  # Only collect chunks from topology neighbors

    # ==================== Experiment 1: Bandwidth Heterogeneity ====================
    # Application-level bandwidth simulation (no root/tc required)
    cfg.bittorrent.bandwidth_heterogeneity = False  # Enable bandwidth simulation
    cfg.bittorrent.slow_client_ratio = 0.5  # Fraction of clients that are slow (0.5 = 50%)
    cfg.bittorrent.slow_client_ids = []  # Explicit list of slow client IDs, or auto-assign if empty
    cfg.bittorrent.fast_bandwidth_mbps = 100.0  # Fast clients: 100 Mbps
    cfg.bittorrent.slow_bandwidth_mbps = 10.0   # Slow clients: 10 Mbps (10x slower)

    # ==================== Experiment 2: Churn Injection ====================
    # Simulate client failures for robustness testing
    cfg.bittorrent.churn_injection = False  # Enable churn simulation
    cfg.bittorrent.churn_rate = 0.0  # Probability of failure per round (0.1 = 10%)
    cfg.bittorrent.churn_mode = 'silent'  # 'silent': sleep then return (blocking), 'delayed': slow response, 'skip': immediate skip (non-blocking, recommended)

    # ==================== Experiment 3: Protocol-only Scaling ====================
    # Synthetic payload mode for scalability testing (no ML computation)
    cfg.bittorrent.protocol_only = False  # Enable protocol-only mode
    cfg.bittorrent.synthetic_chunk_size = 1048576  # 1MB per chunk
    cfg.bittorrent.synthetic_num_chunks = 50  # Number of synthetic chunks per client


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
    
    # 🆕 Validate concurrent request parameters
    assert cfg.bittorrent.max_active_requests > 0, \
        f"cfg.bittorrent.max_active_requests must be positive, but got {cfg.bittorrent.max_active_requests}"
    
    assert cfg.bittorrent.max_pending_queue > 0, \
        f"cfg.bittorrent.max_pending_queue must be positive, but got {cfg.bittorrent.max_pending_queue}"
    
    assert cfg.bittorrent.max_pending_queue >= cfg.bittorrent.max_active_requests, \
        f"cfg.bittorrent.max_pending_queue ({cfg.bittorrent.max_pending_queue}) should be >= max_active_requests ({cfg.bittorrent.max_active_requests})"
    
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
    
    # Validate parameter completion strategy
    valid_completion_strategies = ['global_model', 'zeros', 'local_model']
    assert cfg.bittorrent.parameter_completion in valid_completion_strategies, \
        f"cfg.bittorrent.parameter_completion must be one of {valid_completion_strategies}, but got {cfg.bittorrent.parameter_completion}"
    
    # Log configuration information
    if cfg.bittorrent.enable:
        logger.info(f"BitTorrent chunk exchange enabled with {cfg.bittorrent.chunk_selection} algorithm")
        logger.info(f"Parameter completion strategy: {cfg.bittorrent.parameter_completion}")
        if cfg.bittorrent.verbose:
            logger.info(f"BitTorrent configuration: timeout={cfg.bittorrent.timeout}s, "
                       f"upload_slots={cfg.bittorrent.max_upload_slots}, "
                       f"active_requests={cfg.bittorrent.max_active_requests}, "
                       f"pending_queue={cfg.bittorrent.max_pending_queue}, "
                       f"completion_ratio={cfg.bittorrent.min_completion_ratio}")


register_config("bittorrent", extend_bittorrent_cfg)