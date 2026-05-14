import logging

from federatedscope.core.configs.config import CN
from federatedscope.register import register_config

logger = logging.getLogger(__name__)


def extend_topology_cfg(cfg):
    # ---------------------------------------------------------------------- #
    # Network topology construction related options
    # ---------------------------------------------------------------------- #
    cfg.topology = CN()
    
    # Enable topology construction phase
    cfg.topology.use = False
    
    # Type of network topology to construct
    # Options: 'star', 'ring', 'fully_connected', 'mesh', 'random', 'tree', 'custom'
    cfg.topology.type = 'star'
    
    # Custom topology specification (used when type='custom')
    # Dict format: {client_id: [neighbor_ids]}
    cfg.topology.custom_graph = CN()
    
    # Number of connections per node (for 'mesh' and 'random' topologies)
    # mesh: each node connects to exactly this many neighbors
    # random: each node connects to at least this many neighbors (may have more)
    cfg.topology.connections = 2
    
    # Timeout for topology construction (seconds)
    cfg.topology.timeout = 60.0
    
    # Maximum number of connection attempts per client
    cfg.topology.max_connection_attempts = 3
    
    # Delay between connection attempts (seconds)
    cfg.topology.connection_retry_delay = 2.0
    
    # Whether to validate all connections before starting training
    cfg.topology.require_full_topology = True
    
    # Whether to save topology construction logs
    cfg.topology.save_construction_log = True
    
    # Directory for topology construction logs
    cfg.topology.log_dir = 'topology_logs'
    
    # Whether to print detailed topology construction progress
    cfg.topology.verbose = True

    # Gossip-style DFL configuration
    cfg.topology.gossip = CN()
    cfg.topology.gossip.enable = False           # Enable gossip mode
    cfg.topology.gossip.mode = 'neighbor_avg'    # 'pairwise' | 'neighbor_avg' | 'push_sum' | 'choco_sgd' | 'gossipfl'
    cfg.topology.gossip.iterations = 1           # Gossip iterations per FL round
    cfg.topology.gossip.mixing_weight = 0.5      # Self model weight (neighbor weight = 1 - w)

    # CHOCO-SGD baseline (Koloskova et al. ICML 2019)
    cfg.choco_sgd = CN()
    cfg.choco_sgd.enable = False                # Use CHOCO-SGD aggregation (overrides gossip mode behavior)
    cfg.choco_sgd.k_ratio = 0.01                # Top-k sparsification ratio (e.g. 0.01 = top 1%)
    cfg.choco_sgd.gamma = 0.1                   # Consensus mixing rate γ (typical 0.05–0.3)

    # GossipFL-style baseline (Tang et al. ICML 2022, simplified)
    # We use the S-BAG (block-wise top-k) sparsifier from GossipFL but combine it with
    # gamma-style consensus mixing (rather than uniform 1/(N+1) averaging) to ensure stable
    # convergence under sparse zero-initialised neighbor trackers; BAR (Bandwidth-Aware
    # Rendezvous) and 1-on-1 per-round exchange are NOT implemented since they only affect
    # wall-clock-per-round under bandwidth heterogeneity, not algorithmic convergence.
    cfg.gossipfl = CN()
    cfg.gossipfl.enable = False                 # Use GossipFL-style aggregation
    cfg.gossipfl.k_ratio = 0.01                 # Top-k sparsification ratio (per block)
    cfg.gossipfl.block_size = 0                 # 0 = global topk; >0 = per-block topk (S-BAG)
    cfg.gossipfl.gamma = 0.2                    # Consensus mixing rate (matches CHOCO-SGD form)
    cfg.gossipfl.bandwidth_aware = False        # Reserved (not implemented)

    # Register check function
    cfg.register_cfg_check_fun(assert_topology_cfg)


def assert_topology_cfg(cfg):
    """Validate topology configuration"""
    
    if not cfg.topology.use:
        return  # Skip validation if topology is disabled
    
    # Validate topology type
    valid_types = ['star', 'ring', 'fully_connected', 'mesh', 'random', 'tree', 'custom']
    assert cfg.topology.type in valid_types, \
        f"cfg.topology.type must be one of {valid_types}, but got {cfg.topology.type}"
    
    # Validate timeout
    assert cfg.topology.timeout > 0, \
        f"cfg.topology.timeout must be positive, but got {cfg.topology.timeout}"
    
    # Validate connection attempts
    assert cfg.topology.max_connection_attempts > 0, \
        f"cfg.topology.max_connection_attempts must be positive, " \
        f"but got {cfg.topology.max_connection_attempts}"
    
    # Validate retry delay
    assert cfg.topology.connection_retry_delay >= 0, \
        f"cfg.topology.connection_retry_delay must be non-negative, " \
        f"but got {cfg.topology.connection_retry_delay}"
    
    # Validate custom topology if specified
    if cfg.topology.type == 'custom':
        # Check if custom_graph has any content (for CN type, check if it has items)
        custom_graph = cfg.topology.custom_graph
        if hasattr(custom_graph, 'items') and len(custom_graph) > 0:
            # Validate that all keys and values are integers (client IDs)
            for client_id, neighbors in custom_graph.items():
                assert isinstance(client_id, int), \
                    f"Client ID must be integer, got {type(client_id)}: {client_id}"
                assert isinstance(neighbors, list), \
                    f"Neighbors must be a list, got {type(neighbors)}: {neighbors}"
                for neighbor in neighbors:
                    assert isinstance(neighbor, int), \
                        f"Neighbor ID must be integer, got {type(neighbor)}: {neighbor}"
    
    # Topology construction requires distributed mode
    if cfg.topology.use and hasattr(cfg, 'federate'):
        assert cfg.federate.mode == 'distributed', \
            "Topology construction requires distributed mode. " \
            f"Set cfg.federate.mode='distributed', but got {cfg.federate.mode}"
    
    # Topology construction requires multiple clients
    if cfg.topology.use and hasattr(cfg, 'federate') and cfg.federate.client_num > 0:
        min_clients_required = {
            'star': 2,
            'ring': 3,
            'fully_connected': 2,
            'mesh': 2,
            'random': 2,
            'tree': 2,
            'custom': 1
        }
        
        min_required = min_clients_required.get(cfg.topology.type, 2)
        assert cfg.federate.client_num >= min_required, \
            f"Topology type '{cfg.topology.type}' requires at least {min_required} " \
            f"clients, but only {cfg.federate.client_num} configured"
    
    # Log configuration warnings and info
    if cfg.topology.use:
        logger.info(f"Network topology construction enabled: {cfg.topology.type}")
        
        if cfg.topology.type == 'mesh' and hasattr(cfg, 'federate') and cfg.federate.client_num > 10:
            logger.warning(f"Mesh topology with {cfg.federate.client_num} clients "
                          f"will create {cfg.federate.client_num * (cfg.federate.client_num - 1)} "
                          f"connections. Consider using a different topology for better scalability.")
        
        if not cfg.topology.require_full_topology:
            logger.warning("cfg.topology.require_full_topology=False: "
                          "Training may start with incomplete topology")

    # Validate gossip configuration
    if cfg.topology.use and hasattr(cfg.topology, 'gossip') and cfg.topology.gossip.enable:
        # Validate gossip mode
        valid_gossip_modes = ['pairwise', 'neighbor_avg', 'push_sum', 'choco_sgd', 'gossipfl']
        assert cfg.topology.gossip.mode in valid_gossip_modes, \
            f"cfg.topology.gossip.mode must be one of {valid_gossip_modes}, " \
            f"but got {cfg.topology.gossip.mode}"

        # Validate gossip iterations
        assert cfg.topology.gossip.iterations >= 1, \
            f"cfg.topology.gossip.iterations must be >= 1, " \
            f"but got {cfg.topology.gossip.iterations}"

        # Validate mixing weight
        assert 0.0 < cfg.topology.gossip.mixing_weight < 1.0, \
            f"cfg.topology.gossip.mixing_weight must be in (0.0, 1.0), " \
            f"but got {cfg.topology.gossip.mixing_weight}"

        logger.info(f"Gossip-style DFL enabled: mode={cfg.topology.gossip.mode}, "
                   f"iterations={cfg.topology.gossip.iterations}, "
                   f"mixing_weight={cfg.topology.gossip.mixing_weight}")


register_config("topology", extend_topology_cfg)