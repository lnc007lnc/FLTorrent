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
    
    # Register check function
    cfg.register_cfg_check_fun(assert_topology_cfg)


def assert_topology_cfg(cfg):
    """Validate topology configuration"""
    
    if not cfg.topology.use:
        return  # Skip validation if topology is disabled
    
    # Validate topology type
    valid_types = ['star', 'ring', 'mesh', 'tree', 'custom']
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
            'mesh': 2,
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


register_config("topology", extend_topology_cfg)