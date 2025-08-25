"""
Network Topology Manager for FederatedScope

This module handles network topology construction for distributed federated learning,
allowing clients to form different network structures (star, ring, mesh, etc.)
beyond the default server-client model.
"""

import logging
import time
import random
from typing import Dict, List, Set, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class TopologyType(Enum):
    """Supported network topology types"""
    STAR = "star"                       # A-B-C in sequence, B connects to A and C
    RING = "ring"                       # A-B-C-A circular connections
    FULLY_CONNECTED = "fully_connected" # All-to-all connections (original mesh)
    MESH = "mesh"                       # Limited connections per node based on degree
    RANDOM = "random"                   # Random connections ensuring minimum degree
    TREE = "tree"                       # Binary tree structure
    CUSTOM = "custom"                   # User-defined topology


class TopologyManager:
    """
    Manages network topology construction for federated learning clients
    """
    
    def __init__(self, topology_type: str = "star", client_list: List[int] = None, 
                 connections: int = 2):
        """
        Initialize topology manager
        
        Args:
            topology_type: Type of topology to build
            client_list: List of client IDs to include in topology
            connections: Number of connections per node:
                        - mesh: exactly this many connections per node
                        - random: minimum connections per node (can have more)
        """
        self.topology_type = TopologyType(topology_type.lower())
        self.client_list = client_list or []
        self.connections = connections  # 连接数控制参数
        self.topology_graph = {}  # Dict[client_id] -> List[neighbor_ids]
        self.connection_requirements = {}  # Expected connections for each client
        self.established_connections = {}  # Actual established connections
        self.connection_progress = {}  # Track connection progress per client
        
        logger.info(f"TopologyManager initialized: {self.topology_type.value} topology "
                   f"for clients {self.client_list} (connections: {self.connections})")
    
    def compute_topology(self) -> Dict[int, List[int]]:
        """
        Compute the network topology based on the selected type
        
        Returns:
            Dict mapping each client to its required neighbors
        """
        if self.topology_type == TopologyType.STAR:
            return self._compute_star_topology()
        elif self.topology_type == TopologyType.RING:
            return self._compute_ring_topology()
        elif self.topology_type == TopologyType.FULLY_CONNECTED:
            return self._compute_fully_connected_topology()
        elif self.topology_type == TopologyType.MESH:
            return self._compute_mesh_topology()
        elif self.topology_type == TopologyType.RANDOM:
            return self._compute_random_topology()
        elif self.topology_type == TopologyType.TREE:
            return self._compute_tree_topology()
        else:
            raise ValueError(f"Unsupported topology type: {self.topology_type}")
    
    def _compute_star_topology(self) -> Dict[int, List[int]]:
        """
        Compute star topology: clients arranged in sequence, each connects to neighbors
        Example: [1, 2, 3] -> {1: [2], 2: [1, 3], 3: [2]}
        """
        topology = {}
        
        if len(self.client_list) < 2:
            logger.warning("Star topology requires at least 2 clients")
            return {}
        
        for i, client_id in enumerate(self.client_list):
            neighbors = []
            
            # Connect to previous client (if exists)
            if i > 0:
                neighbors.append(self.client_list[i - 1])
            
            # Connect to next client (if exists)
            if i < len(self.client_list) - 1:
                neighbors.append(self.client_list[i + 1])
            
            topology[client_id] = neighbors
        
        self.topology_graph = topology
        self.connection_requirements = topology.copy()
        
        logger.info(f"Star topology computed: {topology}")
        return topology
    
    def _compute_ring_topology(self) -> Dict[int, List[int]]:
        """
        Compute ring topology: clients connected in a circle
        Example: [1, 2, 3] -> {1: [2, 3], 2: [1, 3], 3: [1, 2]}
        """
        topology = {}
        
        if len(self.client_list) < 3:
            logger.warning("Ring topology requires at least 3 clients")
            return {}
        
        for i, client_id in enumerate(self.client_list):
            # Connect to previous and next client (circular)
            prev_idx = (i - 1) % len(self.client_list)
            next_idx = (i + 1) % len(self.client_list)
            
            neighbors = [self.client_list[prev_idx], self.client_list[next_idx]]
            topology[client_id] = neighbors
        
        self.topology_graph = topology
        self.connection_requirements = topology.copy()
        
        logger.info(f"Ring topology computed: {topology}")
        return topology
    
    def _compute_fully_connected_topology(self) -> Dict[int, List[int]]:
        """
        Compute fully connected topology: all clients connected to all others (原mesh)
        """
        topology = {}
        
        for client_id in self.client_list:
            # Connect to all other clients
            neighbors = [c for c in self.client_list if c != client_id]
            topology[client_id] = neighbors
        
        self.topology_graph = topology
        self.connection_requirements = topology.copy()
        
        logger.info(f"Fully connected topology computed: {topology}")
        return topology
        
    def _compute_mesh_topology(self) -> Dict[int, List[int]]:
        """
        Compute mesh topology: each node connects to exactly 'connections' neighbors
        """
        topology = {}
        
        if len(self.client_list) < 2:
            logger.warning("Mesh topology requires at least 2 clients")
            return {}
            
        # Ensure connections is reasonable
        max_possible = len(self.client_list) - 1
        actual_connections = min(self.connections, max_possible)
        
        if actual_connections < self.connections:
            logger.warning(f"Reduced connections from {self.connections} to {actual_connections} "
                         f"due to limited client count ({len(self.client_list)})")
        
        for client_id in self.client_list:
            # Get all other clients
            other_clients = [c for c in self.client_list if c != client_id]
            
            # Select exactly actual_connections neighbors deterministically
            # Use client_id as seed for consistent selection across runs
            local_random = random.Random(client_id)
            neighbors = local_random.sample(other_clients, min(actual_connections, len(other_clients)))
            topology[client_id] = sorted(neighbors)  # Sort for consistency
        
        self.topology_graph = topology
        self.connection_requirements = topology.copy()
        
        logger.info(f"Mesh topology computed with connections={actual_connections}: {topology}")
        return topology
        
    def _compute_random_topology(self) -> Dict[int, List[int]]:
        """
        Compute random topology: each node connects to at least 'connections' random neighbors,
        with some additional random connections for variety. If connections >= (nodes-1), 
        automatically becomes fully connected topology.
        """
        topology = {}
        
        if len(self.client_list) < 2:
            logger.warning("Random topology requires at least 2 clients")
            return {}
            
        # Check if requested connections would result in full connectivity
        max_possible = len(self.client_list) - 1
        
        if self.connections >= max_possible:
            logger.info(f"Random topology with connections={self.connections} >= {max_possible}, "
                       f"creating fully connected topology instead")
            # Create fully connected topology
            for client_id in self.client_list:
                neighbors = [c for c in self.client_list if c != client_id]
                topology[client_id] = neighbors
        else:
            # Create random topology with specified minimum connections
            min_connections = self.connections
            logger.info(f"Creating random topology with min_connections={min_connections}")
            
            # Global random generator for extra connections
            global_random = random.Random(42)  # Fixed seed for reproducibility
            
            for client_id in self.client_list:
                other_clients = [c for c in self.client_list if c != client_id]
                
                # Start with minimum required connections
                local_random = random.Random(client_id * 13)  # Different seed per client for consistency
                neighbors = local_random.sample(other_clients, min_connections)
                
                # Add 0-2 additional random connections for variety (if possible)
                remaining_clients = [c for c in other_clients if c not in neighbors]
                if remaining_clients:
                    extra_count = global_random.randint(0, min(2, len(remaining_clients)))
                    if extra_count > 0:
                        extra_neighbors = local_random.sample(remaining_clients, extra_count)
                        neighbors.extend(extra_neighbors)
                
                topology[client_id] = sorted(neighbors)  # Sort for consistency
        
        self.topology_graph = topology
        self.connection_requirements = topology.copy()
        
        total_connections = sum(len(neighbors) for neighbors in topology.values())
        avg_connections = total_connections / len(topology)
        logger.info(f"Random topology computed: {len(topology)} nodes, "
                   f"total_connections={total_connections}, avg={avg_connections:.2f}")
        return topology
    
    def _compute_tree_topology(self) -> Dict[int, List[int]]:
        """
        Compute tree topology: binary tree structure
        """
        topology = {}
        
        if len(self.client_list) < 2:
            logger.warning("Tree topology requires at least 2 clients")
            return {}
        
        # Sort clients for consistent tree structure
        sorted_clients = sorted(self.client_list)
        
        for i, client_id in enumerate(sorted_clients):
            neighbors = []
            
            # Parent connection (except root)
            if i > 0:
                parent_idx = (i - 1) // 2
                neighbors.append(sorted_clients[parent_idx])
            
            # Left child
            left_child_idx = 2 * i + 1
            if left_child_idx < len(sorted_clients):
                neighbors.append(sorted_clients[left_child_idx])
            
            # Right child
            right_child_idx = 2 * i + 2
            if right_child_idx < len(sorted_clients):
                neighbors.append(sorted_clients[right_child_idx])
            
            topology[client_id] = neighbors
        
        self.topology_graph = topology
        self.connection_requirements = topology.copy()
        
        logger.info(f"Tree topology computed: {topology}")
        return topology
    
    def get_client_connections(self, client_id: int) -> List[int]:
        """
        Get the list of clients that a specific client should connect to
        
        Args:
            client_id: ID of the client
            
        Returns:
            List of neighbor client IDs
        """
        return self.topology_graph.get(client_id, [])
    
    def record_connection_established(self, from_client: int, to_client: int) -> bool:
        """
        Record that a connection has been established between two clients
        
        Args:
            from_client: Client that initiated the connection
            to_client: Client that received the connection
            
        Returns:
            True if this connection was expected, False otherwise
        """
        # Initialize if needed
        if from_client not in self.established_connections:
            self.established_connections[from_client] = set()
        
        # Record the connection
        self.established_connections[from_client].add(to_client)
        
        # Check if this was an expected connection
        expected_neighbors = set(self.topology_graph.get(from_client, []))
        is_expected = to_client in expected_neighbors
        
        if is_expected:
            logger.info(f"✅ Expected connection established: {from_client} -> {to_client}")
        else:
            logger.warning(f"⚠️ Unexpected connection established: {from_client} -> {to_client}")
        
        return is_expected
    
    def is_topology_complete(self) -> bool:
        """
        Check if the entire topology has been established
        
        Returns:
            True if all required connections are established
        """
        # If no topology requirements exist, topology is not complete
        if not self.connection_requirements:
            logger.debug("No topology requirements defined - topology not complete")
            return False
        
        for client_id, required_neighbors in self.connection_requirements.items():
            established_neighbors = self.established_connections.get(client_id, set())
            required_set = set(required_neighbors)
            
            if not required_set.issubset(established_neighbors):
                missing = required_set - established_neighbors
                logger.debug(f"Client {client_id} missing connections to: {missing}")
                return False
        
        logger.info("🎉 Network topology construction completed!")
        return True
    
    def get_connection_progress(self) -> Dict[int, Dict[str, int]]:
        """
        Get connection progress for each client
        
        Returns:
            Dict with connection progress statistics
        """
        progress = {}
        
        for client_id in self.client_list:
            required = set(self.connection_requirements.get(client_id, []))
            established = self.established_connections.get(client_id, set())
            
            progress[client_id] = {
                'required': len(required),
                'established': len(established & required),
                'missing': len(required - established),
                'extra': len(established - required)
            }
        
        return progress
    
    def print_topology_status(self):
        """Print current topology construction status"""
        logger.info(f"Topology Status ({self.topology_type.value.upper()}):")
        logger.info(f"   Clients: {self.client_list}")
        logger.info(f"   Target topology: {self.topology_graph}")
        
        progress = self.get_connection_progress()
        for client_id, stats in progress.items():
            status = "complete" if stats['missing'] == 0 else "in progress"
            logger.info(f"   Client {client_id}: {stats['established']}/{stats['required']} connections ({status})")
            
            if stats['missing'] > 0:
                required = set(self.connection_requirements.get(client_id, []))
                established = self.established_connections.get(client_id, set())
                missing = required - established
                logger.info(f"      Missing: {list(missing)}")
        
        if self.is_topology_complete():
            logger.info("Topology construction COMPLETE!")
        else:
            total_required = sum(len(neighbors) for neighbors in self.connection_requirements.values())
            total_established = sum(
                len(self.established_connections.get(client_id, set()) & set(neighbors))
                for client_id, neighbors in self.connection_requirements.items()
            )
            logger.info(f"   Overall progress: {total_established}/{total_required} connections")