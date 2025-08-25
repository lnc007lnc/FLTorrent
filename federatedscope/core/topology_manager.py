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
        self.connections = connections  # Connection count control parameter
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
        Compute fully connected topology: all clients connected to all others (original mesh)
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
        Compute balanced mesh topology: each node connects to approximately 'connections' neighbors
        
        🔧 FIXED: Creates symmetric topology where total connections per node are balanced.
        Previous implementation only controlled outbound connections, leading to imbalanced topologies.
        """
        topology = {}
        
        if len(self.client_list) < 2:
            logger.warning("Mesh topology requires at least 2 clients")
            return {}
            
        n = len(self.client_list)
        max_possible = n - 1
        target_connections = min(self.connections, max_possible)
        
        if target_connections < self.connections:
            logger.warning(f"Reduced connections from {self.connections} to {target_connections} "
                         f"due to limited client count ({n})")
        
        # 🔧 FIX: Create balanced symmetric topology
        if target_connections >= max_possible:
            # Full mesh if target >= n-1
            for client_id in self.client_list:
                topology[client_id] = [c for c in self.client_list if c != client_id]
            logger.info(f"Mesh topology: full connectivity (target={target_connections} >= max={max_possible})")
        else:
            # 🆕 Create symmetric balanced mesh
            topology = {client_id: [] for client_id in self.client_list}
            edges = set()  # Track unique edges to avoid duplicates
            
            # Use deterministic seed for reproducible topology
            mesh_random = random.Random(42)
            
            # Create edge list with balanced distribution
            attempts = 0
            max_attempts = n * target_connections * 2  # Reasonable attempt limit
            
            while len(edges) < (n * target_connections) // 2 and attempts < max_attempts:
                # Randomly select two different nodes
                nodes = mesh_random.sample(self.client_list, 2)
                node1, node2 = min(nodes), max(nodes)  # Normalize edge order
                edge = (node1, node2)
                
                # Check if this edge would exceed target connections for either node
                if (edge not in edges and 
                    len(topology[node1]) < target_connections and 
                    len(topology[node2]) < target_connections):
                    
                    edges.add(edge)
                    topology[node1].append(node2)
                    topology[node2].append(node1)
                
                attempts += 1
            
            # 🔧 Ensure minimum connectivity if needed
            # Add missing connections to nodes that are under-connected
            for client_id in self.client_list:
                current_connections = len(topology[client_id])
                if current_connections < target_connections:
                    # Find potential neighbors not yet connected
                    available_neighbors = [
                        c for c in self.client_list 
                        if c != client_id and c not in topology[client_id] 
                        and len(topology[c]) < target_connections
                    ]
                    
                    # Add connections to reach target
                    needed = min(target_connections - current_connections, len(available_neighbors))
                    if needed > 0:
                        # Use client_id as seed for deterministic neighbor selection
                        client_random = random.Random(client_id)
                        chosen_neighbors = client_random.sample(available_neighbors, needed)
                        
                        for neighbor in chosen_neighbors:
                            if neighbor not in topology[client_id]:  # Double check
                                topology[client_id].append(neighbor)
                                topology[neighbor].append(client_id)
                                edges.add((min(client_id, neighbor), max(client_id, neighbor)))
            
            # Sort neighbor lists for consistency
            for client_id in topology:
                topology[client_id].sort()
            
            # Calculate statistics
            total_edges = len(edges)
            connections_per_node = [len(neighbors) for neighbors in topology.values()]
            avg_connections = sum(connections_per_node) / n
            min_connections = min(connections_per_node)
            max_connections = max(connections_per_node)
            
            logger.info(f"Balanced mesh topology: {n} nodes, {total_edges} edges")
            logger.info(f"Connections per node - Target: {target_connections}, "
                       f"Actual: [{min_connections}, {max_connections}], Avg: {avg_connections:.1f}")
        
        self.topology_graph = topology
        self.connection_requirements = topology.copy()
        
        logger.info(f"Mesh topology computed (balanced): {topology}")
        return topology
        
    def _compute_random_topology(self) -> Dict[int, List[int]]:
        """
        Compute balanced random topology: each node connects to approximately 'connections' neighbors.
        
        🔧 FIXED: Creates symmetric topology with balanced random connections.
        Previous implementation only controlled outbound connections, leading to imbalanced topologies.
        New implementation uses symmetric edge management with randomized balanced distribution.
        """
        topology = {}
        
        if len(self.client_list) < 2:
            logger.warning("Random topology requires at least 2 clients")
            return {}
            
        n = len(self.client_list)
        max_possible = n - 1
        target_connections = min(self.connections, max_possible)
        
        if target_connections < self.connections:
            logger.warning(f"Reduced connections from {self.connections} to {target_connections} "
                         f"due to limited client count ({n})")
        
        # 🔧 FIX: Create balanced symmetric random topology
        if target_connections >= max_possible:
            # Full mesh if target >= n-1
            for client_id in self.client_list:
                topology[client_id] = [c for c in self.client_list if c != client_id]
            logger.info(f"Random topology: full connectivity (target={target_connections} >= max={max_possible})")
        else:
            # 🆕 Create symmetric balanced random topology
            topology = {client_id: [] for client_id in self.client_list}
            edges = set()  # Track unique edges to avoid duplicates
            
            # Use deterministic seed for reproducible topology with randomness
            random_gen = random.Random(42)
            
            # Create target number of edges with random selection
            target_edges = (n * target_connections) // 2
            
            # Generate all possible edges and shuffle them randomly
            all_possible_edges = []
            for i, client1 in enumerate(self.client_list):
                for j, client2 in enumerate(self.client_list[i+1:], i+1):
                    all_possible_edges.append((client1, client2))
            
            random_gen.shuffle(all_possible_edges)  # Randomize edge order
            
            # Try to add edges while maintaining balance
            for edge in all_possible_edges:
                node1, node2 = edge
                
                # Check if adding this edge would exceed target connections for either node
                if (len(topology[node1]) < target_connections and 
                    len(topology[node2]) < target_connections):
                    
                    edges.add(edge)
                    topology[node1].append(node2)
                    topology[node2].append(node1)
                    
                    # Stop if we have enough edges
                    if len(edges) >= target_edges:
                        break
            
            # 🔧 Ensure minimum connectivity with random selection
            # Add missing connections to under-connected nodes
            under_connected = [client_id for client_id in self.client_list 
                             if len(topology[client_id]) < target_connections]
            
            while under_connected:
                # Randomly select an under-connected node
                client_id = random_gen.choice(under_connected)
                current_connections = len(topology[client_id])
                
                if current_connections >= target_connections:
                    under_connected.remove(client_id)
                    continue
                
                # Find potential neighbors not yet connected
                available_neighbors = [
                    c for c in self.client_list 
                    if c != client_id and c not in topology[client_id] 
                    and len(topology[c]) < target_connections
                ]
                
                if not available_neighbors:
                    # If no balanced neighbors available, try any unconnected neighbor
                    available_neighbors = [
                        c for c in self.client_list 
                        if c != client_id and c not in topology[client_id]
                    ]
                
                if available_neighbors:
                    # Randomly choose a neighbor to connect to
                    neighbor = random_gen.choice(available_neighbors)
                    topology[client_id].append(neighbor)
                    topology[neighbor].append(client_id)
                    edges.add((min(client_id, neighbor), max(client_id, neighbor)))
                else:
                    # No more connections possible for this node
                    under_connected.remove(client_id)
            
            # Sort neighbor lists for consistency
            for client_id in topology:
                topology[client_id].sort()
            
            # Calculate statistics
            total_edges = len(edges)
            connections_per_node = [len(neighbors) for neighbors in topology.values()]
            avg_connections = sum(connections_per_node) / n
            min_connections = min(connections_per_node)
            max_connections = max(connections_per_node)
            
            logger.info(f"Balanced random topology: {n} nodes, {total_edges} edges")
            logger.info(f"Connections per node - Target: {target_connections}, "
                       f"Actual: [{min_connections}, {max_connections}], Avg: {avg_connections:.1f}")
        
        self.topology_graph = topology
        self.connection_requirements = topology.copy()
        
        logger.info(f"Random topology computed (balanced): {topology}")
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