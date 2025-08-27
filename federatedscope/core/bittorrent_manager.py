"""
BitTorrent Protocol Manager
Implements classic BitTorrent protocol for chunk exchange in FederatedScope
"""

import time
import hashlib
import random
import logging
import threading
from typing import Dict, Set, List, Tuple, Optional, Any
from collections import defaultdict

logger = logging.getLogger(__name__)


class BitTorrentManager:
    """Manage BitTorrent protocol core logic (including critical bug fixes)"""
    
    def __init__(self, client_id: int, round_num: int, chunk_manager, comm_manager, neighbors: List[int]):
        self.client_id = client_id
        self.round_num = round_num  # 🔴 Critical: current round
        self.chunk_manager = chunk_manager
        self.comm_manager = comm_manager
        self.neighbors = neighbors  # 🔧 Fix: directly pass neighbor list
        
        # BitTorrent status
        self.peer_bitfields: Dict[int, Dict] = {}  # {peer_id: bitfield}
        self.interested_in: Set[int] = set()  # Interested peers
        self.interested_by: Set[int] = set()  # Peers interested in me
        self.choked_peers: Set[int] = set()  # Choked peers
        self.unchoked_peers: Set[int] = set()  # Unchoked peers (can download)
        
        # Performance management
        self.download_rate: Dict[int, float] = {}  # {peer_id: bytes/sec}
        self.upload_rate: Dict[int, float] = {}  # {peer_id: bytes/sec}
        self.last_unchoke_time = 0
        self.optimistic_unchoke_peer = None
        
        # 🔧 Fix: simplify state management, avoid complex locking mechanism
        # FederatedScope is single-threaded message-driven, no locks needed
        
        # 🔧 Bug fix 2: deadlock prevention mechanism
        self.ever_unchoked: Set[int] = set()  # Record peers that were ever unchoked
        self.last_activity: Dict[int, float] = {}  # {peer_id: timestamp} last activity time
        self.stalled_threshold = 30.0  # 30 seconds without activity considered stalled
        
        # 🔧 Bug fix 3: message retransmission mechanism
        self.pending_requests: Dict[Tuple, Tuple[int, float]] = {}  # {(source_id, chunk_id): (peer_id, timestamp)}
        self.request_timeout = 5.0  # 5 second request timeout
        self.max_retries = 3  # Maximum retry count
        self.retry_count: Dict[Tuple, int] = {}  # {(source_id, chunk_id): count}
        
        # 🆕 Dual pool request management system - solves priority inversion and duplicate selection issues
        self.MAX_ACTIVE_REQUESTS = 2  # Active request pool size: actual concurrent request count sent
        self.MAX_PENDING_QUEUE = 2    # Pending queue pool size: pre-selected chunk queue size
        self.pending_queue: List[Tuple] = []  # Pending queue: chunk list sorted by importance
        
        # 🔧 Bug fix 4: ensure minimum unchoke count
        self.MIN_UNCHOKE_SLOTS = 1  # Keep at least 1 unchoke to prevent complete deadlock
        self.MAX_UPLOAD_SLOTS = 4
        
        # 🔧 Fix: no background threads, check timeouts through message callbacks
        self.last_timeout_check = time.time()
        
        # Statistics
        self.total_downloaded = 0
        self.total_uploaded = 0
        self.chunks_per_client = 10  # Default value, configurable
        
        # 🔧 CRITICAL FIX: Exchange state management
        self.is_stopped = False  # Stop flag for exchange termination
        
        logger.info(f"[BT] BitTorrentManager initialized for client {client_id}, round {round_num}")
        
    def start_exchange(self):
        """Start BitTorrent chunk exchange process (no Tracker needed)"""
        logger.info(f"[BT] Client {self.client_id}: Starting BitTorrent exchange")
        logger.info(f"[BT] Client {self.client_id}: Neighbors: {self.neighbors}")
        
        # 1. Send bitfield directly to all topology neighbors
        for neighbor_id in self.neighbors:
            logger.info(f"[BT] Client {self.client_id}: Sending bitfield to neighbor {neighbor_id}")
            self._send_bitfield(neighbor_id)
        
        # 2. Start periodic unchoke algorithm (every 10 seconds)
        self._schedule_regular_unchoke()
        
        # 3. Start optimistic unchoke (every 30 seconds)
        self._schedule_optimistic_unchoke()
        
    def handle_bitfield(self, sender_id: int, bitfield_content: Dict):
        """Handle received bitfield messages (containing importance scores)"""
        # 🔧 CRITICAL FIX: Check if exchange is stopped
        if self.is_stopped:
            logger.debug(f"[BT] Client {self.client_id}: Ignoring bitfield from peer {sender_id} - exchange stopped")
            return
        
        # 🆕 Handle new format bitfield (containing importance scores)
        if isinstance(bitfield_content, dict) and 'bitfield' in bitfield_content:
            # New format: {round_num: x, bitfield: [{round, source, chunk, importance_score}, ...]}
            bitfield_list = bitfield_content.get('bitfield', [])
            
            # Convert to internal format and store importance scores
            bitfield = {}
            if not hasattr(self, 'peer_importance_scores'):
                self.peer_importance_scores = {}  # {peer_id: {chunk_key: importance_score}}
            
            if sender_id not in self.peer_importance_scores:
                self.peer_importance_scores[sender_id] = {}
            
            for chunk_entry in bitfield_list:
                chunk_key = (chunk_entry['round'], chunk_entry['source'], chunk_entry['chunk'])
                bitfield[chunk_key] = True
                
                # 🆕 Store importance scores
                importance_score = chunk_entry.get('importance_score', 0.0)
                self.peer_importance_scores[sender_id][chunk_key] = importance_score
                
                logger.debug(f"[BT] Client {self.client_id}: Peer {sender_id} has chunk {chunk_key} with importance {importance_score:.4f}")
        else:
            # Compatible with old format
            bitfield = bitfield_content
        
        self.peer_bitfields[sender_id] = bitfield
        logger.debug(f"[BT] Client {self.client_id}: Received bitfield from peer {sender_id} with {len(bitfield)} chunks")
        
        # 🔧 Debug: output detailed bitfield analysis
        logger.debug(f"[BT] Client {self.client_id}: BitTorrent Manager received bitfield from peer {sender_id}:")
        if bitfield:
            for chunk_key, has_chunk in bitfield.items():
                round_num, source_id, chunk_id = chunk_key
                importance_score = self.peer_importance_scores.get(sender_id, {}).get(chunk_key, 0.0)
                logger.debug(f"[BT] Client {self.client_id}: - Round {round_num}, Source {source_id}, Chunk {chunk_id}: {has_chunk} (importance: {importance_score:.4f})")
        else:
            logger.warning(f"[BT] Client {self.client_id}: ⚠️ BitTorrent Manager got EMPTY bitfield from peer {sender_id}!")
        
        # Check if peer has chunks I need
        if self._has_interesting_chunks(sender_id):
            logger.debug(f"[BT] Client {self.client_id}: Peer {sender_id} has interesting chunks, sending interested")
            self._send_interested(sender_id)
        else:
            logger.info(f"[BT] Client {self.client_id}: Peer {sender_id} has no interesting chunks")
            
    def handle_interested(self, sender_id: int):
        """Handle interested messages"""
        self.interested_by.add(sender_id)
        logger.debug(f"[BT] Client {self.client_id}: Peer {sender_id} is interested")
        # Decide whether to unchoke based on current upload slots
        self._evaluate_unchoke(sender_id)
        
    def handle_request(self, sender_id: int, round_num: int, source_client_id: int, chunk_id: int):
        """Handle chunk requests"""
        # 🔧 CRITICAL FIX: Check if exchange is stopped
        if self.is_stopped:
            logger.debug(f"[BT] Client {self.client_id}: Ignoring request from peer {sender_id} - exchange stopped")
            return
        logger.debug(f"[BT-HANDLE] Client {self.client_id}: Handling request from {sender_id} for chunk {source_client_id}:{chunk_id}")
        
        # 🔴 Verify round matching
        if round_num != self.round_num:
            logger.warning(f"[BT-HANDLE] Client {self.client_id}: Round mismatch - Request round {round_num} vs BitTorrent round {self.round_num}")
            logger.warning(f"[BT-HANDLE] Client {self.client_id}: Skipping request due to round mismatch")
            return
            
        if sender_id not in self.choked_peers:
            logger.debug(f"[BT-HANDLE] Client {self.client_id}: Peer {sender_id} is not choked, processing request")
            # Send chunk data
            # 🔴 Add round_num parameter to get_chunk_data
            logger.debug(f"[BT-HANDLE] Client {self.client_id}: Querying chunk_data with params (round={round_num}, source_client={source_client_id}, chunk_id={chunk_id})")
            chunk_data = self.chunk_manager.get_chunk_data(round_num, source_client_id, chunk_id)
            if chunk_data is not None:
                # Send chunk data, even if chunk is empty
                chunk_size = len(chunk_data) if hasattr(chunk_data, '__len__') else 0
                if chunk_size > 0:
                    logger.debug(f"[BT-HANDLE] Client {self.client_id}: Found non-empty chunk data (size={chunk_size}), sending piece to {sender_id}")
                else:
                    logger.debug(f"[BT-HANDLE] Client {self.client_id}: Found empty chunk data (size={chunk_size}), sending empty piece to {sender_id}")
                
                self._send_piece(sender_id, round_num, source_client_id, chunk_id, chunk_data)
                logger.debug(f"[BT-HANDLE] Client {self.client_id}: Successfully sent chunk {source_client_id}:{chunk_id} to peer {sender_id}")
            else:
                logger.warning(f"[BT-HANDLE] Client {self.client_id}: Chunk {source_client_id}:{chunk_id} not found in database (round={round_num})")
                logger.warning(f"[BT-HANDLE] Client {self.client_id}: Database query returned: {chunk_data}")
        else:
            logger.info(f"[BT-HANDLE] Client {self.client_id}: Peer {sender_id} is choked, ignoring request")
            
    def handle_piece(self, sender_id: int, round_num: int, source_client_id: int, chunk_id: int, chunk_data: bytes, checksum: str):
        """
        Handle received chunk data (with integrity verification)
        🔴 Critical change: verify round_num matching
        """
        # 🔧 CRITICAL FIX: Check if exchange is stopped
        if self.is_stopped:
            logger.debug(f"[BT] Client {self.client_id}: Ignoring piece from peer {sender_id} - exchange stopped")
            return
        logger.debug(f"[BT-PIECE] Client {self.client_id}: Received piece from {sender_id} for chunk {source_client_id}:{chunk_id} (piece_round={round_num}, bt_round={self.round_num}, timestamp={time.time():.3f})")
        
        # 🔴 Verify if rounds match
        if round_num != self.round_num:
            logger.warning(f"[BT-PIECE] Client {self.client_id}: Round mismatch - Piece round {round_num} vs BitTorrent round {self.round_num}")
            logger.warning(f"[BT-PIECE] Client {self.client_id}: Rejecting piece due to round mismatch")
            return False
            
        # 🔧 Fix: chunk_data is now base64 encoded string, need to decode before verification
        logger.debug(f"[BT-PIECE] Client {self.client_id}: Received encoded chunk data, type={type(chunk_data)}, size={len(chunk_data)}")
        
        # Decode base64 data
        try:
            import base64
            import pickle
            decoded_data = base64.b64decode(chunk_data.encode('utf-8'))
            logger.debug(f"[BT-PIECE] Client {self.client_id}: Decoded base64 data, size={len(decoded_data)}")
        except Exception as e:
            logger.error(f"[BT-PIECE] Client {self.client_id}: Failed to decode base64 data: {e}")
            return False
        
        # Calculate hash of decoded serialized data
        calculated_checksum = hashlib.sha256(decoded_data).hexdigest()
        logger.debug(f"[BT-PIECE] Client {self.client_id}: Checksum verification - calculated={calculated_checksum[:8]}..., received={checksum[:8]}..., size={len(decoded_data)}")
        
        if calculated_checksum != checksum:
            logger.error(f"[BT-PIECE] Client {self.client_id}: Chunk integrity check failed for {source_client_id}:{chunk_id}")
            logger.error(f"[BT-PIECE] Client {self.client_id}: Expected={checksum}, Got={calculated_checksum}")
            # Re-request this chunk
            chunk_key = (round_num, source_client_id, chunk_id)
            # 🔧 FIX: Atomic retry count increment (thread-safe dict operations)
            self.retry_count[chunk_key] = self.retry_count.get(chunk_key, 0) + 1
            logger.warning(f"[BT-PIECE] Client {self.client_id}: Retry count for chunk {chunk_key}: {self.retry_count[chunk_key]}")
            return False
        
        # 🔧 Deserialize to get original chunk data
        try:
            deserialized_data = pickle.loads(decoded_data)
            logger.debug(f"[BT-PIECE] Client {self.client_id}: Successfully deserialized chunk data, type={type(deserialized_data)}")
        except Exception as e:
            logger.error(f"[BT-PIECE] Client {self.client_id}: Failed to deserialize chunk data: {e}")
            return False
        
        # Save to local database
        # 🔴 Pass round_num to save method, use deserialized data
        self.chunk_manager.save_remote_chunk(round_num, source_client_id, chunk_id, deserialized_data)
        
        # Clear pending requests
        chunk_key = (round_num, source_client_id, chunk_id)
        if chunk_key in self.pending_requests:
            logger.debug(f"[BT-PIECE] Client {self.client_id}: Clearing pending request for chunk {chunk_key}")
            del self.pending_requests[chunk_key]
            logger.debug(f"[BT-PIECE] Client {self.client_id}: Active pool: {len(self.pending_requests)}/{self.MAX_ACTIVE_REQUESTS}, Queue: {len(self.pending_queue)}/{self.MAX_PENDING_QUEUE}")
            
            # 🆕 Dual pool system: transfer requests from queue pool to active pool
            self._transfer_from_queue_to_active()
        else:
            logger.debug(f"[BT-PIECE] Client {self.client_id}: No pending request found for chunk {chunk_key}")
        
        # Send have messages to all neighbors
        # 🔴 Pass round_num information
        logger.debug(f"[BT-PIECE] Client {self.client_id}: Broadcasting have message for chunk {source_client_id}:{chunk_id}")
        self._broadcast_have(round_num, source_client_id, chunk_id)
        
        # Update download rate and activity time
        self._update_download_rate(sender_id, len(decoded_data))
        self.last_activity[sender_id] = time.time()
        self.total_downloaded += len(decoded_data)
        
        logger.debug(f"[BT] Client {self.client_id}: Received chunk {source_client_id}:{chunk_id} from peer {sender_id}")
        return True
        
    def handle_have(self, sender_id: int, round_num: int, source_client_id: int, chunk_id: int, importance_score: float = 0.0):
        """Handle have messages (containing importance scores)"""
        if round_num != self.round_num:
            return
            
        chunk_key = (round_num, source_client_id, chunk_id)
        if sender_id not in self.peer_bitfields:
            self.peer_bitfields[sender_id] = {}
        self.peer_bitfields[sender_id][chunk_key] = True
        
        # 🆕 Store importance scores
        if not hasattr(self, 'peer_importance_scores'):
            self.peer_importance_scores = {}
        if sender_id not in self.peer_importance_scores:
            self.peer_importance_scores[sender_id] = {}
        
        self.peer_importance_scores[sender_id][chunk_key] = importance_score
        
        logger.debug(f"[BT] Client {self.client_id}: Peer {sender_id} has chunk {source_client_id}:{chunk_id} with importance {importance_score:.4f}")
        
    def handle_choke(self, sender_id: int):
        """Handle choke messages"""
        self.choked_peers.add(sender_id)
        logger.debug(f"[BT] Client {self.client_id}: Choked by peer {sender_id}")
        
    def handle_unchoke(self, sender_id: int):
        """Handle unchoke messages"""
        self.choked_peers.discard(sender_id)
        logger.debug(f"[BT] Client {self.client_id}: Unchoked by peer {sender_id}")
        
    def _importance_guided_selection(self) -> Optional[Tuple]:
        """Importance-guided chunk selection algorithm (importance-first + rarest-first hybrid strategy)"""
        # 🆕 Importance score difference threshold (chunks with difference below this value are considered similar)
        IMPORTANCE_SIMILARITY_THRESHOLD = 0.01  # 0.01 means chunks with importance difference < 1% are considered similar
        
        # Count rarity of each chunk
        chunk_availability = {}
        # 🔧 FIX: Create thread-safe copy to avoid "dictionary changed size during iteration"
        peer_bitfields_copy = dict(self.peer_bitfields)
        for peer_id, bitfield in peer_bitfields_copy.items():
            bitfield_copy = dict(bitfield) if isinstance(bitfield, dict) else bitfield
            for chunk_key, has_chunk in bitfield_copy.items():
                # 🔴 Only consider chunks from current round
                if has_chunk and chunk_key[0] == self.round_num:
                    chunk_availability[chunk_key] = chunk_availability.get(chunk_key, 0) + 1
        
        # Select available chunks
        my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
        
        # 🔧 Exclude chunks already owned and being requested
        needed_chunks = []
        for chunk_key, availability_count in chunk_availability.items():
            if chunk_key not in my_bitfield and chunk_key not in self.pending_requests:
                # 🆕 Get chunk importance score
                importance_score = self._get_chunk_importance_score(chunk_key)
                needed_chunks.append({
                    'chunk_key': chunk_key,
                    'availability': availability_count,
                    'importance': importance_score
                })
        
        # 🔧 Debug information
        pending_chunks = [k for k, v in chunk_availability.items() if k in self.pending_requests]
        already_have = [k for k, v in chunk_availability.items() if k in my_bitfield]
        
        if not chunk_availability:
            if not hasattr(self, '_logged_no_chunks'):
                logger.info(f"[BT] Client {self.client_id}: No chunks available from peers. Peer count: {len(self.peer_bitfields)}")
                # 🔧 FIX: Use thread-safe copy for logging iteration
                peer_bitfields_copy = dict(self.peer_bitfields)
                for peer_id, bitfield in peer_bitfields_copy.items():
                    logger.info(f"[BT] Client {self.client_id}: Peer {peer_id} bitfield size: {len(bitfield)}")
                self._logged_no_chunks = True
        elif not needed_chunks:
            total_available = len(chunk_availability)
            pending_count = len(pending_chunks)
            have_count = len(already_have)
            logger.debug(f"[BT] Client {self.client_id}: No needed chunks - Total: {total_available}, Already have: {have_count}, Pending: {pending_count}")
            
            if not hasattr(self, '_logged_all_chunks'):
                logger.info(f"[BT] Client {self.client_id}: All chunks handled - My chunks: {len(my_bitfield)}, Pending requests: {len(self.pending_requests)}")
                self._logged_all_chunks = True
        
        if needed_chunks:
            # 🆕 Importance-guided selection strategy
            logger.debug(f"[BT] Client {self.client_id}: Evaluating {len(needed_chunks)} candidate chunks for selection")
            
            # 1. Sort by importance score in descending order
            needed_chunks.sort(key=lambda x: x['importance'], reverse=True)
            
            if len(needed_chunks) == 1:
                selected = needed_chunks[0]
                logger.debug(f"[BT] Client {self.client_id}: Selected only candidate chunk {selected['chunk_key']} (importance: {selected['importance']:.4f}, rarity: {selected['availability']})")
                return selected['chunk_key']
            
            # 2. Find chunk with highest importance
            highest_importance = needed_chunks[0]['importance']
            
            # 3. Find all chunks with similar high importance
            similar_importance_chunks = []
            for chunk in needed_chunks:
                importance_diff = abs(chunk['importance'] - highest_importance)
                if importance_diff <= IMPORTANCE_SIMILARITY_THRESHOLD:
                    similar_importance_chunks.append(chunk)
                else:
                    break  # Due to sorting, subsequent chunks have lower importance
            
            logger.debug(f"[BT] Client {self.client_id}: Found {len(similar_importance_chunks)} chunks with similar high importance (threshold: {IMPORTANCE_SIMILARITY_THRESHOLD})")
            
            # 4. Select by rarity among chunks with similar importance
            if len(similar_importance_chunks) == 1:
                selected = similar_importance_chunks[0]
                logger.info(f"[BT] Client {self.client_id}: Selected chunk {selected['chunk_key']} by importance priority (importance: {selected['importance']:.4f}, rarity: {selected['availability']})")
                return selected['chunk_key']
            else:
                # Sort by rarity (fewer peers owning = more rare)
                similar_importance_chunks.sort(key=lambda x: (x['availability'], random.random()))
                selected = similar_importance_chunks[0]
                logger.info(f"[BT] Client {self.client_id}: Selected chunk {selected['chunk_key']} by rarity among high-importance chunks (importance: {selected['importance']:.4f}, rarity: {selected['availability']})")
                return selected['chunk_key']
        
        return None
    
    def _transfer_from_queue_to_active(self):
        """Transfer requests from pending queue to active pool"""
        while (len(self.pending_requests) < self.MAX_ACTIVE_REQUESTS and 
               len(self.pending_queue) > 0):
            
            # Take chunk from queue head (already sorted by importance)
            chunk_key = self.pending_queue.pop(0)
            
            # Check if chunk is still needed
            my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
            if chunk_key in my_bitfield or chunk_key in self.pending_requests:
                continue  # Skip chunks already owned or being requested
            
            # Find peer that has this chunk
            peer_id = self._find_peer_with_chunk(chunk_key)
            if peer_id and peer_id not in self.choked_peers:
                round_num, source_id, chunk_id = chunk_key
                try:
                    success = self._send_request(peer_id, source_id, chunk_id)
                    if success:
                        logger.debug(f"[BT-POOL] Client {self.client_id}: Transferred chunk {chunk_key} from queue to active pool")
                        break  # Successfully transferred one request
                    else:
                        logger.debug(f"[BT-POOL] Client {self.client_id}: Failed to transfer chunk {chunk_key} to active pool")
                except Exception as e:
                    logger.error(f"[BT-POOL] Client {self.client_id}: Exception while sending request for chunk {chunk_key}: {e}")
                    # Continue to next chunk in queue
            else:
                logger.debug(f"[BT-POOL] Client {self.client_id}: No available peer for chunk {chunk_key}")
    
    def _fill_pending_queue(self):
        """Fill pending queue (only called when queue is empty)"""
        if len(self.pending_queue) > 0:
            return  # Queue not empty, no need to fill
        
        logger.debug(f"[BT-POOL] Client {self.client_id}: Filling pending queue...")
        
        # Get all needed chunks and sort by importance
        needed_chunks = []
        
        # Count rarity of each chunk
        chunk_availability = {}
        # 🔧 FIX: Create thread-safe copy to avoid "dictionary changed size during iteration"
        peer_bitfields_copy = dict(self.peer_bitfields)  # Shallow copy for thread safety
        for peer_id, bitfield in peer_bitfields_copy.items():
            # 🔧 FIX: Also make bitfield copy in case it's modified during iteration
            bitfield_copy = dict(bitfield) if isinstance(bitfield, dict) else bitfield
            for chunk_key, has_chunk in bitfield_copy.items():
                if has_chunk and chunk_key[0] == self.round_num:
                    chunk_availability[chunk_key] = chunk_availability.get(chunk_key, 0) + 1
        
        my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
        
        # Select needed chunks
        for chunk_key, availability_count in chunk_availability.items():
            if (chunk_key not in my_bitfield and 
                chunk_key not in self.pending_requests and
                chunk_key not in self.pending_queue):
                
                importance_score = self._get_chunk_importance_score(chunk_key)
                needed_chunks.append({
                    'chunk_key': chunk_key,
                    'availability': availability_count,
                    'importance': importance_score
                })
        
        if needed_chunks:
            # Sort by importance, high importance first
            needed_chunks.sort(key=lambda x: x['importance'], reverse=True)
            
            # Fill queue, maximum to MAX_PENDING_QUEUE size
            for i, chunk in enumerate(needed_chunks[:self.MAX_PENDING_QUEUE]):
                self.pending_queue.append(chunk['chunk_key'])
            
            logger.info(f"[BT-POOL] Client {self.client_id}: Filled pending queue with {len(self.pending_queue)} chunks (from {len(needed_chunks)} candidates)")
            
            # Output details of first few high importance chunks
            for i, chunk in enumerate(needed_chunks[:3]):
                logger.debug(f"[BT-POOL] Client {self.client_id}: Queue #{i+1}: {chunk['chunk_key']} (importance: {chunk['importance']:.4f}, rarity: {chunk['availability']})")
        else:
            logger.debug(f"[BT-POOL] Client {self.client_id}: No chunks available to fill queue")
    
    def _get_chunk_importance_score(self, chunk_key: Tuple[int, int, int]) -> float:
        """Get chunk importance score"""
        round_num, source_client_id, chunk_id = chunk_key
        
        # 🆕 Get from all known importance scores
        # 1. First check if it's own chunk
        if source_client_id == self.client_id:
            chunk_importance_scores = self.chunk_manager.get_chunk_importance_scores(round_num)
            if chunk_id in chunk_importance_scores:
                chunk_data = chunk_importance_scores[chunk_id]
                return chunk_data.get('importance_score', 0.0)
        
        # 2. Get importance score from peer's bitfield
        if hasattr(self, 'peer_importance_scores'):
            # 🔧 FIX: Create thread-safe copy to avoid "dictionary changed size during iteration"
            peer_importance_scores_copy = dict(self.peer_importance_scores)
            for peer_id, peer_scores in peer_importance_scores_copy.items():
                if chunk_key in peer_scores:
                    return peer_scores[chunk_key]
        
        # 3. Return 0.0 by default
        return 0.0
    
    def _rarest_first_selection(self) -> Optional[Tuple]:
        """Rarest First chunk selection algorithm (compatibility alias)"""
        return self._importance_guided_selection()
        
    def _regular_unchoke_algorithm(self):
        """Classic Reciprocal Unchoke algorithm (with deadlock prevention improvements)"""
        # 🔧 Bug fix 6: dynamically adjust upload slots
        # Star topology center node needs more slots
        if self._is_central_node():
            self.MAX_UPLOAD_SLOTS = 8
        
        # Sort interested peers by download rate
        interested_peers = list(self.interested_by)
        interested_peers.sort(key=lambda p: self.download_rate.get(p, 0), reverse=True)
        
        # Select top N peers for regular unchoke (reserve 1 for optimistic)
        regular_slots = self.MAX_UPLOAD_SLOTS - 1
        new_unchoked = set(interested_peers[:regular_slots])
        
        # 🔧 Bug fix 7: fairness guarantee - ensure each peer is unchoked at least once
        for peer_id in self.interested_by:
            if peer_id not in self.ever_unchoked and len(new_unchoked) < self.MAX_UPLOAD_SLOTS:
                new_unchoked.add(peer_id)
                self.ever_unchoked.add(peer_id)
                logger.debug(f"[BT] Fairness unchoke for peer {peer_id}")
        
        # 🔧 Bug fix 8: ensure at least MIN_UNCHOKE_SLOTS unchoking
        if len(new_unchoked) == 0 and len(self.interested_by) > 0:
            # Randomly select a peer for unchoke to prevent complete deadlock
            emergency_peer = random.choice(list(self.interested_by))
            new_unchoked.add(emergency_peer)
            logger.warning(f"[BT] Emergency unchoke for peer {emergency_peer}")
        
        # Update choke/unchoke status
        for peer_id in self.unchoked_peers - new_unchoked:
            self._send_choke(peer_id)
        for peer_id in new_unchoked - self.unchoked_peers:
            self._send_unchoke(peer_id)
            
        self.unchoked_peers = new_unchoked
        
    def _optimistic_unchoke(self):
        """Optimistic unchoke mechanism (key to preventing deadlock)"""
        # Randomly select from choked interested peers
        choked_interested = self.interested_by - self.unchoked_peers
        if choked_interested:
            # 🔧 Bug fix 9: prioritize peers never unchoked before
            never_unchoked = choked_interested - self.ever_unchoked
            if never_unchoked:
                self.optimistic_unchoke_peer = random.choice(list(never_unchoked))
            else:
                self.optimistic_unchoke_peer = random.choice(list(choked_interested))
            
            self._send_unchoke(self.optimistic_unchoke_peer)
            self.unchoked_peers.add(self.optimistic_unchoke_peer)
            self.ever_unchoked.add(self.optimistic_unchoke_peer)
            logger.info(f"[BT] Optimistic unchoke for peer {self.optimistic_unchoke_peer}")
            
    def _is_central_node(self) -> bool:
        """🐛 Bug fix 27: determine if node is center of star topology"""
        # Simple judgment: if connected neighbor count exceeds half of total nodes, might be center node
        if len(self.neighbors) > 2:  # Assume 3+ connections indicate center node
            return True
        return False
        
    def _find_alternative_peers(self, chunk_key: Tuple, exclude: int = None) -> List[int]:
        """🐛 Bug fix 28: find alternative peers that have specified chunk"""
        alternatives = []
        # 🔧 FIX: Use thread-safe copy to avoid "dictionary changed size during iteration"
        peer_bitfields_copy = dict(self.peer_bitfields)
        for peer_id, bitfield in peer_bitfields_copy.items():
            if peer_id != exclude and chunk_key in bitfield and bitfield[chunk_key]:
                alternatives.append(peer_id)
        return alternatives
        
    def _find_peer_with_chunk(self, chunk_key: Tuple) -> Optional[int]:
        """Find peer that has specified chunk"""
        # 🔧 FIX: Use thread-safe copy to avoid "dictionary changed size during iteration"
        peer_bitfields_copy = dict(self.peer_bitfields)
        for peer_id, bitfield in peer_bitfields_copy.items():
            if chunk_key in bitfield and bitfield[chunk_key]:
                return peer_id
        return None
        
    def _send_bitfield(self, peer_id: int):
        """Send bitfield to specified peer (containing importance scores)"""
        from federatedscope.core.message import Message
        
        # 🔧 Fix: convert bitfield to serializable format
        my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
        logger.info(f"[BT] Client {self.client_id}: My bitfield for round {self.round_num}: {len(my_bitfield)} chunks")
        
        # 🆕 Get chunk importance scores for current round
        chunk_importance_scores = self.chunk_manager.get_chunk_importance_scores(self.round_num)
        logger.debug(f"[BT] Client {self.client_id}: Got {len(chunk_importance_scores)} importance scores for round {self.round_num}")
        
        # 🔧 Debug: detailed output of chunks I own
        if my_bitfield:
            logger.info(f"[BT] Client {self.client_id}: My chunks for round {self.round_num}:")
            for chunk_key, has_chunk in my_bitfield.items():
                if has_chunk:
                    round_num, source_id, chunk_id = chunk_key
                    # Silently record owned chunks
                    pass
        else:
            logger.warning(f"[BT] Client {self.client_id}: ⚠️ I have NO chunks for round {self.round_num}!")
        
        # Convert to list format, including importance scores
        bitfield_list = []
        for (round_num, source_id, chunk_id), has_chunk in my_bitfield.items():
            if has_chunk:
                # 🆕 Get chunk importance scores
                importance_score = 0.0
                if source_id == self.client_id and chunk_id in chunk_importance_scores:
                    # Own chunk, use locally saved importance score
                    chunk_data = chunk_importance_scores[chunk_id]
                    importance_score = chunk_data.get('importance_score', 0.0)
                    logger.debug(f"[BT] Client {self.client_id}: Using local importance {importance_score:.4f} for own chunk {chunk_id}")
                
                bitfield_list.append({
                    'round': round_num,
                    'source': source_id,
                    'chunk': chunk_id,
                    'importance_score': importance_score  # 🆕 Add importance score
                })
        
        logger.info(f"[BT] Client {self.client_id}: Sending {len(bitfield_list)} chunks in bitfield to peer {peer_id}")
        
        self.comm_manager.send(
            Message(msg_type='bitfield',
                   sender=self.client_id,
                   receiver=[peer_id],
                   state=self.round_num,
                   content={
                       'round_num': self.round_num,
                       'bitfield': bitfield_list
                   })
        )
        
    def _send_interested(self, peer_id: int):
        """Send interested message"""
        self.interested_in.add(peer_id)
        from federatedscope.core.message import Message
        self.comm_manager.send(
            Message(msg_type='interested',
                   sender=self.client_id,
                   receiver=[peer_id],
                   state=self.round_num,
                   content={})
        )
        
    def _send_unchoke(self, peer_id: int):
        """Send unchoke message"""
        from federatedscope.core.message import Message
        self.comm_manager.send(
            Message(msg_type='unchoke',
                   sender=self.client_id,
                   receiver=[peer_id],
                   state=self.round_num,
                   content={})
        )
        
    def _send_choke(self, peer_id: int):
        """Send choke message"""
        from federatedscope.core.message import Message
        self.comm_manager.send(
            Message(msg_type='choke',
                   sender=self.client_id,
                   receiver=[peer_id],
                   state=self.round_num,
                   content={})
        )
    
    def _broadcast_have(self, round_num: int, source_client_id: int, chunk_id: int):
        """Send have message to all neighbors (containing importance scores)"""
        # 🔴 have message contains round information
        from federatedscope.core.message import Message
        
        # 🆕 Get chunk importance scores
        importance_score = 0.0
        if source_client_id == self.client_id:
            # Own chunk, get importance score from database
            chunk_importance_scores = self.chunk_manager.get_chunk_importance_scores(round_num)
            if chunk_id in chunk_importance_scores:
                chunk_data = chunk_importance_scores[chunk_id]
                importance_score = chunk_data.get('importance_score', 0.0)
                logger.debug(f"[BT] Client {self.client_id}: Broadcasting have with importance {importance_score:.4f} for own chunk {chunk_id}")
        
        for neighbor_id in self.neighbors:
            self.comm_manager.send(
                Message(msg_type='have',
                       sender=self.client_id,
                       receiver=[neighbor_id],
                       state=round_num,
                       content={
                           'round_num': round_num,
                           'source_client_id': source_client_id,
                           'chunk_id': chunk_id,
                           'importance_score': importance_score  # 🆕 Add importance score
                       })
            )
                
    def check_timeouts(self):
        """🔧 Fix: non-blocking timeout check, called during message processing"""
        current_time = time.time()
        
        # Check once per second
        if current_time - self.last_timeout_check < 1.0:
            return
        
        self.last_timeout_check = current_time
        timeout_requests = []
        
        # Find timed out requests
        # 🔧 FIX: Create thread-safe copy to avoid "dictionary changed size during iteration"
        pending_requests_copy = dict(self.pending_requests)
        for chunk_key, (peer_id, timestamp) in pending_requests_copy.items():
            if current_time - timestamp > self.request_timeout:
                timeout_requests.append((chunk_key, peer_id))
        
        # Handle timed out requests
        # 🔧 FIX: Create thread-safe snapshot of retry_count to avoid concurrent modification
        retry_count_snapshot = dict(self.retry_count)
        
        for chunk_key, peer_id in timeout_requests:
            # 🔴 chunk_key now contains round information
            round_num, source_id, chunk_id = chunk_key
            retry_count = retry_count_snapshot.get(chunk_key, 0)
            
            if retry_count < self.max_retries:
                # Re-request
                logger.warning(f"[BT] Request timeout for chunk {chunk_key}, retrying ({retry_count+1}/{self.max_retries})")
                
                # Request from other peers
                alternative_peers = self._find_alternative_peers(chunk_key, exclude=peer_id)
                if alternative_peers:
                    new_peer = alternative_peers[0]
                    # 🔴 Pass correct parameters to _send_request
                    try:
                        self._send_request(new_peer, source_id, chunk_id)
                        self.pending_requests[chunk_key] = (new_peer, current_time)
                        # 🔧 FIX: Atomic retry count update - read current value and increment
                        self.retry_count[chunk_key] = self.retry_count.get(chunk_key, 0) + 1
                    except Exception as e:
                        logger.error(f"[BT] Client {self.client_id}: Failed to send request for chunk {chunk_key}: {e}")
                        # Remove from pending requests since send failed
                        if chunk_key in self.pending_requests:
                            del self.pending_requests[chunk_key]
                else:
                    logger.error(f"[BT] No alternative peers for chunk {chunk_key}")
                    # Safe deletion: check if key exists before deleting
                    if chunk_key in self.pending_requests:
                        del self.pending_requests[chunk_key]
            else:
                # Reached maximum retry count
                logger.error(f"[BT] Max retries reached for chunk {chunk_key}")
                # Safe deletion: check if key exists before deleting
                if chunk_key in self.pending_requests:
                    del self.pending_requests[chunk_key]
                if chunk_key in self.retry_count:
                    del self.retry_count[chunk_key]
                        
    def _send_request(self, peer_id: int, source_id: int, chunk_id: int):
        """Send chunk request (dual pool management system)"""
        # 🔴 chunk_key contains round information
        chunk_key = (self.round_num, source_id, chunk_id)
        
        # 🔧 CRITICAL FIX: Check for duplicate requests to prevent network flooding
        if chunk_key in self.pending_requests:
            existing_peer, existing_time = self.pending_requests[chunk_key]
            logger.debug(f"[BT-REQ] Client {self.client_id}: DUPLICATE REQUEST PREVENTED for chunk {source_id}:{chunk_id} - already pending from peer {existing_peer} for {time.time() - existing_time:.1f}s")
            return False
        
        # 🆕 Check if active pool is full
        if len(self.pending_requests) >= self.MAX_ACTIVE_REQUESTS:
            logger.debug(f"[BT-REQ] Client {self.client_id}: ACTIVE POOL FULL ({len(self.pending_requests)}/{self.MAX_ACTIVE_REQUESTS}), skipping request for chunk {source_id}:{chunk_id}")
            return False
        
        self.pending_requests[chunk_key] = (peer_id, time.time())
        
        logger.debug(f"[BT-REQ] Client {self.client_id}: Sending request to peer {peer_id} for chunk {source_id}:{chunk_id}")
        logger.debug(f"[BT-REQ] Client {self.client_id}: Active pool: {len(self.pending_requests)}/{self.MAX_ACTIVE_REQUESTS}, Queue: {len(self.pending_queue)}/{self.MAX_PENDING_QUEUE}")
        
        from federatedscope.core.message import Message
        self.comm_manager.send(
            Message(msg_type='request',
                   sender=self.client_id,
                   receiver=[peer_id],
                   state=self.round_num,
                   content={
                       'round_num': self.round_num,  # 🔴 Requested round
                       'source_client_id': source_id,
                       'chunk_id': chunk_id
                   })
        )
        return True
    
    def _send_piece(self, peer_id: int, round_num: int, source_client_id: int, chunk_id: int, chunk_data):
        """Send chunk data"""
        # 🔧 Fix: pre-serialize chunk_data and base64 encode to avoid data type changes during network transmission
        import pickle
        import base64
        serialized_data = pickle.dumps(chunk_data)
        encoded_data = base64.b64encode(serialized_data).decode('utf-8')
        checksum = hashlib.sha256(serialized_data).hexdigest()
        
        logger.debug(f"[BT-SEND] Client {self.client_id}: Serializing chunk {source_client_id}:{chunk_id}, original_type={type(chunk_data)}, serialized_size={len(serialized_data)}, encoded_size={len(encoded_data)}")
        
        # 🔴 Message contains round information
        from federatedscope.core.message import Message
        self.comm_manager.send(
            Message(msg_type='piece',
                   sender=self.client_id,
                   receiver=[peer_id],
                   state=round_num,
                   content={
                       'round_num': round_num,  # 🔴 Round chunk belongs to
                       'source_client_id': source_client_id,
                       'chunk_id': chunk_id,
                       'data': encoded_data,  # 🔧 Send base64 encoded string
                       'checksum': checksum
                   })
        )
        
        # Update upload statistics
        self.total_uploaded += len(serialized_data)
        
    def _has_interesting_chunks(self, peer_id: int) -> bool:
        """Check if peer has chunks I need"""
        if peer_id not in self.peer_bitfields:
            return False
            
        my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
        peer_bitfield = self.peer_bitfields[peer_id]
        
        # Check if peer has chunks I don't have
        # 🔧 FIX: Create thread-safe copy to avoid "dictionary changed size during iteration"
        peer_bitfield_copy = dict(peer_bitfield) if isinstance(peer_bitfield, dict) else peer_bitfield
        for chunk_key, has_chunk in peer_bitfield_copy.items():
            if has_chunk and chunk_key not in my_bitfield and chunk_key[0] == self.round_num:
                return True
        return False
        
    def _evaluate_unchoke(self, peer_id: int):
        """Evaluate whether to unchoke specified peer"""
        if len(self.unchoked_peers) < self.MAX_UPLOAD_SLOTS:
            self._send_unchoke(peer_id)
            self.unchoked_peers.add(peer_id)
            self.ever_unchoked.add(peer_id)
            
    def _schedule_regular_unchoke(self):
        """Schedule regular unchoke"""
        # In actual implementation, this should be called through timer or message loop
        self.last_unchoke_time = time.time()
        
    def _schedule_optimistic_unchoke(self):
        """Schedule optimistic unchoke"""
        # In actual implementation, this should be called through timer or message loop
        pass
        
    def _update_download_rate(self, peer_id: int, bytes_received: int):
        """Update download rate statistics"""
        current_time = time.time()
        if peer_id not in self.last_activity:
            self.last_activity[peer_id] = current_time
            self.download_rate[peer_id] = 0
            
        time_diff = current_time - self.last_activity[peer_id]
        if time_diff > 0:
            # Simple rate calculation
            rate = bytes_received / time_diff
            # Exponential moving average
            if peer_id in self.download_rate:
                self.download_rate[peer_id] = 0.8 * self.download_rate[peer_id] + 0.2 * rate
            else:
                self.download_rate[peer_id] = rate
                
    def get_progress(self) -> Dict[str, Any]:
        """Get exchange progress information"""
        my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
        total_expected = len(self.neighbors) * self.chunks_per_client + self.chunks_per_client  # Including own chunks
        
        return {
            'chunks_collected': len(my_bitfield),
            'total_expected': total_expected,
            'progress_ratio': len(my_bitfield) / total_expected if total_expected > 0 else 0,
            'active_peers': len(self.peer_bitfields),
            'pending_requests': len(self.pending_requests),
            'bytes_downloaded': self.total_downloaded,
            'bytes_uploaded': self.total_uploaded
        }
    
    def stop_exchange(self):
        """
        🔧 CRITICAL FIX: Stop BitTorrent exchange immediately
        This method is called when server timeout occurs or new round begins
        to prevent interference with next round BitTorrent operations
        """
        logger.info(f"[BT] Client {self.client_id}: Stopping BitTorrent exchange for round {self.round_num}")
        
        # Set stop flag
        self.is_stopped = True
        
        # Clear all pending operations
        self.pending_requests.clear()
        self.retry_count.clear()
        
        # Clear peer state
        self.peer_bitfields.clear()
        self.interested_in.clear()
        self.interested_by.clear()
        self.choked_peers.clear()
        self.unchoked_peers.clear()
        self.ever_unchoked.clear()
        self.last_activity.clear()
        
        # Clear rate tracking
        self.download_rate.clear()
        self.upload_rate.clear()
        
        logger.info(f"[BT] Client {self.client_id}: BitTorrent exchange stopped successfully")
        # Convert to MB for readable logging
        mb_downloaded = self.total_downloaded / (1024 * 1024)
        mb_uploaded = self.total_uploaded / (1024 * 1024)
        logger.info(f"[BT] Client {self.client_id}: Final stats - Downloaded: {mb_downloaded:.2f} MB, Uploaded: {mb_uploaded:.2f} MB")