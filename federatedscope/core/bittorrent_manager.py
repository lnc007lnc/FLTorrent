"""
BitTorrent Protocol Manager
Implements classic BitTorrent protocol for chunk exchange in FederatedScope
"""

import time
import hashlib
import random
import logging
import threading
import queue
from typing import Dict, Set, List, Tuple, Optional, Any
from collections import defaultdict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ChunkWriteQueue:
    """🚀 OPTIMIZATION 3: Single writer thread for SQLite operations with queue-based architecture"""
    
    def __init__(self, client_id: int, chunk_manager, streaming_manager=None, cfg=None, on_persisted_callback=None):
        self.client_id = client_id
        self.chunk_manager = chunk_manager
        self.streaming_manager = streaming_manager  # 🔧 Add streaming manager reference
        self.cfg = cfg  # 🚀 NEW: Configuration reference
        self.on_persisted_callback = on_persisted_callback  # 🚀 NEW: Callback for persistence completion
        
        # 🚀 FIX: Get queue capacity from configuration to prevent unlimited memory growth
        queue_size = 1000  # default value
        if cfg and hasattr(cfg, 'bittorrent'):
            queue_size = cfg.bittorrent.write_queue_size
            
        self.write_queue = queue.Queue(maxsize=queue_size)
        self.writer_thread = None
        self.is_running = False
        
        
    def start_writer_thread(self):
        """Start the single writer thread"""
        if self.is_running:
            return
            
        self.is_running = True
        self.writer_thread = threading.Thread(
            target=self._writer_loop,
            daemon=True,
            name=f"ChunkWriter-{self.client_id}"
        )
        self.writer_thread.start()
        logger.debug(f"[ChunkWriteQueue] Started single writer thread for client {self.client_id}")
        
    def stop_writer_thread(self, force_immediate=False, max_wait_time=30.0):
        """
        🚀 Graceful shutdown of write thread: wait for queue processing to complete, avoid data loss
        Args:
            force_immediate: whether to force immediate shutdown (emergency case)
            max_wait_time: maximum wait time (seconds)
        """
        if not self.is_running:
            return
            
        queue_size = self.write_queue.qsize()
        if queue_size > 0 and not force_immediate:
            logger.debug(f"[ChunkWriteQueue] Graceful shutdown: waiting for {queue_size} chunks to complete writing...")
        
        self.is_running = False
        
        if force_immediate:
            # Emergency shutdown: stop immediately
            self.write_queue.put(None)  # send shutdown signal
            if self.writer_thread and self.writer_thread.is_alive():
                self.writer_thread.join(timeout=2.0)
            logger.warning(f"[ChunkWriteQueue] Force close write thread, may lose {queue_size} chunks")
        else:
            # Graceful shutdown: wait for queue processing to complete
            self.write_queue.put(None)  # send shutdown signal
            
            if self.writer_thread and self.writer_thread.is_alive():
                logger.debug(f"[ChunkWriteQueue] Waiting for background writing to complete (max {max_wait_time} seconds)...")
                self.writer_thread.join(timeout=max_wait_time)
                
                if self.writer_thread.is_alive():
                    logger.error(f"[ChunkWriteQueue] Write thread did not complete within {max_wait_time} seconds, forcing termination")
                else:
                    logger.debug(f"[ChunkWriteQueue] Background write thread graceful shutdown completed")
            
        logger.debug(f"[ChunkWriteQueue] Write thread stopped for client {self.client_id}")
        
    def enqueue_chunk_write(self, round_num: int, source_client_id: int, chunk_id: int, 
                          chunk_data: bytes, checksum: str, timestamp: float):
        """Enqueue chunk write operation"""
        if not self.is_running:
            logger.warning(f"[ChunkWriteQueue] Writer thread not running, starting...")
            self.start_writer_thread()
            
        write_task = {
            'type': 'chunk_write',
            'round_num': round_num,
            'source_client_id': source_client_id,
            'chunk_id': chunk_id,
            'chunk_data': chunk_data,
            'checksum': checksum,
            'timestamp': timestamp
        }
        
        try:
            self.write_queue.put(write_task, timeout=1.0)
            logger.debug(f"[ChunkWriteQueue] Enqueued chunk write: {source_client_id}:{chunk_id}")
        except queue.Full:
            logger.error(f"[ChunkWriteQueue] Write queue full, dropping chunk {source_client_id}:{chunk_id}")
    
    def search_pending_chunk(self, round_num: int, source_client_id: int, chunk_id: int):
        """
        🚀 NEW: Search for pending chunk data in write queue
        Args:
            round_num: round number
            source_client_id: source client ID
            chunk_id: chunk ID
        Returns:
            chunk_data: if found return deserialized chunk data, otherwise return None
        """
        if not self.is_running or self.write_queue.empty():
            return None
            
        # Create queue copy for safe traversal (avoid concurrent modifications)
        import copy
        try:
            # Get queue snapshot (thread-safe)
            queue_snapshot = []
            temp_queue = queue.Queue()
            
            # Transfer tasks from queue to temporary queue while copying to snapshot
            while not self.write_queue.empty():
                try:
                    task = self.write_queue.get_nowait()
                    queue_snapshot.append(copy.deepcopy(task))
                    temp_queue.put(task)
                except queue.Empty:
                    break
            
            # Put tasks back to original queue
            while not temp_queue.empty():
                try:
                    task = temp_queue.get_nowait()
                    self.write_queue.put(task)
                except queue.Empty:
                    break
            
            # Search for matching chunk in snapshot
            for task in queue_snapshot:
                if (task.get('type') == 'chunk_write' and
                    task.get('round_num') == round_num and
                    task.get('source_client_id') == source_client_id and
                    task.get('chunk_id') == chunk_id):
                    
                    # Found matching chunk, deserialize chunk_data
                    import pickle
                    try:
                        chunk_data = pickle.loads(task['chunk_data'])
                        logger.debug(f"[ChunkWriteQueue] 🎯 Found chunk in write queue: {source_client_id}:{chunk_id}")
                        return chunk_data
                    except Exception as e:
                        logger.error(f"[ChunkWriteQueue] Failed to deserialize pending chunk: {e}")
                        return None
            
            return None
            
        except Exception as e:
            logger.error(f"[ChunkWriteQueue] Error searching pending chunk: {e}")
            return None
            
    def _writer_loop(self):
        """Background writer thread loop - single SQLite writer with graceful shutdown"""
        logger.debug(f"[ChunkWriteQueue] Writer thread started for client {self.client_id}")
        processed_count = 0
        
        while self.is_running or not self.write_queue.empty():  # 🚀 Continue processing until queue is empty
            try:
                # Block until task available or timeout
                task = self.write_queue.get(timeout=1.0)
                
                if task is None:  # Sentinel for shutdown
                    logger.debug(f"[ChunkWriteQueue] Received shutdown signal, remaining queue: {self.write_queue.qsize()}")
                    # Do not break immediately, continue processing remaining tasks
                    if self.write_queue.empty():
                        break
                    else:
                        continue
                    
                if task['type'] == 'chunk_write':
                    success = self._process_chunk_write(task)
                    if success:
                        processed_count += 1
                        if processed_count % 50 == 0:  # Record progress every 50 chunks processed
                            logger.debug(f"[ChunkWriteQueue] Processed {processed_count} chunks, remaining queue: {self.write_queue.qsize()}")
                    
                self.write_queue.task_done()
                
            except queue.Empty:
                # Timeout: if stopped running and queue is empty, exit
                if not self.is_running and self.write_queue.empty():
                    break
                continue
            except Exception as e:
                logger.error(f"[ChunkWriteQueue] Writer thread error: {e}")
                self.write_queue.task_done()  # Mark task complete to avoid deadlock
                time.sleep(0.1)  # Brief pause on error
                
        logger.debug(f"[ChunkWriteQueue] Writer thread gracefully exited, processed {processed_count} chunks for client {self.client_id}")
        
    def _process_chunk_write(self, task: Dict):
        """Process chunk write task with heavy validation in background"""
        try:
            round_num = task['round_num']
            source_client_id = task['source_client_id']
            chunk_id = task['chunk_id']
            chunk_data = task['chunk_data']
            checksum = task['checksum']
            
            # 🚀 Heavy validation in background thread
            calculated_checksum = hashlib.sha256(chunk_data).hexdigest()
            
            if calculated_checksum != checksum:
                logger.error(f"[ChunkWriteQueue] Background checksum validation failed for chunk {source_client_id}:{chunk_id}")
                return False
                
            # Deserialize in background
            import pickle
            try:
                deserialized_data = pickle.loads(chunk_data)
            except Exception as e:
                logger.error(f"[ChunkWriteQueue] Background deserialization failed: {e}")
                return False
                
            # 🚀 OPTIMIZED: Save to cache with immediate HAVE broadcast, DB write continues async
            self.chunk_manager.save_remote_chunk(
                round_num, source_client_id, chunk_id, deserialized_data, 
                on_cache_saved_callback=self.on_persisted_callback
            )
            
            # 🔧 CRITICAL FIX: After chunk is successfully saved, notify streaming manager to update status
            if self.streaming_manager and hasattr(self.streaming_manager, 'channels'):
                chunk_key = (round_num, source_client_id, chunk_id)
                # Notify all streaming channels that this chunk is completed
                for channel in self.streaming_manager.channels.values():
                    if hasattr(channel, 'mark_chunk_completed'):
                        channel.mark_chunk_completed(round_num, source_client_id, chunk_id)
                logger.debug(f"[ChunkWriteQueue] 🔧 Notified streaming channels that chunk {source_client_id}:{chunk_id} is completed")
            
            logger.debug(f"[ChunkWriteQueue] Background write completed for chunk {source_client_id}:{chunk_id}")
            return True
            
        except Exception as e:
            logger.error(f"[ChunkWriteQueue] Failed to process chunk write: {e}")
            return False


class BitTorrentManager:
    """Manage BitTorrent protocol core logic (including critical bug fixes)"""
    
    def __init__(self, client_id: int, round_num: int, chunk_manager, comm_manager, neighbors: List[int], cfg=None, streaming_manager=None):
        self.client_id = client_id
        self.round_num = round_num  # 🔴 Critical: current round
        self.chunk_manager = chunk_manager
        self.comm_manager = comm_manager
        self.neighbors = neighbors  # 🔧 Fix: directly pass neighbor list
        self.cfg = cfg  # 🆕 Configuration object
        
        # 🚀 STREAMING OPTIMIZATION: gRPC streaming channel manager
        self.streaming_manager = streaming_manager
        self.use_streaming = streaming_manager is not None
        if self.use_streaming:
            logger.debug(f"[BT] Client {client_id}: BitTorrent will use gRPC streaming channels")
        else:
            logger.debug(f"[BT] Client {client_id}: BitTorrent will use traditional message passing")
        
        # 🚀 OPTIMIZATION 3: Initialize single writer thread for SQLite operations
        self.chunk_write_queue = ChunkWriteQueue(client_id, chunk_manager, streaming_manager, cfg, self._broadcast_have)
        
        # 🚀 NEW: Allow ChunkManager to access ChunkWriteQueue
        chunk_manager.set_chunk_write_queue(self.chunk_write_queue)
        
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
        self.request_timeout = 5  # 5 second request timeout
        
        # ✅ Round-level lifecycle management
        self.is_download_complete = False  # Mark whether current node has completed download, used for seeding mode
        self.max_retries = 3  # Maximum retry count
        self.retry_count: Dict[Tuple, int] = {}  # {(source_id, chunk_id): count}
        
        # 🚀 OPTIMIZATION: Cache importance scores to avoid repeated database queries
        self._importance_cache: Dict[int, Dict[int, float]] = {}  # {round_num: {chunk_id: importance_score}}
        self._cache_initialized: Set[int] = set()  # Track which rounds have been cached
        
        # 🆕 Dual pool request management system - solves priority inversion and duplicate selection issues
        # Use configuration values if available, otherwise fall back to defaults
        if cfg and hasattr(cfg, 'bittorrent'):
            self.MAX_ACTIVE_REQUESTS = cfg.bittorrent.max_active_requests
            self.MAX_PENDING_QUEUE = cfg.bittorrent.max_pending_queue
            self.MAX_UPLOAD_SLOTS = cfg.bittorrent.max_upload_slots
        else:
            # Fallback defaults
            self.MAX_ACTIVE_REQUESTS = 10  # Active request pool size: actual concurrent request count sent
            self.MAX_PENDING_QUEUE = 20   # Pending queue pool size: pre-selected chunk queue size
            self.MAX_UPLOAD_SLOTS = 4     # Default upload slots
        
        self.pending_queue: List[Tuple] = []  # Pending queue: chunk list sorted by importance
        
        # 🔧 Bug fix 4: ensure minimum unchoke count
        self.MIN_UNCHOKE_SLOTS = 1  # Keep at least 1 unchoke to prevent complete deadlock
        
        # 🔧 Fix: no background threads, check timeouts through message callbacks
        self.last_timeout_check = time.time()
        
        # Statistics
        self.total_downloaded = 0
        self.total_uploaded = 0
        self.chunks_per_client = 10  # Default value, configurable
        
        # 🔧 CRITICAL FIX: Exchange state management
        self.is_stopped = False  # Stop flag for exchange termination
        
        # 🚀 ENDGAME OPTIMIZATION: Multi-peer parallel requests for final chunks
        self.min_completion_ratio = cfg.bittorrent.min_completion_ratio if (cfg and hasattr(cfg, 'bittorrent') and hasattr(cfg.bittorrent, 'min_completion_ratio')) else 0.8
        self.is_endgame_mode = False  # Endgame mode flag
        self.endgame_requests: Dict[Tuple, List[int]] = {}  # {chunk_key: [peer_id_list]} - Track multiple requests
        self.endgame_max_parallel_peers = 2  # Maximum parallel peers to request from in endgame
        self.endgame_start_time = None  # When endgame mode started
        
        logger.debug(f"[BT] BitTorrentManager initialized for client {client_id}, round {round_num}")
        logger.debug(f"[BT] Client {client_id}: Concurrent settings - Active requests: {self.MAX_ACTIVE_REQUESTS}, Pending queue: {self.MAX_PENDING_QUEUE}, Upload slots: {self.MAX_UPLOAD_SLOTS}")
        logger.debug(f"[BT] Client {client_id}: Endgame settings - Threshold: {self.min_completion_ratio:.1%}, Max parallel peers: {self.endgame_max_parallel_peers}")
        
    def start_exchange(self):
        """Start BitTorrent chunk exchange process (no Tracker needed)"""
        logger.info(f"[BT] Client {self.client_id}: Starting BitTorrent exchange")
        logger.debug(f"[BT] Client {self.client_id}: Neighbors: {self.neighbors}")
        
        # 🚀 OPTIMIZATION 3: Start single writer thread
        self.chunk_write_queue.start_writer_thread()
        
        # 1. Send bitfield directly to all topology neighbors
        for neighbor_id in self.neighbors:
            logger.debug(f"[BT] Client {self.client_id}: Sending bitfield to neighbor {neighbor_id}")
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
            logger.debug(f"[BT] Client {self.client_id}: Peer {sender_id} has no interesting chunks")
            
    def handle_interested(self, sender_id: int):
        """Handle interested messages"""
        self.interested_by.add(sender_id)
        logger.debug(f"[BT] Client {self.client_id}: Peer {sender_id} is interested")
        # Decide whether to unchoke based on current upload slots
        self._evaluate_unchoke(sender_id)
        
    def handle_request(self, sender_id: int, round_num: int, source_client_id: int, chunk_id: int):
        """Handle chunk requests"""
        # 🔍 DEBUG LOG: Detailed request handling logs
        logger.debug(f"⚡ [BT-HANDLE] Client {self.client_id}: HANDLE_REQUEST called from peer {sender_id}")
        logger.debug(f"⚡ [BT-HANDLE] Client {self.client_id}: Request details - round={round_num}, source={source_client_id}, chunk={chunk_id}")
        
        # 🔧 CRITICAL FIX: Check if exchange is stopped
        if self.is_stopped:
            logger.debug(f"⚡ [BT-HANDLE] Client {self.client_id}: IGNORING request - exchange STOPPED")
            logger.debug(f"[BT] Client {self.client_id}: Ignoring request from peer {sender_id} - exchange stopped")
            return
        logger.debug(f"[BT-HANDLE] Client {self.client_id}: Handling request from {sender_id} for chunk {source_client_id}:{chunk_id}")
        
        # 🔴 Verify round matching
        if round_num != self.round_num:
            logger.debug(f"⚡ [BT-HANDLE] Client {self.client_id}: ROUND MISMATCH - Request round {round_num} vs BitTorrent round {self.round_num}")
            logger.warning(f"[BT-HANDLE] Client {self.client_id}: Round mismatch - Request round {round_num} vs BitTorrent round {self.round_num}")
            logger.warning(f"[BT-HANDLE] Client {self.client_id}: Skipping request due to round mismatch")
            return
            
        logger.debug(f"⚡ [BT-HANDLE] Client {self.client_id}: Round check PASSED - processing request")
            
        if sender_id not in self.choked_peers:
            logger.debug(f"[BT-HANDLE] Client {self.client_id}: Peer {sender_id} is not choked, processing request")
            # Send chunk data
            # 🔴 Add round_num parameter to get_chunk_data
            logger.debug(f"[BT-HANDLE] Client {self.client_id}: Querying chunk_data with params (round={round_num}, source_client={source_client_id}, chunk_id={chunk_id})")
            chunk_data = self.chunk_manager.get_chunk_data(round_num, source_client_id, chunk_id)
            
            # 🚀 GENTLE RETRY: If chunk not found, wait briefly for potential persistence completion
            if chunk_data is None:
                logger.debug(f"[BT-HANDLE] Client {self.client_id}: Chunk {source_client_id}:{chunk_id} not found, trying gentle retry...")
                time.sleep(0.05)  # 50ms gentle wait for potential write queue completion
                chunk_data = self.chunk_manager.get_chunk_data(round_num, source_client_id, chunk_id)
                if chunk_data is not None:
                    logger.debug(f"[BT-HANDLE] Client {self.client_id}: ✅ Chunk {source_client_id}:{chunk_id} found after gentle retry")
            
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
                logger.warning(f"[BT-HANDLE] Client {self.client_id}: Chunk {source_client_id}:{chunk_id} not found in database (round={round_num} Requested by {sender_id})")
                logger.warning(f"[BT-HANDLE] Client {self.client_id}: Database query returned: {chunk_data}")
        else:
            logger.debug(f"[BT-HANDLE] Client {self.client_id}: Peer {sender_id} is choked, ignoring request")
            
    def handle_piece(self, sender_id: int, round_num: int, source_client_id: int, chunk_id: int, chunk_data, checksum: str):
        """
        🔧 Optimization: Lightweight chunk reception callback - avoid duplicate statistics and processing
        """
        # 🔧 CRITICAL FIX: Check if exchange is stopped
        if self.is_stopped:
            logger.debug(f"[BT] Client {self.client_id}: Ignoring piece from peer {sender_id} - exchange stopped")
            return
        
        logger.debug(f"[BT-PIECE] Client {self.client_id}: Processing chunk {source_client_id}:{chunk_id} from peer {sender_id}")
        
        # 🔴 Verify if rounds match (quick check)
        if round_num != self.round_num:
            logger.warning(f"[BT-PIECE] Client {self.client_id}: Round mismatch - Piece round {round_num} vs BitTorrent round {self.round_num}")
            return False
        
        # 🔧 Anti-duplicate processing check
        chunk_key = (round_num, source_client_id, chunk_id)
        if not hasattr(self, 'processed_pieces'):
            self.processed_pieces = set()
        
        if chunk_key in self.processed_pieces:
            logger.debug(f"[BT-PIECE] Client {self.client_id}: 🚫 Duplicate piece blocked for chunk {source_client_id}:{chunk_id}")
            return True  # Already processed, return success directly
        
        # 🚀 Process ChunkData object, extract raw bytes
        from federatedscope.core.message import ChunkData
        if isinstance(chunk_data, ChunkData):
            raw_bytes = chunk_data.raw_bytes
            logger.debug(f"[BT-PIECE] Client {self.client_id}: ChunkData received, size: {len(raw_bytes)}B")
        elif isinstance(chunk_data, bytes):
            raw_bytes = chunk_data
            logger.debug(f"[BT-PIECE] Client {self.client_id}: Raw bytes received: {len(raw_bytes)}B")
        else:
            logger.error(f"[BT-PIECE] Client {self.client_id}: Unexpected data type: {type(chunk_data)}")
            return False
            
        # Quick size reasonableness check
        if len(raw_bytes) == 0 or len(raw_bytes) > 50 * 1024 * 1024:  # 0 bytes or >50MB suspicious
            logger.error(f"[BT-PIECE] Client {self.client_id}: Suspicious chunk size: {len(raw_bytes)} bytes")
            return False
        
        # 🔧 Mark as processed to prevent duplicates
        self.processed_pieces.add(chunk_key)
        
        # 🚀 FIX: Check queue capacity, wait if approaching full capacity
        queue_size = self.chunk_write_queue.write_queue.qsize()
        max_size = self.chunk_write_queue.write_queue.maxsize
        if queue_size >= max_size * 0.9:  # Start waiting when queue usage reaches 90%
            logger.warning(f"[BT] Write queue nearly full ({queue_size}/{max_size}), waiting for space...")
            while self.chunk_write_queue.write_queue.qsize() >= max_size * 0.95:  # Wait until below 95% before continuing
                time.sleep(0.01)  # Brief wait
        
        # 🚀 Background queue processing, count only once
        self.chunk_write_queue.enqueue_chunk_write(
            round_num=round_num,
            source_client_id=source_client_id,
            chunk_id=chunk_id,
            chunk_data=raw_bytes,
            checksum=checksum,
            timestamp=time.time()
        )
        
        # 🚀 Clear pending request status
        if chunk_key in self.pending_requests:
            del self.pending_requests[chunk_key]
            logger.debug(f"[BT-PIECE] Client {self.client_id}: Cleared pending request for chunk {chunk_key}")
        
        # 🔧 Unified download statistics (count only once when successfully processed)
        self.total_downloaded += len(raw_bytes)
        logger.debug(f"[BT-PIECE] Client {self.client_id}: ✅ Processed unique chunk {source_client_id}:{chunk_id} ({len(raw_bytes)}B)")
            
        # 🆕 Dual pool system: transfer requests from queue pool to active pool
        self._transfer_from_queue_to_active()
        
        # 🚀 REMOVED: Immediate have broadcast - now done after persistence via callback
        logger.debug(f"[BT-PIECE] Client {self.client_id}: Chunk {source_client_id}:{chunk_id} queued for background processing - HAVE will be broadcast after persistence")
        
        # Update download statistics (lightweight) - Note: do not duplicate count total_downloaded here
        self._update_download_rate(sender_id, len(chunk_data))
        self.last_activity[sender_id] = time.time()
        
        logger.debug(f"[BT] Client {self.client_id}: Queued chunk {source_client_id}:{chunk_id} from peer {sender_id} for background processing")
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
        
    
    def _transfer_from_queue_to_active(self):
        """🔧 Smart dual pool system: automatically replenish queue and transfer to active pool"""
        # 🆕 Monitor queue length, automatically replenish
        if len(self.pending_queue) == 0:
            self._fill_pending_queue()
        
        while (len(self.pending_requests) < self.MAX_ACTIVE_REQUESTS and 
               len(self.pending_queue) > 0):
            
            # Take chunk from queue head (already sorted by importance)
            chunk_key = self.pending_queue.pop(0)
            
            # Check if chunk is still needed
            my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
            if chunk_key in my_bitfield or chunk_key in self.pending_requests:
                continue  # Skip chunks already owned or being requested
            
            # Find all peers that have this chunk
            peer_ids = self._find_peer_with_chunk(chunk_key)
            if not peer_ids:
                logger.debug(f"[BT-POOL] Client {self.client_id}: No available peer for chunk {chunk_key}")
                continue
            
            # Filter out choked peers
            available_peers = [peer_id for peer_id in peer_ids if peer_id not in self.choked_peers]
            if not available_peers:
                logger.debug(f"[BT-POOL] Client {self.client_id}: All peers with chunk {chunk_key} are choked")
                continue
            
            # 🚀 ENDGAME MODE: Check if we should enter endgame mode
            round_num, source_id, chunk_id = chunk_key
            current_chunks = len(self.chunk_manager.get_global_bitfield(round_num))
            # Calculate total expected chunks (same logic as get_progress())
            expected_chunks = len(self.neighbors) * self.chunks_per_client + self.chunks_per_client
            completion_ratio = current_chunks / max(expected_chunks, 1)
            
            
            
            if completion_ratio >= self.min_completion_ratio:
                # 🎯 ENDGAME MODE: Send requests to multiple peers (up to 2)
                endgame_peer_count = min(2, len(available_peers))
                logger.debug(f"[BT-ENDGAME] Client {self.client_id}: Entering endgame mode (completion: {completion_ratio:.2f}) - sending to {endgame_peer_count} peers")
                
                for i in range(endgame_peer_count):
                    peer_id = available_peers[i]
                    try:
                        success = self._send_request(peer_id, source_id, chunk_id)
                        if success:
                            logger.debug(f"[BT-ENDGAME] Client {self.client_id}: Sent endgame request for chunk {chunk_key} to peer {peer_id}")
                        else:
                            logger.debug(f"[BT-ENDGAME] Client {self.client_id}: Failed to send endgame request for chunk {chunk_key} to peer {peer_id}")
                    except Exception as e:
                        logger.error(f"[BT-ENDGAME] Client {self.client_id}: Exception while sending endgame request to peer {peer_id}: {e}")
                break  # Successfully processed one request (endgame mode)
            else:
                # 🎯 NORMAL MODE: Send request to first available peer
                peer_id = available_peers[0]
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
        for bitfield in peer_bitfields_copy.values():
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
                
                # 🚀 NEW: Calculate rarity score (scarcity)
                # smaller availability_count = more rare = higher rarity score
                total_peers = len(self.peer_bitfields) if self.peer_bitfields else 1
                rarity_score = 1.0 - (availability_count / max(total_peers, 1))
                
                needed_chunks.append({
                    'chunk_key': chunk_key,
                    'availability': availability_count,
                    'importance': importance_score,
                    'rarity': rarity_score  # 🚀 NEW: Add rarity score
                })
        
        if needed_chunks:
            # 🚀 NEW: Multi-factor sorting algorithm - combining importance, rarity and random perturbation
            import random
            
            # Get hyperparameters from configuration, use default values if not configured
            if self.cfg and hasattr(self.cfg, 'bittorrent'):
                tau = self.cfg.bittorrent.rarity_weight      # rarity weight
                eps = self.cfg.bittorrent.rarity_adjustment  # rarity adjustment parameter  
                gamma = self.cfg.bittorrent.random_noise     # random perturbation strength
            else:
                # Default values
                tau = 0.01      # rarity weight
                eps = 1e-6      # rarity adjustment parameter  
                gamma = 1e-4    # random perturbation strength
            
            needed_chunks.sort(
                key=lambda x: (
                    x['importance'] + (tau - eps) * x['rarity'] + gamma * random.uniform(-1, 1)
                ),
                reverse=True
            )
            
            # Fill queue, maximum to MAX_PENDING_QUEUE size
            for i, chunk in enumerate(needed_chunks[:self.MAX_PENDING_QUEUE]):
                self.pending_queue.append(chunk['chunk_key'])
            
            logger.debug(f"[BT-POOL] Client {self.client_id}: Filled pending queue with {len(self.pending_queue)} chunks (from {len(needed_chunks)} candidates)")
            
            # Output details of first few high priority chunks
            for i, chunk in enumerate(needed_chunks[:3]):
                # Recalculate final score for display
                final_score = (
                    chunk['importance'] + (tau - eps) * chunk['rarity'] + 
                    gamma * 0.0  # No random disturbance for display
                )
                logger.debug(f"[BT-POOL] Client {self.client_id}: Queue #{i+1}: {chunk['chunk_key']} "
                           f"(final_score: {final_score:.6f}, importance: {chunk['importance']:.4f}, "
                           f"rarity: {chunk['rarity']:.4f}, availability: {chunk['availability']})")
        else:
            logger.debug(f"[BT-POOL] Client {self.client_id}: No chunks available to fill queue")
    
    def _get_cached_importance_scores(self, round_num: int) -> Dict[int, float]:
        """Get cached importance scores for own chunks in the given round"""
        if round_num not in self._cache_initialized:
            # Initialize cache for this round
            chunk_importance_scores = self.chunk_manager.get_chunk_importance_scores(round_num)
            self._importance_cache[round_num] = {
                chunk_id: chunk_data.get('importance_score', 0.0)
                for chunk_id, chunk_data in chunk_importance_scores.items()
            }
            self._cache_initialized.add(round_num)
            logger.debug(f"[BT] Client {self.client_id}: Cached importance scores for {len(chunk_importance_scores)} chunks in round {round_num}")
        
        return self._importance_cache.get(round_num, {})
    
    def clear_importance_cache(self, keep_rounds: Optional[Set[int]] = None):
        """Clear importance cache, optionally keeping specified rounds"""
        if keep_rounds is None:
            keep_rounds = {self.round_num}  # Keep current round by default
        
        rounds_to_remove = set(self._importance_cache.keys()) - keep_rounds
        for round_num in rounds_to_remove:
            if round_num in self._importance_cache:
                del self._importance_cache[round_num]
            self._cache_initialized.discard(round_num)
        
        if rounds_to_remove:
            logger.debug(f"[BT] Client {self.client_id}: Cleared importance cache for rounds {rounds_to_remove}")
    
    def _get_chunk_importance_score(self, chunk_key: Tuple[int, int, int]) -> float:
        """Get chunk importance score with caching optimization"""
        round_num, source_client_id, chunk_id = chunk_key
        
        # 🚀 OPTIMIZATION: Use cached importance scores for own chunks
        if source_client_id == self.client_id:
            cached_scores = self._get_cached_importance_scores(round_num)
            return cached_scores.get(chunk_id, 0.0)
        
        # 2. Get importance score from peer's bitfield (for other clients' chunks)
        if hasattr(self, 'peer_importance_scores'):
            # 🔧 FIX: Create thread-safe copy to avoid "dictionary changed size during iteration"
            peer_importance_scores_copy = dict(self.peer_importance_scores)
            for peer_scores in peer_importance_scores_copy.values():
                if chunk_key in peer_scores:
                    return peer_scores[chunk_key]
        
        # 3. Return 0.0 by default
        return 0.0
    
        
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
            logger.debug(f"[BT] Optimistic unchoke for peer {self.optimistic_unchoke_peer}")
            
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
        
    def _find_peer_with_chunk(self, chunk_key: Tuple) -> List[int]:
        """Find all peers that have specified chunk, return list of peer IDs"""
        peers_with_chunk = []
        # 🔧 FIX: Use thread-safe copy to avoid "dictionary changed size during iteration"
        peer_bitfields_copy = dict(self.peer_bitfields)
        for peer_id, bitfield in peer_bitfields_copy.items():
            # 🔧 CRITICAL FIX: Exclude self, cannot send requests to self
            if peer_id == self.client_id:
                continue
            if chunk_key in bitfield and bitfield[chunk_key]:
                peers_with_chunk.append(peer_id)
        return peers_with_chunk
        
    def _send_bitfield(self, peer_id: int):
        """Send bitfield to specified peer (containing importance scores)"""
        from federatedscope.core.message import Message
        
        # 🔧 Fix: convert bitfield to serializable format
        my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
        logger.debug(f"[BT] Client {self.client_id}: My bitfield for round {self.round_num}: {len(my_bitfield)} chunks")
        
        # 🚀 OPTIMIZATION: Get cached importance scores for current round
        cached_importance_scores = self._get_cached_importance_scores(self.round_num)
        logger.debug(f"[BT] Client {self.client_id}: Using cached importance scores for {len(cached_importance_scores)} chunks in round {self.round_num}")
        
        # 🔧 Debug: detailed output of chunks I own
        if my_bitfield:
            logger.debug(f"[BT] Client {self.client_id}: My chunks for round {self.round_num}:")
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
                # 🚀 OPTIMIZATION: Use cached importance scores
                importance_score = 0.0
                if source_id == self.client_id:
                    # Own chunk, use cached importance score
                    importance_score = cached_importance_scores.get(chunk_id, 0.0)
                    logger.debug(f"[BT] Client {self.client_id}: Using cached importance {importance_score:.4f} for own chunk {chunk_id}")
                
                bitfield_list.append({
                    'round': round_num,
                    'source': source_id,
                    'chunk': chunk_id,
                    'importance_score': importance_score  # 🆕 Add importance score
                })
        
        logger.debug(f"[BT] Client {self.client_id}: Sending {len(bitfield_list)} chunks in bitfield to peer {peer_id}")
        
        # 🚀 STREAMING OPTIMIZATION: Prioritize using streaming channel to send bitfield
        if self.use_streaming and self.streaming_manager:
            logger.debug(f"[BT] Client {self.client_id}: Attempting STREAMING bitfield transmission to peer {peer_id}")
            success = self.streaming_manager.send_bittorrent_message(
                peer_id=peer_id,
                msg_type='bitfield',
                round_num=self.round_num,
                bitfield=bitfield_list
            )
            
            if success:
                logger.debug(f"[BT] Client {self.client_id}: STREAMING bitfield transmission SUCCESS to peer {peer_id}")
                return
            else:
                logger.debug(f"[BT] Client {self.client_id}: STREAMING bitfield transmission FAILED, using traditional fallback")
        
        # Traditional way to send bitfield (fallback when streaming channel fails)
        logger.debug(f"[BT] Client {self.client_id}: Using TRADITIONAL bitfield transmission to peer {peer_id}")
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
        """Send interested message - using streaming channel"""
        self.interested_in.add(peer_id)
        
        # 🚀 Use streaming channel to send INTERESTED message
        if self.use_streaming and self.streaming_manager:
            success = self.streaming_manager.send_bittorrent_message(
                peer_id=peer_id,
                msg_type='interested',
                round_num=self.round_num
            )
            
            if success:
                logger.debug(f"[BT] Client {self.client_id}: STREAMING INTERESTED message SUCCESS to peer {peer_id}")
            else:
                logger.error(f"[BT] Client {self.client_id}: STREAMING INTERESTED message FAILED to peer {peer_id}")
        else:
            logger.error(f"[BT] Client {self.client_id}: No streaming manager available for INTERESTED to peer {peer_id}")
        
    def _send_unchoke(self, peer_id: int):
        """Send unchoke message - using streaming channel"""
        # 🚀 Use streaming channel to send UNCHOKE message
        if self.use_streaming and self.streaming_manager:
            success = self.streaming_manager.send_bittorrent_message(
                peer_id=peer_id,
                msg_type='unchoke',
                round_num=self.round_num
            )
            
            if success:
                logger.debug(f"[BT] Client {self.client_id}: STREAMING UNCHOKE message SUCCESS to peer {peer_id}")
            else:
                logger.error(f"[BT] Client {self.client_id}: STREAMING UNCHOKE message FAILED to peer {peer_id}")
        else:
            logger.error(f"[BT] Client {self.client_id}: No streaming manager available for UNCHOKE to peer {peer_id}")
        
    def _send_choke(self, peer_id: int):
        """Send choke message - using streaming channel"""
        # 🚀 Use streaming channel to send CHOKE message
        if self.use_streaming and self.streaming_manager:
            success = self.streaming_manager.send_bittorrent_message(
                peer_id=peer_id,
                msg_type='choke',
                round_num=self.round_num
            )
            
            if success:
                logger.debug(f"[BT] Client {self.client_id}: STREAMING CHOKE message SUCCESS to peer {peer_id}")
            else:
                logger.debug(f"[BT] Client {self.client_id}: STREAMING CHOKE message FAILED to peer {peer_id}")
        else:
            logger.error(f"[BT] Client {self.client_id}: No streaming manager available for CHOKE to peer {peer_id}")
    
    def _broadcast_have(self, round_num: int, source_client_id: int, chunk_id: int):
        """Send have message to all neighbors (containing importance scores)"""
        # 🔴 have message contains round information
        from federatedscope.core.message import Message
        
        # 🚀 OPTIMIZATION: Get importance scores using cache
        importance_score = 0.0
        if source_client_id == self.client_id:
            # Own chunk, get importance score from cache
            cached_importance_scores = self._get_cached_importance_scores(round_num)
            importance_score = cached_importance_scores.get(chunk_id, 0.0)
            logger.debug(f"[BT] Client {self.client_id}: Broadcasting have with cached importance {importance_score:.4f} for own chunk {chunk_id}")
        
        for neighbor_id in self.neighbors:
            # 🚀 STREAMING OPTIMIZATION: Prioritize using streaming channel to send HAVE message
            sent_via_streaming = False
            if self.use_streaming and self.streaming_manager:
                success = self.streaming_manager.send_bittorrent_message(
                    peer_id=neighbor_id,
                    msg_type='have',
                    round_num=round_num,
                    source_client_id=source_client_id,
                    chunk_id=chunk_id,
                    importance_score=importance_score
                )
                
                if success:
                    sent_via_streaming = True
                else:
                    logger.debug(f"[BT] Client {self.client_id}: HAVE message STREAMING failed to peer {neighbor_id}, using fallback")
            
            # Traditional way to send HAVE message (fallback when streaming channel fails)
            if not sent_via_streaming:
                logger.debug(f"[BT] Client {self.client_id}: HAVE message sent via TRADITIONAL channel to peer {neighbor_id}")
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
                logger.debug(f"[BT] Request timeout for chunk {chunk_key}, retrying ({retry_count+1}/{self.max_retries}), last peer {peer_id}")
                
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
                    try:
                        self._send_request(peer_id, source_id, chunk_id)
                        self.pending_requests[chunk_key] = (peer_id, current_time)
                        # 🔧 FIX: Atomic retry count update - read current value and increment
                        self.retry_count[chunk_key] = self.retry_count.get(chunk_key, 0) + 1
                    except Exception as e:
                        logger.error(f"[BT] Client {self.client_id}: Failed to send request for chunk {chunk_key}: {e}")
                        # Remove from pending requests since send failed
                        if chunk_key in self.pending_requests:
                            del self.pending_requests[chunk_key]
                    # # Safe deletion: check if key exists before deleting
                    # if chunk_key in self.pending_requests:
                    #     logger.error(f"[BT] No alternative peers for chunk {chunk_key}")
                    #     del self.pending_requests[chunk_key]
            else:
                # Reached maximum retry count
                logger.error(f"[BT] Max retries reached for chunk {chunk_key}")
                # Safe deletion: check if key exists before deleting
                if chunk_key in self.pending_requests:
                    del self.pending_requests[chunk_key]
                if chunk_key in self.retry_count:
                    del self.retry_count[chunk_key]
                        
    def _send_request(self, peer_id: int, source_id: int, chunk_id: int):
        """🔧 Refactor: Only use Streaming layer to handle chunk requests - unified entry, avoid duplication"""
        chunk_key = (self.round_num, source_id, chunk_id)
        
        logger.debug(f"[BT-REQ] Client {self.client_id}: Requesting chunk {source_id}:{chunk_id} from peer {peer_id}")
        
        # 🚀 Only use Streaming layer to handle all chunk requests (deduplication handled by Streaming layer)
        if self.use_streaming and self.streaming_manager:
            logger.debug(f"[BT-REQ] Client {self.client_id}: Delegating to streaming layer for chunk {source_id}:{chunk_id}")
            
            # Delegate to Streaming manager for processing, including deduplication, batch optimization, etc.
            success = self.streaming_manager.send_chunk_request(
                peer_id=peer_id,
                round_num=self.round_num,
                source_client_id=source_id,
                chunk_id=chunk_id,
                importance_score=self._get_chunk_importance_score((self.round_num, source_id, chunk_id))
            )
            
            if success:
                # 🔧 Only record to pending_requests when streaming succeeds (for timeout management)
                self.pending_requests[chunk_key] = (peer_id, time.time())
                logger.debug(f"[BT-REQ] Client {self.client_id}: Request delegated to streaming for {source_id}:{chunk_id}")
                return True
            else:
                logger.warning(f"[BT-REQ] Client {self.client_id}: Streaming layer rejected request for {source_id}:{chunk_id}")
                return False
        else:
            logger.error(f"[BT-REQ] Client {self.client_id}: No streaming available, cannot request {source_id}:{chunk_id}")
            return False
    
    def _send_piece(self, peer_id: int, round_num: int, source_client_id: int, chunk_id: int, chunk_data):
        """🚀 Optimization 4: Smart sending strategy - multi-optimized chunk transmission"""
        import pickle
        
        # 🚀 Data preparation and preprocessing
        send_start_time = time.time()
        serialized_data = pickle.dumps(chunk_data)
        checksum = hashlib.sha256(serialized_data).hexdigest()
        data_size = len(serialized_data)
        
        logger.debug(f"[BT-SEND] Client {self.client_id}: Preparing chunk {source_client_id}:{chunk_id}, size={data_size}B")
        
        # 🚀 Smart transmission strategy selection
        use_streaming = self._should_use_streaming(peer_id, data_size)
        transmission_method = "STREAMING" if use_streaming else "TRADITIONAL"
        
        logger.debug(f"📤 [BT-PIECE-SEND] Client {self.client_id}: {transmission_method} transmission for chunk {source_client_id}:{chunk_id} to peer {peer_id}")
        logger.debug(f"📤 [BT-PIECE-SEND] Client {self.client_id}: Size={data_size}B, method={transmission_method}")
        
        # 🚀 Multiple fallback mechanism
        transmission_attempts = []
        final_success = False
        
        # 🚀 Attempt 1: High-performance streaming transmission
        if use_streaming and self.streaming_manager:
            streaming_start = time.time()
            logger.debug(f"📤 [BT-PIECE-SEND] Client {self.client_id}: Attempting streaming transmission to peer {peer_id}")
            
            streaming_success = self.streaming_manager.send_bittorrent_message(
                peer_id=peer_id,
                msg_type='piece',
                round_num=round_num,
                source_client_id=source_client_id,
                chunk_id=chunk_id,
                chunk_data=serialized_data,
                checksum=checksum,
                importance_score=self._get_chunk_importance_score((round_num, source_client_id, chunk_id))
            )
            
            streaming_time = time.time() - streaming_start
            transmission_attempts.append({
                'method': 'streaming',
                'success': streaming_success,
                'time': streaming_time
            })
            
            if streaming_success:
                self.total_uploaded += data_size
                self._update_streaming_success_stats(peer_id, data_size, streaming_time)
                final_success = True
                
                logger.debug(f"📤 [BT-PIECE-SEND] ✅ Client {self.client_id}: Streaming SUCCESS to peer {peer_id} in {streaming_time:.3f}s")
                logger.debug(f"[BT-SEND] Client {self.client_id}: Streaming throughput: {data_size/streaming_time:.0f}B/s")
                
            else:
                logger.debug(f"📤 [BT-PIECE-SEND] ❌ Client {self.client_id}: Streaming FAILED to peer {peer_id}, trying fallback")
                self._update_streaming_failure_stats(peer_id)
        
        # 🚀 Attempt 2: Traditional message transmission (intelligent fallback)
        if not final_success:
            traditional_start = time.time()
            logger.debug(f"📤 [BT-PIECE-SEND] Client {self.client_id}: Using traditional message transmission to peer {peer_id}")
            
            try:
                from federatedscope.core.message import Message, ChunkData
                chunk_wrapper = ChunkData(serialized_data, checksum)
                
                message = Message(
                    msg_type='piece',
                    sender=self.client_id,
                    receiver=[peer_id],
                    state=round_num,
                    content={
                        'round_num': round_num,
                        'source_client_id': source_client_id,
                        'chunk_id': chunk_id,
                        'data': chunk_wrapper,
                        'checksum': checksum
                    }
                )
                
                self.comm_manager.send(message)
                traditional_time = time.time() - traditional_start
                
                transmission_attempts.append({
                    'method': 'traditional',
                    'success': True,
                    'time': traditional_time
                })
                
                self.total_uploaded += data_size
                self._update_traditional_success_stats(peer_id, data_size, traditional_time)
                final_success = True
                
                logger.debug(f"📤 [BT-PIECE-SEND] ✅ Client {self.client_id}: Traditional SUCCESS to peer {peer_id} in {traditional_time:.3f}s")
                
            except Exception as e:
                traditional_time = time.time() - traditional_start
                transmission_attempts.append({
                    'method': 'traditional',
                    'success': False,
                    'time': traditional_time,
                    'error': str(e)
                })
                
                logger.error(f"📤 [BT-PIECE-SEND] ❌ Client {self.client_id}: Traditional transmission FAILED to peer {peer_id}: {e}")
        
        # 🚀 Performance analysis and adaptive optimization
        total_send_time = time.time() - send_start_time
        self._analyze_transmission_performance(peer_id, transmission_attempts, data_size, total_send_time)
        
        if not final_success:
            logger.error(f"📤 [BT-PIECE-SEND] ❌ Client {self.client_id}: ALL transmission methods FAILED for chunk {source_client_id}:{chunk_id} to peer {peer_id}")
        else:
            logger.debug(f"[BT-SEND] Client {self.client_id}: Chunk transmission completed in {total_send_time:.3f}s")
    
    def _should_use_streaming(self, peer_id: int, data_size: int) -> bool:
        """🚀 Intelligent transmission method selection"""
        if not self.use_streaming or not self.streaming_manager:
            return False
        
        # 🚀 Intelligent selection based on historical performance
        if not hasattr(self, 'peer_streaming_stats'):
            self.peer_streaming_stats = {}
        
        if peer_id not in self.peer_streaming_stats:
            # Initial connection, prefer streaming for large data
            return data_size > 1024  # Use streaming for data over 1KB
        
        stats = self.peer_streaming_stats[peer_id]
        streaming_success_rate = stats.get('streaming_success_rate', 0.0)
        traditional_avg_time = stats.get('traditional_avg_time', float('inf'))
        streaming_avg_time = stats.get('streaming_avg_time', float('inf'))
        
        # 🚀 Intelligent decision logic
        if streaming_success_rate < 0.5:  # Success rate below 50%
            return False
        elif streaming_avg_time < traditional_avg_time * 0.8:  # Streaming significantly faster
            return True
        elif data_size > 10240:  # Over 10KB, prefer streaming
            return True
        else:
            return streaming_success_rate > 0.8  # Use streaming when high success rate
    
    
    def _update_streaming_success_stats(self, peer_id: int, data_size: int, transmission_time: float):
        """🚀 Update streaming success statistics"""
        if not hasattr(self, 'peer_streaming_stats'):
            self.peer_streaming_stats = {}
        
        if peer_id not in self.peer_streaming_stats:
            self.peer_streaming_stats[peer_id] = {
                'streaming_attempts': 0,
                'streaming_successes': 0,
                'streaming_total_time': 0.0,
                'streaming_success_rate': 0.0,
                'streaming_avg_time': 0.0
            }
        
        stats = self.peer_streaming_stats[peer_id]
        stats['streaming_attempts'] += 1
        stats['streaming_successes'] += 1
        stats['streaming_total_time'] += transmission_time
        stats['streaming_success_rate'] = stats['streaming_successes'] / stats['streaming_attempts']
        stats['streaming_avg_time'] = stats['streaming_total_time'] / stats['streaming_successes']
    
    def _update_streaming_failure_stats(self, peer_id: int):
        """🚀 Update streaming failure statistics"""
        if not hasattr(self, 'peer_streaming_stats'):
            self.peer_streaming_stats = {}
        
        if peer_id not in self.peer_streaming_stats:
            self.peer_streaming_stats[peer_id] = {
                'streaming_attempts': 0,
                'streaming_successes': 0,
                'streaming_total_time': 0.0,
                'streaming_success_rate': 0.0,
                'streaming_avg_time': 0.0
            }
        
        stats = self.peer_streaming_stats[peer_id]
        stats['streaming_attempts'] += 1
        stats['streaming_success_rate'] = stats['streaming_successes'] / stats['streaming_attempts']
    
    def _update_traditional_success_stats(self, peer_id: int, data_size: int, transmission_time: float):
        """🚀 Update traditional success statistics"""
        if not hasattr(self, 'peer_streaming_stats'):
            self.peer_streaming_stats = {}
        
        if peer_id not in self.peer_streaming_stats:
            self.peer_streaming_stats[peer_id] = {}
        
        stats = self.peer_streaming_stats[peer_id]
        if 'traditional_attempts' not in stats:
            stats.update({
                'traditional_attempts': 0,
                'traditional_successes': 0,
                'traditional_total_time': 0.0,
                'traditional_avg_time': 0.0
            })
        
        stats['traditional_attempts'] += 1
        stats['traditional_successes'] += 1
        stats['traditional_total_time'] += transmission_time
        stats['traditional_avg_time'] = stats['traditional_total_time'] / stats['traditional_successes']
    
    def _analyze_transmission_performance(self, peer_id: int, attempts: list, data_size: int, total_time: float):
        """🚀 Performance analysis and adaptive optimization"""
        if not attempts:
            return
        
        # Record detailed transmission performance
        successful_attempts = [a for a in attempts if a['success']]
        if successful_attempts:
            best_attempt = min(successful_attempts, key=lambda x: x['time'])
            throughput = data_size / total_time if total_time > 0 else 0
            
            logger.debug(f"[BT-PERF] Client {self.client_id}: Best transmission to peer {peer_id}: "
                        f"{best_attempt['method']} in {best_attempt['time']:.3f}s, "
                        f"throughput: {throughput:.0f}B/s")
        
        # 🚀 Adaptive parameter adjustment (future extensible)
        # Adjust transmission strategy, batch processing size, etc. based on performance data
        
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
        """🚀 Performance optimization: Execute first unchoke immediately to speed up startup"""
        self.last_unchoke_time = time.time()
        # Execute first unchoke immediately, don't wait 10 seconds
        self._regular_unchoke_algorithm()
        logger.debug(f"[BT] Client {self.client_id}: Initial unchoke completed")
        
    def _schedule_optimistic_unchoke(self):
        """🚀 Performance optimization: Execute first optimistic unchoke immediately"""
        # Execute first optimistic unchoke immediately
        self._optimistic_unchoke()
        logger.debug(f"[BT] Client {self.client_id}: Initial optimistic unchoke completed")
        
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
        logger.debug(f"[BT] Client {self.client_id}: Stopping BitTorrent exchange for round {self.round_num}")
        
        # Set stop flag
        self.is_stopped = True
        
        # 🚀 OPTIMIZATION 3: Gracefully close write thread to ensure data integrity
        queue_size = self.chunk_write_queue.write_queue.qsize()
        if queue_size > 0:
            logger.debug(f"[BT] Client {self.client_id}: Detected {queue_size} pending chunks, waiting for background writes to complete...")
            # Normal case: wait for queue processing to complete (max 30 seconds)
            self.chunk_write_queue.stop_writer_thread(force_immediate=False, max_wait_time=30.0)
        else:
            # Queue is empty: can close immediately
            self.chunk_write_queue.stop_writer_thread(force_immediate=True)
        
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
        
        # 🔧 CRITICAL FIX: Clear processed_pieces to prevent memory leak
        if hasattr(self, 'processed_pieces'):
            pieces_count = len(self.processed_pieces)
            self.processed_pieces.clear()
            logger.debug(f"[BT] Client {self.client_id}: Cleared {pieces_count} processed_pieces")
        
        # 🔧 CRITICAL FIX: Clear streaming channel deduplication sets
        if self.streaming_manager:
            total_requested = 0
            total_completed = 0
            for channel in self.streaming_manager.channels.values():
                if hasattr(channel, 'requested_chunks'):
                    total_requested += len(channel.requested_chunks)
                    channel.requested_chunks.clear()
                if hasattr(channel, 'completed_chunks'):
                    total_completed += len(channel.completed_chunks)
                    channel.completed_chunks.clear()
            logger.debug(f"[BT] Client {self.client_id}: Cleared streaming deduplication - {total_requested} requested, {total_completed} completed chunks")
        
        logger.info(f"[BT] Client {self.client_id}: BitTorrent exchange stopped successfully")
        # Convert to MB for readable logging
        mb_downloaded = self.total_downloaded / (1024 * 1024)
        mb_uploaded = self.total_uploaded / (1024 * 1024)
        logger.info(f"[BT] Client {self.client_id}: Final stats - Downloaded: {mb_downloaded:.2f} MB, Uploaded: {mb_uploaded:.2f} MB")
        
    def emergency_stop(self):
        """🚨 Emergency stop: Immediately close all operations, for system abnormal exit"""
        logger.warning(f"[BT] Client {self.client_id}: Emergency stop triggered!")
        
        self.is_stopped = True
        
        # Emergency close write thread, may lose data
        queue_size = self.chunk_write_queue.write_queue.qsize()
        if queue_size > 0:
            logger.warning(f"[BT] Client {self.client_id}: Emergency shutdown, {queue_size} chunks may be lost")
        
        self.chunk_write_queue.stop_writer_thread(force_immediate=True)
        
        # Clean up all state
        self.pending_requests.clear()
        self.retry_count.clear()
        self.peer_bitfields.clear()
        
        # 🔧 Emergency cleanup: Clear deduplication sets
        if hasattr(self, 'processed_pieces'):
            self.processed_pieces.clear()
        
        if self.streaming_manager:
            for channel in self.streaming_manager.channels.values():
                if hasattr(channel, 'requested_chunks'):
                    channel.requested_chunks.clear()
                if hasattr(channel, 'completed_chunks'):
                    channel.completed_chunks.clear()
        
        logger.warning(f"[BT] Client {self.client_id}: Emergency stop completed")