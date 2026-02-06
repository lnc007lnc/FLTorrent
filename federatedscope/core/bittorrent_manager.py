"""
BitTorrent Protocol Manager
Implements classic BitTorrent protocol for chunk exchange in FederatedScope
"""

import gc
import time
import hashlib
import random
import logging
import threading
import queue
from typing import Dict, Set, List, Tuple, Optional, Any
from collections import defaultdict

# 🚀 FIX: Import at module level to avoid import lock contention in multi-threaded environment
# Dynamic imports inside functions can cause thread blocking due to Python's import lock
from federatedscope.core.message import Message, ChunkData

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

        # 🚀 DEADLOCK FIX: Thread safety for start/stop operations
        self._state_lock = threading.RLock()
        self._stopping = False


    def start_writer_thread(self):
        """Start the single writer thread (thread-safe)"""
        with self._state_lock:
            # 🚀 DEADLOCK FIX: Don't start if stopping in progress
            if self._stopping:
                logger.debug(f"[ChunkWriteQueue] Cannot start writer - stop in progress for client {self.client_id}")
                return
            # Check if already running and thread is alive
            if self.writer_thread and self.writer_thread.is_alive():
                self.is_running = True
                return
            if self.is_running:
                return

            self.is_running = True
            t = threading.Thread(
                target=self._writer_loop,
                daemon=True,
                name=f"ChunkWriter-{self.client_id}"
            )
            self.writer_thread = t
            t.start()
            logger.debug(f"[ChunkWriteQueue] Started single writer thread for client {self.client_id}")
        
    def stop_writer_thread(self, force_immediate=False, max_wait_time=30.0):
        """
        🚀 Graceful shutdown of write thread: wait for queue processing to complete, avoid data loss

        🚀 DEADLOCK FIX: Capture old_thread reference before any state changes to prevent
        race condition where callback thread overwrites self.writer_thread during join().

        Args:
            force_immediate: whether to force immediate shutdown (emergency case)
            max_wait_time: maximum wait time (seconds)
        """
        # 🚀 DEADLOCK FIX: Atomically set stopping state and capture thread reference
        with self._state_lock:
            if not self.is_running and not (self.writer_thread and self.writer_thread.is_alive()):
                return
            self._stopping = True
            self.is_running = False
            old_thread = self.writer_thread  # Capture reference BEFORE any callback can overwrite it
            queue_size = self.write_queue.qsize()

        if queue_size > 0 and not force_immediate:
            logger.debug(f"[ChunkWriteQueue] Graceful shutdown: waiting for {queue_size} chunks to complete writing...")

        # 🚀 DEADLOCK FIX: Use non-blocking put with timeout to avoid blocking on full queue
        deadline = time.time() + max_wait_time
        sentinel_sent = False
        while not sentinel_sent:
            try:
                self.write_queue.put_nowait(None)  # send shutdown signal (non-blocking)
                sentinel_sent = True
            except queue.Full:
                if time.time() >= deadline:
                    logger.warning(f"[ChunkWriteQueue] Timeout sending shutdown signal, queue still full")
                    break
                time.sleep(0.01)

        if force_immediate:
            # Emergency shutdown: stop immediately
            if old_thread and old_thread.is_alive():
                old_thread.join(timeout=2.0)
            logger.warning(f"[ChunkWriteQueue] Force close write thread, may lose {queue_size} chunks")
        else:
            # Graceful shutdown: wait for old_thread (NOT self.writer_thread which may be overwritten)
            if old_thread and old_thread.is_alive():
                start_time = time.time()
                if queue_size > 0:
                    logger.debug(f"[ChunkWriteQueue] Waiting for all {queue_size} chunks to be written (max {max_wait_time}s)...")
                else:
                    logger.debug(f"[ChunkWriteQueue] Waiting for write thread to complete...")
                # 🚀 DEADLOCK FIX: Use timeout instead of indefinite wait
                old_thread.join(timeout=max_wait_time)
                elapsed_time = time.time() - start_time
                if old_thread.is_alive():
                    logger.warning(f"[ChunkWriteQueue] Write thread still alive after {elapsed_time:.2f}s timeout")
                else:
                    logger.info(f"[ChunkWriteQueue] Background write thread completed in {elapsed_time:.2f} seconds, processed {queue_size} chunks")

        # 🚀 DEADLOCK FIX: Reset stopping state (must be done even if thread didn't fully stop)
        with self._state_lock:
            # Only clear writer_thread if it hasn't been replaced by a new thread
            if self.writer_thread is old_thread:
                self.writer_thread = None
            self._stopping = False

        logger.debug(f"[ChunkWriteQueue] Write thread stopped for client {self.client_id}")
        
    def enqueue_chunk_write(self, round_num: int, source_client_id: int, chunk_id: int,
                          chunk_data: bytes, checksum: str, timestamp: float) -> bool:
        """Enqueue chunk write operation - NON-BLOCKING

        🚀 DEADLOCK FIX: Callback threads must NEVER start/stop writer thread.
        If writer is not running or stopping, reject the enqueue - caller will retry via timeout.

        Returns:
            bool: True if successfully enqueued, False if not running/stopping/full
        """
        # 🚀 DEADLOCK FIX: Check state with lock, never auto-start from callback thread
        with self._state_lock:
            if self._stopping:
                logger.debug(f"[ChunkWriteQueue] Rejecting chunk {source_client_id}:{chunk_id} - writer stopping")
                return False
            if not self.is_running or not (self.writer_thread and self.writer_thread.is_alive()):
                logger.debug(f"[ChunkWriteQueue] Rejecting chunk {source_client_id}:{chunk_id} - writer not running")
                return False

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
            # 🚀 DEADLOCK FIX: Use put_nowait, never block in callback thread
            self.write_queue.put_nowait(write_task)
            logger.debug(f"[ChunkWriteQueue] Enqueued chunk write: {source_client_id}:{chunk_id}")
            return True
        except queue.Full:
            # Let caller know enqueue failed - they should NOT mark as processed
            # Timeout mechanism will trigger retry from peer
            # 🔧 Changed to debug: queue full is expected under high load, not an error
            logger.debug(f"[ChunkWriteQueue] Write queue FULL, reject chunk {source_client_id}:{chunk_id} for retry")
            return False
    
    def search_pending_chunk(self, round_num: int, source_client_id: int, chunk_id: int):
        """
        🚀 NEW: Search for pending chunk data in write queue

        🔧 MEMORY FIX: Removed deepcopy which caused 1.5GB+ temporary allocation per call!
        Each task contains ~3MB chunk_data, queue has 500 items = 1.5GB copied per search.
        Now we just iterate through items without copying, returning original reference.

        Args:
            round_num: round number
            source_client_id: source client ID
            chunk_id: chunk ID
        Returns:
            chunk_bytes: if found return bytes reference directly, otherwise return None
        """
        if not self.is_running or self.write_queue.empty():
            return None

        try:
            # 🔧 MEMORY FIX: Don't use deepcopy! Just iterate through queue items
            # Only store references, not copies of the 3MB chunk_data
            found_chunk_data = None
            items_to_restore = []

            # Transfer items out of queue, check each one, then restore
            while not self.write_queue.empty():
                try:
                    task = self.write_queue.get_nowait()
                    if task is None:
                        # Sentinel for shutdown, put it back
                        items_to_restore.append(task)
                        continue

                    items_to_restore.append(task)  # Store reference, NOT copy

                    # Check if this is the chunk we're looking for (NO deepcopy!)
                    if (found_chunk_data is None and  # Only match first occurrence
                        task.get('type') == 'chunk_write' and
                        task.get('round_num') == round_num and
                        task.get('source_client_id') == source_client_id and
                        task.get('chunk_id') == chunk_id):

                        # Found matching chunk - return reference to original bytes, no copy
                        found_chunk_data = task['chunk_data']
                        logger.debug(f"[ChunkWriteQueue] 🎯 Found chunk in write queue: {source_client_id}:{chunk_id}")

                except queue.Empty:
                    break

            # Restore all items back to queue in original order
            for task in items_to_restore:
                try:
                    self.write_queue.put_nowait(task)
                except queue.Full:
                    logger.error(f"[ChunkWriteQueue] Queue full during restore, item lost!")
                    break

            return found_chunk_data

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
                    # 🚀 DEADLOCK FIX: Must call task_done() for sentinel to prevent queue.join() deadlock
                    self.write_queue.task_done()
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
                
            # 🚀 OPTIMIZED: Save to cache with immediate HAVE broadcast, DB write continues async
            # Pass bytes directly to avoid redundant pickle.loads() → pickle.dumps()
            self.chunk_manager.save_remote_chunk(
                round_num, source_client_id, chunk_id, chunk_data,  # Pass bytes directly
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

        # Gossip-style DFL: neighbor-only collection mode
        self.neighbor_only = (cfg and hasattr(cfg, 'bittorrent') and
                              hasattr(cfg.bittorrent, 'neighbor_only_collection') and
                              cfg.bittorrent.neighbor_only_collection)
        if self.neighbor_only:
            logger.info(f"[BT] Client {client_id}: Neighbor-only collection mode enabled")
        
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

        # 🚀 CRITICAL FIX: Memory-based chunk counter to avoid DB queries for completion check
        # This prevents process freeze caused by SQLite lock contention
        self.chunks_received_set: Set[Tuple] = set()  # {(round_num, source_id, chunk_id)}
        self.own_chunks_set: Set[Tuple] = set()  # {(round_num, source_id, chunk_id)} for own chunks
        self.own_chunks_count = 0  # Will be set when own chunks are registered
        self.expected_total_chunks = 0  # Total chunks expected from all clients
        self.final_chunk_count = 0  # 🚀 Saved by stop_exchange() before clearing counters
        
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
            # 🔧 FIX: Restored reasonable defaults (was 50/100, caused excessive concurrent requests)
            self.MAX_ACTIVE_REQUESTS = 10  # Active request pool size
            self.MAX_PENDING_QUEUE = 20   # Pending queue pool size
            self.MAX_UPLOAD_SLOTS = 4     # Default upload slots
        
        self.pending_queue: List[Tuple] = []  # Pending queue: chunk list sorted by importance
        
        # 🔧 Bug fix 4: ensure minimum unchoke count
        self.MIN_UNCHOKE_SLOTS = 1  # Keep at least 1 unchoke to prevent complete deadlock
        
        # 🔧 Fix: no background threads, check timeouts through message callbacks
        self.last_timeout_check = time.time()
        
        # Statistics
        self.total_downloaded = 0
        self.total_uploaded = 0
        # 🔧 CRITICAL FIX: Read chunks_per_client from config instead of hardcoding
        self.chunks_per_client = cfg.chunk.num_chunks if (cfg and hasattr(cfg, 'chunk') and hasattr(cfg.chunk, 'num_chunks')) else 16
        # 🔧 CRITICAL FIX: Store total client count for proper expected_chunks calculation
        self.total_clients = cfg.federate.client_num if (cfg and hasattr(cfg, 'federate') and hasattr(cfg.federate, 'client_num')) else 50
        
        # 🔧 CRITICAL FIX: Exchange state management
        self.is_stopped = False  # Stop flag for exchange termination
        
        # 🚀 ENDGAME OPTIMIZATION: Multi-peer parallel requests for final chunks
        # 🔧 FIX: Changed defaults - 0.9 threshold (was 0.5) and max 2 parallel peers (was 10)
        # Previous values caused massive duplicate requests (50% of training in endgame, 10x requests per chunk)
        self.min_completion_ratio = cfg.bittorrent.min_completion_ratio if (cfg and hasattr(cfg, 'bittorrent') and hasattr(cfg.bittorrent, 'min_completion_ratio')) else 0.9
        self.is_endgame_mode = False  # Endgame mode flag
        self.endgame_requests: Dict[Tuple, List[int]] = {}  # {chunk_key: [peer_id_list]} - Track multiple requests
        self.endgame_max_parallel_peers = cfg.bittorrent.endgame_max_parallel_peers if (cfg and hasattr(cfg, 'bittorrent') and hasattr(cfg.bittorrent, 'endgame_max_parallel_peers')) else 2
        self.endgame_start_time = None  # When endgame mode started

        # 🚀 DEADLOCK FIX S1: Upload task queue + worker thread
        # Purpose: Make handle_request() non-blocking by moving DB read + send to background worker
        # This breaks the deadlock cycle where callback threads block on DB/send operations
        # 🔧 MEMORY TEST: Reduced from 10000 to 100 to limit pending upload tasks
        self._upload_task_queue = queue.Queue(maxsize=100)
        self._upload_worker_thread = None  # Will be started in start_exchange()

        # 🚀 EVENT-DRIVEN: Request trigger queue
        # Callbacks put chunk_keys here, main loop processes them to send REQUESTs
        # This enables event-driven requests instead of busy-polling
        # 🔧 MEMORY TEST: Reduced from 10000 to 500
        self._request_trigger_queue = queue.Queue(maxsize=500)

        logger.debug(f"[BT] BitTorrentManager initialized for client {client_id}, round {round_num}")
        logger.debug(f"[BT] Client {client_id}: Concurrent settings - Active requests: {self.MAX_ACTIVE_REQUESTS}, Pending queue: {self.MAX_PENDING_QUEUE}, Upload slots: {self.MAX_UPLOAD_SLOTS}")
        logger.debug(f"[BT] Client {client_id}: Endgame settings - Threshold: {self.min_completion_ratio:.1%}, Max parallel peers: {self.endgame_max_parallel_peers}")
        
    def start_exchange(self):
        """Start BitTorrent chunk exchange process (no Tracker needed)"""
        logger.info(f"[BT] Client {self.client_id}: Starting BitTorrent exchange")
        logger.debug(f"[BT] Client {self.client_id}: Neighbors: {self.neighbors}")

        # 🚀 OPTIMIZATION 3: Start single writer thread
        self.chunk_write_queue.start_writer_thread()

        # 🚀 DEADLOCK FIX S1: Start upload worker thread
        # This thread handles DB read + send operations, keeping callback thread non-blocking
        self._upload_worker_thread = threading.Thread(
            target=self._upload_worker_loop,
            daemon=True,
            name=f"UploadWorker-{self.client_id}"
        )
        self._upload_worker_thread.start()
        logger.debug(f"[BT] Client {self.client_id}: Upload worker thread started")

        # 1. Send bitfield directly to all topology neighbors
        for neighbor_id in self.neighbors:
            logger.debug(f"[BT] Client {self.client_id}: Sending bitfield to neighbor {neighbor_id}")
            self._send_bitfield(neighbor_id)
        
        # 2. Start periodic unchoke algorithm (every 10 seconds)
        self._schedule_regular_unchoke()
        
        # 3. Start optimistic unchoke (every 30 seconds)
        self._schedule_optimistic_unchoke()
        
    def handle_bitfield(self, sender_id: int, bitfield_content: Dict):
        """
        🚀 EVENT-DRIVEN: Non-blocking bitfield handler that triggers requests

        Critical: This runs in gRPC callback thread. MUST NOT:
        - Read DB (get_global_bitfield)
        - Send network messages
        - Block on any I/O

        Only allowed: store bitfield in memory, enqueue needed chunks for request.
        """
        if self.is_stopped:
            return

        # Gossip-style DFL: ignore bitfield from non-neighbors
        if self.neighbor_only and sender_id not in self.neighbors:
            logger.debug(f"[BT] Client {self.client_id}: Ignoring bitfield from non-neighbor {sender_id}")
            return

        # Parse bitfield (memory operations only, no blocking)
        if isinstance(bitfield_content, dict) and 'bitfield' in bitfield_content:
            bitfield_list = bitfield_content.get('bitfield', [])
            bitfield = {}

            if not hasattr(self, 'peer_importance_scores'):
                self.peer_importance_scores = {}

            if sender_id not in self.peer_importance_scores:
                self.peer_importance_scores[sender_id] = {}

            for chunk_entry in bitfield_list:
                chunk_key = (chunk_entry['round'], chunk_entry['source'], chunk_entry['chunk'])
                bitfield[chunk_key] = True
                importance_score = chunk_entry.get('importance_score', 0.0)
                self.peer_importance_scores[sender_id][chunk_key] = importance_score

                # 🚀 EVENT-DRIVEN: Trigger request for each chunk we might need
                # 🔧 FIX: Also check chunks_received_set to prevent duplicate requests for recently received chunks
                if (chunk_key not in self.pending_requests and
                    chunk_key not in getattr(self, 'processed_pieces', set()) and
                    chunk_key not in getattr(self, 'chunks_received_set', set()) and
                    chunk_key not in getattr(self, 'own_chunks_set', set())):
                    try:
                        self._request_trigger_queue.put_nowait((chunk_key, sender_id, importance_score))
                    except queue.Full:
                        break  # Queue full, remaining chunks will be handled via HAVE messages
        else:
            bitfield = bitfield_content
            # Old format: enqueue all chunks
            for chunk_key in bitfield.keys():
                # 🔧 FIX: Also check chunks_received_set to prevent duplicate requests
                if (chunk_key not in self.pending_requests and
                    chunk_key not in getattr(self, 'processed_pieces', set()) and
                    chunk_key not in getattr(self, 'chunks_received_set', set()) and
                    chunk_key not in getattr(self, 'own_chunks_set', set())):
                    try:
                        self._request_trigger_queue.put_nowait((chunk_key, sender_id, 0.0))
                    except queue.Full:
                        break

        # Store bitfield (memory operation)
        self.peer_bitfields[sender_id] = bitfield

        logger.info(f"[BT] Client {self.client_id}: Received bitfield from peer {sender_id} ({len(bitfield)} chunks), enqueued for requests")
            
    def handle_interested(self, sender_id: int):
        """Handle interested messages"""
        self.interested_by.add(sender_id)
        logger.debug(f"[BT] Client {self.client_id}: Peer {sender_id} is interested")
        # Decide whether to unchoke based on current upload slots
        self._evaluate_unchoke(sender_id)
        
    def handle_request(self, sender_id: int, round_num: int, source_client_id: int, chunk_id: int):
        """🚀 DEADLOCK FIX S1: Non-blocking handle_request - only enqueue task, return immediately.

        This breaks the deadlock cycle where callback threads block on DB read / send operations.
        The actual DB read + send work is done by the upload worker thread.
        """
        # Quick checks that don't block
        if self.is_stopped:
            logger.debug(f"[BT-HANDLE] Client {self.client_id}: Ignoring request - exchange stopped")
            return

        if round_num != self.round_num:
            logger.debug(f"[BT-HANDLE] Client {self.client_id}: Round mismatch {round_num} vs {self.round_num}, skipping")
            return

        if sender_id in self.choked_peers:
            logger.debug(f"[BT-HANDLE] Client {self.client_id}: Peer {sender_id} is choked, ignoring request")
            return

        # 🚀 DEADLOCK FIX: Only enqueue the task, don't do DB read or send here
        # This ensures callback thread returns immediately, never blocking message processing
        try:
            self._upload_task_queue.put_nowait((sender_id, round_num, source_client_id, chunk_id))
            logger.debug(f"[BT-HANDLE] Client {self.client_id}: Queued upload task for chunk {source_client_id}:{chunk_id} to peer {sender_id}")
        except queue.Full:
            logger.warning(f"[BT-HANDLE] Client {self.client_id}: Upload task queue full, dropping request for {source_client_id}:{chunk_id} from {sender_id}")
            
    def handle_piece(self, sender_id: int, round_num: int, source_client_id: int, chunk_id: int, chunk_data, checksum: str):
        """
        🚀 DEADLOCK FIX: Non-blocking chunk reception callback

        Critical: This runs in gRPC callback thread. MUST NOT block on:
        - Database reads
        - Network sends
        - Queue waits
        - Any synchronization primitives

        Only allowed: put_nowait to enqueue, then return immediately.
        """
        # Quick checks that don't block
        if self.is_stopped:
            return False

        if round_num != self.round_num:
            return False

        chunk_key = (round_num, source_client_id, chunk_id)
        if not hasattr(self, 'processed_pieces'):
            self.processed_pieces = set()

        # Check duplicate BEFORE any state changes
        if chunk_key in self.processed_pieces:
            return True  # Already processed

        # Extract raw bytes (no blocking, just type conversion)
        if isinstance(chunk_data, ChunkData):
            raw_bytes = chunk_data.raw_bytes
        elif isinstance(chunk_data, bytes):
            raw_bytes = chunk_data
        else:
            logger.error(f"[BT-PIECE] Client {self.client_id}: Unexpected data type: {type(chunk_data)}")
            return False

        # Quick size check
        if len(raw_bytes) == 0 or len(raw_bytes) > 50 * 1024 * 1024:
            return False

        # 🚀 CRITICAL FIX: Try enqueue FIRST, before modifying ANY state
        # If enqueue fails, return False immediately - let timeout trigger retry
        ok = self.chunk_write_queue.enqueue_chunk_write(
            round_num=round_num,
            source_client_id=source_client_id,
            chunk_id=chunk_id,
            chunk_data=raw_bytes,
            checksum=checksum,
            timestamp=time.time()
        )

        if not ok:
            # Enqueue failed (queue full) - DON'T mark as processed!
            # Timeout mechanism will trigger retry from peer
            # 🔧 Changed to debug: queue full is expected under high load, not an error
            logger.debug(f"[BT-PIECE] Client {self.client_id}: Enqueue failed for {source_client_id}:{chunk_id}, will retry")
            return False

        # 🚀 ONLY after successful enqueue: mark as processed and update state
        self.processed_pieces.add(chunk_key)

        # 🚀 CRITICAL FIX: Update memory-based chunk counter to avoid DB queries
        self.chunks_received_set.add(chunk_key)

        if chunk_key in self.pending_requests:
            del self.pending_requests[chunk_key]

        self.total_downloaded += len(raw_bytes)

        # Lightweight stats update (no blocking)
        self._update_download_rate(sender_id, len(raw_bytes))
        self.last_activity[sender_id] = time.time()

        # 🚀 FIX: Signal that a slot is available for new requests
        # This triggers immediate refill instead of waiting for next loop cycle
        # Put a dummy trigger to wake up the main loop
        try:
            self._request_trigger_queue.put_nowait(("__REFILL__", -1, 0.0))
        except:
            pass  # Queue full is OK, main loop will refill anyway

        return True
        
    def handle_have(self, sender_id: int, round_num: int, source_client_id: int, chunk_id: int, importance_score: float = 0.0):
        """
        🚀 EVENT-DRIVEN: Handle have messages and trigger request if needed

        When peer announces a chunk we need, enqueue it for request.
        This is the core event-driven trigger for BitTorrent protocol.
        """
        if round_num != self.round_num:
            return

        chunk_key = (round_num, source_client_id, chunk_id)

        # Update peer bitfield (memory only, no blocking)
        if sender_id not in self.peer_bitfields:
            self.peer_bitfields[sender_id] = {}
        self.peer_bitfields[sender_id][chunk_key] = True

        # Store importance scores
        if not hasattr(self, 'peer_importance_scores'):
            self.peer_importance_scores = {}
        if sender_id not in self.peer_importance_scores:
            self.peer_importance_scores[sender_id] = {}
        self.peer_importance_scores[sender_id][chunk_key] = importance_score

        # 🚀 EVENT-DRIVEN: Trigger request if we need this chunk
        # 🔧 FIX: Also check chunks_received_set to prevent duplicate requests for recently received chunks
        if (chunk_key not in self.pending_requests and
            chunk_key not in getattr(self, 'processed_pieces', set()) and
            chunk_key not in getattr(self, 'chunks_received_set', set()) and
            chunk_key not in getattr(self, 'own_chunks_set', set())):
            try:
                self._request_trigger_queue.put_nowait((chunk_key, sender_id, importance_score))
            except queue.Full:
                pass  # Queue full, will retry via timeout mechanism
        
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
        
        # 🚀 CRITICAL FIX: Get memory bitfield once before loop (NO DB QUERY!)
        my_bitfield = self.get_memory_bitfield()
        current_chunks = len(my_bitfield)
        expected_chunks = self.total_clients * self.chunks_per_client
        completion_ratio = current_chunks / max(expected_chunks, 1)

        while (len(self.pending_requests) < self.MAX_ACTIVE_REQUESTS and
               len(self.pending_queue) > 0):

            # Take chunk from queue head (already sorted by importance)
            chunk_key = self.pending_queue.pop(0)

            # Check if chunk is still needed (using memory bitfield)
            if chunk_key in my_bitfield or chunk_key in self.pending_requests:
                continue  # Skip chunks already owned or being requested

            # 🔧 FIX: Real-time check for recently received chunks (not in cached bitfield)
            # This prevents duplicate requests when chunks arrive during loop execution
            if (chunk_key in getattr(self, 'chunks_received_set', set()) or
                chunk_key in getattr(self, 'processed_pieces', set())):
                continue

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
            
            
            
            if completion_ratio >= self.min_completion_ratio:
                # 🎯 ENDGAME MODE: Send requests to multiple peers for faster completion
                endgame_peer_count = min(self.endgame_max_parallel_peers, len(available_peers))
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
                # 🚀 FIX: Continue to fill more slots instead of breaking after 1 chunk
            else:
                # 🎯 NORMAL MODE: Send request to first available peer
                peer_id = available_peers[0]
                try:
                    success = self._send_request(peer_id, source_id, chunk_id)
                    if success:
                        logger.debug(f"[BT-POOL] Client {self.client_id}: Transferred chunk {chunk_key} from queue to active pool")
                        # 🚀 FIX: Continue to fill more slots instead of breaking after 1 request
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
        
        # 🚀 CRITICAL FIX: Use memory bitfield (NO DB QUERY!)
        my_bitfield = self.get_memory_bitfield()

        # Select needed chunks
        # 🔧 FIX: Also check processed_pieces to prevent adding already received chunks to queue
        processed_pieces = getattr(self, 'processed_pieces', set())
        for chunk_key, availability_count in chunk_availability.items():
            if (chunk_key not in my_bitfield and
                chunk_key not in self.pending_requests and
                chunk_key not in self.pending_queue and
                chunk_key not in processed_pieces):

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
            if tau < 1:
                needed_chunks.sort(
                    key=lambda x: (
                        x['importance'] + (tau - eps) * x['rarity'] + gamma * random.uniform(-1, 1)
                    ),
                    reverse=True
                )
            else:
                # Sort needed chunks by rarity
                needed_chunks.sort(
                    key=lambda x: (
                        (tau - eps) * x['rarity'] + gamma * random.uniform(-1, 1)
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
        # Note: Message is imported at module level to avoid import lock contention

        # 🚀 CRITICAL FIX: Use memory bitfield to avoid DB query
        my_bitfield = self.get_memory_bitfield()

        # 🔧 GOSSIP FIX: In neighbor_only mode, only send own chunks in bitfield
        # This prevents peers from requesting non-neighbor chunks through relay
        if self.neighbor_only:
            original_count = len(my_bitfield)
            filtered_bitfield = {}
            for chunk_key, has_chunk in my_bitfield.items():
                if has_chunk:
                    round_num, source_id, chunk_id = chunk_key
                    if source_id == self.client_id:
                        filtered_bitfield[chunk_key] = True
            my_bitfield = filtered_bitfield
            logger.info(f"[BT] Client {self.client_id}: Gossip mode - filtered bitfield from {original_count} to {len(my_bitfield)} (own chunks only)")

        logger.debug(f"[BT] Client {self.client_id}: My bitfield for round {self.round_num}: {len(my_bitfield)} chunks (memory-based)")
        
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
        # Note: Message is imported at module level to avoid import lock contention

        # 🔧 GOSSIP FIX: In neighbor_only mode, only broadcast own chunks
        # This prevents relay of non-neighbor chunks which would break Gossip semantics
        if self.neighbor_only and source_client_id != self.client_id:
            logger.debug(f"[BT] Client {self.client_id}: Gossip mode - skipping HAVE broadcast for non-own chunk (source={source_client_id})")
            return

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
                logger.debug(f"[BT] Max retries reached for chunk {chunk_key}")
                # Safe deletion: check if key exists before deleting
                if chunk_key in self.pending_requests:
                    del self.pending_requests[chunk_key]
                if chunk_key in self.retry_count:
                    del self.retry_count[chunk_key]

    def process_request_triggers(self, max_requests: int = 10, cached_bitfield: dict = None) -> int:
        """
        🚀 EVENT-DRIVEN: Process request trigger queue and send REQUESTs

        This is called by main loop to process chunks that need to be requested.
        Returns the number of requests sent.

        Args:
            max_requests: Maximum number of requests to process in one call
            cached_bitfield: Optional pre-fetched bitfield to avoid duplicate DB query

        Returns:
            Number of requests actually sent
        """
        if self.is_stopped:
            return 0

        requests_sent = 0
        processed_chunks = set()  # Avoid duplicate processing in same batch

        # 🚀 CRITICAL FIX: Use cached bitfield if provided to avoid SQLite lock contention
        # This prevents process freeze when main loop already queried the bitfield
        if cached_bitfield is not None:
            my_bitfield = cached_bitfield
        else:
            # 🚀 CRITICAL FIX: Use memory bitfield as fallback (NO DB QUERY!)
            my_bitfield = self.get_memory_bitfield()

        while requests_sent < max_requests:
            try:
                item = self._request_trigger_queue.get_nowait()
            except queue.Empty:
                break

            chunk_key, peer_id, importance_score = item

            # 🚀 FIX: Handle refill trigger (signals slot available)
            if chunk_key == "__REFILL__":
                continue  # Skip dummy trigger, main loop will call _transfer_from_queue_to_active()

            # Skip if already processed in this batch
            if chunk_key in processed_chunks:
                continue
            processed_chunks.add(chunk_key)

            # Skip if we already have this chunk (using cached bitfield)
            if chunk_key in my_bitfield:
                continue

            # Skip if already pending
            if chunk_key in self.pending_requests:
                continue

            # Skip if already processed
            if chunk_key in getattr(self, 'processed_pieces', set()):
                continue

            # 🔧 FIX: Also check chunks_received_set to prevent race condition
            # This catches chunks that were received after bitfield was cached
            if chunk_key in getattr(self, 'chunks_received_set', set()):
                continue

            # 🔧 FIX: Check own_chunks_set as well
            if chunk_key in getattr(self, 'own_chunks_set', set()):
                continue

            # Skip if peer is choked
            if peer_id in self.choked_peers:
                # Try to find another peer
                alternative_peers = self._find_peer_with_chunk(chunk_key)
                available_peers = [p for p in alternative_peers if p not in self.choked_peers]
                if available_peers:
                    peer_id = available_peers[0]
                else:
                    continue  # No available peer

            # Check if we have room for more requests
            if len(self.pending_requests) >= self.MAX_ACTIVE_REQUESTS:
                # Put back in queue for later
                try:
                    self._request_trigger_queue.put_nowait((chunk_key, peer_id, importance_score))
                except queue.Full:
                    pass
                break  # Stop processing, we're at capacity

            # Send the request
            round_num, source_id, chunk_id = chunk_key
            try:
                success = self._send_request(peer_id, source_id, chunk_id)
                if success:
                    requests_sent += 1
                else:
                    # 🔧 CRITICAL FIX: Requeue failed requests instead of dropping them!
                    # This prevents "dropped trigger" deadlock when streaming layer has backpressure
                    try:
                        self._request_trigger_queue.put_nowait((chunk_key, peer_id, importance_score))
                        logger.debug(f"[BT] Client {self.client_id}: Requeued rejected request for {chunk_key}")
                    except queue.Full:
                        logger.warning(f"[BT] Client {self.client_id}: Queue full, cannot requeue {chunk_key}")
            except Exception as e:
                logger.warning(f"[BT] Client {self.client_id}: Failed to send request for {chunk_key}: {e}")
                # 🔧 Also requeue on exception to prevent chunk loss
                try:
                    self._request_trigger_queue.put_nowait((chunk_key, peer_id, importance_score))
                except queue.Full:
                    pass

        return requests_sent

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

    def _upload_worker_loop(self):
        """🚀 DEADLOCK FIX S1: Background worker for upload tasks (DB read + send).

        This thread handles all potentially blocking operations:
        - Database reads (chunk_manager.get_chunk_data)
        - Network sends (_send_piece)

        By moving these out of the callback thread, we break the deadlock cycle
        where callback threads block on DB/send, preventing message processing.

        The worker NEVER exits on errors - if it stops, the queue fills up forever.
        """
        logger.debug(f"[UploadWorker] Client {self.client_id}: Upload worker started")
        processed_count = 0
        retry_backoff = 0.02  # Initial backoff for congestion

        while not self.is_stopped:
            try:
                # Block waiting for task with timeout (allows checking is_stopped)
                try:
                    sender_id, round_num, source_id, chunk_id = self._upload_task_queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                # Skip if exchange stopped or round changed
                if self.is_stopped or round_num != self.round_num:
                    self._upload_task_queue.task_done()
                    continue

                try:
                    # DB read - may take time but won't block callback thread
                    chunk_data = self.chunk_manager.get_chunk_data(round_num, source_id, chunk_id)

                    # Gentle retry if not found (write queue might still be processing)
                    if chunk_data is None:
                        time.sleep(0.05)
                        chunk_data = self.chunk_manager.get_chunk_data(round_num, source_id, chunk_id)

                    if chunk_data is not None:
                        # Send piece - may block on network but won't block callback thread
                        success = self._send_piece(sender_id, round_num, source_id, chunk_id, chunk_data)
                        if success:
                            processed_count += 1
                            if processed_count % 100 == 0:
                                logger.debug(f"[UploadWorker] Client {self.client_id}: Processed {processed_count} upload tasks")
                        else:
                            # 🚀 P1 FIX: DON'T requeue on send failure!
                            # Let peer's request timeout trigger a new request
                            # This prevents self-amplifying congestion when peer is slow
                            logger.debug(f"[UploadWorker] Client {self.client_id}: Send failed for {source_id}:{chunk_id} to peer {sender_id}, peer will retry via timeout")
                    else:
                        logger.warning(f"[UploadWorker] Client {self.client_id}: Chunk {source_id}:{chunk_id} not found for peer {sender_id}")

                except Exception as e:
                    logger.error(f"[UploadWorker] Client {self.client_id}: Error processing task: {e}")
                    # Don't exit on error - keep worker alive

                self._upload_task_queue.task_done()

            except Exception as e:
                logger.error(f"[UploadWorker] Client {self.client_id}: Worker loop error: {e}")
                time.sleep(0.1)  # Brief pause to avoid error spinning

        logger.debug(f"[UploadWorker] Client {self.client_id}: Upload worker stopped, processed {processed_count} tasks")

    def _send_piece(self, peer_id: int, round_num: int, source_client_id: int, chunk_id: int, chunk_data):
        """🚀 Optimization 4: Smart sending strategy - multi-optimized chunk transmission"""
        import pickle
        
        # 🚀 Data preparation and preprocessing
        send_start_time = time.time()
        # chunk_data is already bytes from optimized cache/write_queue, no need to re-serialize
        if isinstance(chunk_data, bytes):
            serialized_data = chunk_data  # Use bytes directly
        else:
            serialized_data = pickle.dumps(chunk_data)  # Fallback for legacy data
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
                # Note: Message and ChunkData are imported at module level to avoid import lock contention
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

        # 🚀 CRITICAL FIX: Use memory bitfield (NO DB QUERY!)
        my_bitfield = self.get_memory_bitfield()
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
        # 🚀 CRITICAL FIX: Use memory bitfield (NO DB QUERY!)
        my_bitfield = self.get_memory_bitfield()
        # 🔧 CRITICAL FIX: Use total_clients for expected chunks calculation
        total_expected = self.total_clients * self.chunks_per_client  # All clients' chunks

        return {
            'chunks_collected': len(my_bitfield),
            'total_expected': total_expected,
            'progress_ratio': len(my_bitfield) / total_expected if total_expected > 0 else 0,
            'active_peers': len(self.peer_bitfields),
            'pending_requests': len(self.pending_requests),
            'bytes_downloaded': self.total_downloaded,
            'bytes_uploaded': self.total_uploaded
        }

    def set_own_chunks_count(self, count: int):
        """🚀 CRITICAL FIX: Set own chunks count for memory-based completion check
        Called when client's own chunks are registered at the start of exchange"""
        self.own_chunks_count = count
        logger.debug(f"[BT] Client {self.client_id}: Set own_chunks_count = {count}")

    def set_expected_total_chunks(self, total: int):
        """🚀 CRITICAL FIX: Set expected total chunks for completion check
        Called at start of exchange to know when download is complete"""
        self.expected_total_chunks = total
        logger.debug(f"[BT] Client {self.client_id}: Set expected_total_chunks = {total}")

    def get_memory_chunk_count(self) -> int:
        """🚀 CRITICAL FIX: Get current chunk count using memory counter (NO DB QUERY!)
        Returns: own_chunks + received_chunks (memory-only, instant)"""
        return self.own_chunks_count + len(self.chunks_received_set)

    def is_complete_by_memory(self) -> bool:
        """🚀 CRITICAL FIX: Check if download is complete using memory counter (NO DB QUERY!)
        Returns True if own_chunks + received_chunks >= expected_total_chunks"""
        if self.expected_total_chunks <= 0:
            return False  # Not initialized yet
        current_count = self.get_memory_chunk_count()
        return current_count >= self.expected_total_chunks

    def get_memory_bitfield(self) -> Dict[Tuple, bool]:
        """🚀 CRITICAL FIX: Get bitfield from memory (NO DB QUERY!)
        Returns dict of {(round_num, source_id, chunk_id): True} for all owned chunks.
        This replaces get_global_bitfield() calls in the main loop to avoid SQLite lock.

        🔧 MEMORY FIX: Uses cached bitfield to avoid creating new objects on every call.
        The cache is invalidated when chunks_received_set or own_chunks_set changes.
        """
        # 🔧 MEMORY FIX: Cache the bitfield to avoid repeated object creation
        # Check if cache is valid (same size as underlying sets)
        current_size = len(self.own_chunks_set) + len(self.chunks_received_set)

        if (hasattr(self, '_bitfield_cache') and
            hasattr(self, '_bitfield_cache_size') and
            self._bitfield_cache_size == current_size):
            return self._bitfield_cache

        # Cache miss or invalidated - rebuild
        bitfield = {}
        # 🚀 Thread-safety fix: Create copies to avoid "Set changed size during iteration"
        try:
            own_chunks_copy = set(self.own_chunks_set)
            received_chunks_copy = set(self.chunks_received_set)
        except RuntimeError:
            # Fallback if copy fails during concurrent modification
            own_chunks_copy = set()
            received_chunks_copy = set()
        # Add own chunks
        for chunk_key in own_chunks_copy:
            bitfield[chunk_key] = True
        # Add received chunks
        for chunk_key in received_chunks_copy:
            bitfield[chunk_key] = True

        # Update cache
        self._bitfield_cache = bitfield
        self._bitfield_cache_size = current_size
        return bitfield

    def register_own_chunks(self, chunk_keys: list):
        """🚀 Register own chunks to memory set for fast lookup.
        Called when client's own chunks are saved at the start of exchange."""
        for chunk_key in chunk_keys:
            self.own_chunks_set.add(chunk_key)
        self.own_chunks_count = len(self.own_chunks_set)
        logger.debug(f"[BT] Client {self.client_id}: Registered {len(chunk_keys)} own chunks to memory set")

    def stop_exchange(self):
        """
        🔧 CRITICAL FIX: Stop BitTorrent exchange immediately
        This method is called when server timeout occurs or new round begins
        to prevent interference with next round BitTorrent operations
        """
        logger.debug(f"[BT] Client {self.client_id}: Stopping BitTorrent exchange for round {self.round_num}")

        # 🚀 CRITICAL FIX: Save final chunk count BEFORE setting is_stopped
        # The BT thread will read this after detecting is_stopped=True
        # This prevents race condition where counters are cleared before BT thread reads them
        self.final_chunk_count = len(self.own_chunks_set) + len(self.chunks_received_set)
        logger.debug(f"[BT] Client {self.client_id}: Saved final_chunk_count={self.final_chunk_count} before stopping")

        # Set stop flag
        self.is_stopped = True
        
        # 🚀 OPTIMIZATION 3: Gracefully close write thread to ensure data integrity
        queue_size = self.chunk_write_queue.write_queue.qsize()
        if queue_size > 0:
            logger.debug(f"[BT] Client {self.client_id}: Detected {queue_size} pending chunks, waiting for background writes to complete...")
            # Normal case: wait for queue processing to complete (max 30 seconds)
            self.chunk_write_queue.stop_writer_thread(force_immediate=False, max_wait_time=30.0)

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

        # 🚀 CRITICAL FIX: Clear memory-based chunk counter
        received_count = len(self.chunks_received_set)
        self.chunks_received_set.clear()
        self.own_chunks_count = 0
        self.expected_total_chunks = 0
        logger.debug(f"[BT] Client {self.client_id}: Cleared memory counter - {received_count} received chunks")

        # 🔧 MEMORY LEAK FIX: Clear own_chunks_set to prevent memory growth between rounds
        # This set stores (round_num, source_id, chunk_id) tuples for all own chunks
        own_count = len(self.own_chunks_set)
        self.own_chunks_set.clear()
        logger.debug(f"[BT] Client {self.client_id}: Cleared own_chunks_set - {own_count} entries")

        # 🔧 MEMORY FIX: Clear bitfield cache to prevent stale data
        if hasattr(self, '_bitfield_cache'):
            del self._bitfield_cache
        if hasattr(self, '_bitfield_cache_size'):
            del self._bitfield_cache_size

        # 🔧 MEMORY LEAK FIX: Clear additional data structures that accumulate between rounds
        # pending_queue stores chunk_key tuples waiting to be requested
        if hasattr(self, 'pending_queue'):
            pending_count = len(self.pending_queue)
            self.pending_queue.clear()
            if pending_count > 0:
                logger.debug(f"[BT] Client {self.client_id}: Cleared pending_queue - {pending_count} entries")

        # endgame_requests stores {chunk_key: [peer_id_list]} for parallel requests
        if hasattr(self, 'endgame_requests'):
            endgame_count = len(self.endgame_requests)
            self.endgame_requests.clear()
            if endgame_count > 0:
                logger.debug(f"[BT] Client {self.client_id}: Cleared endgame_requests - {endgame_count} entries")

        # _importance_cache stores {round_num: {chunk_id: importance_score}}
        if hasattr(self, '_importance_cache'):
            cache_count = len(self._importance_cache)
            self._importance_cache.clear()
            if cache_count > 0:
                logger.debug(f"[BT] Client {self.client_id}: Cleared _importance_cache - {cache_count} rounds")

        if hasattr(self, '_cache_initialized'):
            self._cache_initialized.clear()

        # peer_importance_scores stores {peer_id: {chunk_key: importance_score}}
        if hasattr(self, 'peer_importance_scores'):
            peer_count = len(self.peer_importance_scores)
            self.peer_importance_scores.clear()
            if peer_count > 0:
                logger.debug(f"[BT] Client {self.client_id}: Cleared peer_importance_scores - {peer_count} peers")

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

        # 🔧 MEMORY FIX: Force garbage collection after clearing all data structures
        # This ensures Python releases memory immediately instead of waiting for automatic GC
        gc.collect()
        logger.debug(f"[BT] Client {self.client_id}: Forced garbage collection after stop_exchange")

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
        
        #Clear old file files
        if self.cfg and hasattr(self.cfg, 'chunk_keep_rounds'):
            keep_rounds = self.cfg.chunk_keep_rounds
        else:
            keep_rounds = 2
        
        self.chunk_manager.cleanup_old_rounds(keep_rounds=keep_rounds, current_round=self.round_num)
        
        # 🔧 Emergency cleanup: Clear deduplication sets
        if hasattr(self, 'processed_pieces'):
            self.processed_pieces.clear()

        # 🚀 Emergency cleanup: Clear memory-based chunk counter
        self.chunks_received_set.clear()
        self.own_chunks_count = 0
        self.expected_total_chunks = 0

        if self.streaming_manager:
            for channel in self.streaming_manager.channels.values():
                if hasattr(channel, 'requested_chunks'):
                    channel.requested_chunks.clear()
                if hasattr(channel, 'completed_chunks'):
                    channel.completed_chunks.clear()

        # 🔧 MEMORY FIX: Force garbage collection
        gc.collect()
        logger.warning(f"[BT] Client {self.client_id}: Emergency stop completed")