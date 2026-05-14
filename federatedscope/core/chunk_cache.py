"""
🚀 High-Performance Chunk Data Cache System
Two-tier storage: RAM Cache + Disk Spool for chunk data

Replaces database storage with direct file I/O for maximum performance
File naming: client_{source_client_id}_chunk_{chunk_id}_r{round_num}.dat
"""

import gc
import os
import time
import hashlib
import threading
import logging
import tempfile
from typing import Dict, Optional, Tuple, Any
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
# pickle removed - no automatic serialization for security

logger = logging.getLogger(__name__)

class ChunkDataCache:
    """
    🚀 Two-tier chunk data cache system
    
    Tier 1: RAM Cache - Fast in-memory access for recently accessed chunks
    Tier 2: Disk Spool - Direct file storage for persistent data
    
    Features:
    - Zero database overhead
    - Direct bytes storage/retrieval
    - LRU eviction policy for RAM
    - Thread-safe operations
    - Async disk spooling
    """
    
    def __init__(self, client_id: int, cache_dir: str = None, max_ram_chunks: int = 100, max_ram_size_mb: int = 256):
        """
        Initialize chunk data cache system
        
        Args:
            client_id: Client identifier
            cache_dir: Directory for disk spool (defaults to tmp/client_X/chunks/)
            max_ram_chunks: Maximum chunks in RAM cache
            max_ram_size_mb: Maximum RAM cache size in MB
        """
        self.client_id = client_id
        
        # Setup cache directory
        if cache_dir is None:
            cache_dir = os.path.join(os.getcwd(), "tmp", f"client_{client_id}", "chunks")
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # RAM Cache configuration
        self.max_ram_chunks = max_ram_chunks
        self.max_ram_size = max_ram_size_mb * 1024 * 1024  # Convert to bytes
        
        # RAM Cache: LRU ordered dictionary {chunk_key: (data_bytes, size, timestamp)}
        self.ram_cache: OrderedDict[str, Tuple[bytes, int, float]] = OrderedDict()
        self.ram_cache_size = 0
        self.cache_lock = threading.RLock()
        
        # Async disk spooling with backpressure to prevent OOM
        # 🔧 MEMORY FIX: Limit in-flight disk writes to prevent unbounded queue growth
        # Without backpressure, Future objects hold references to chunk_bytes indefinitely
        # when submit rate > disk write rate, causing memory to grow unbounded.
        #
        # max_inflight calculation: assuming avg chunk ~3MB, allow ~60MB in-flight
        # This provides buffer for burst writes while preventing OOM
        self.spool_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix=f"ChunkSpool-{client_id}")
        self.spool_semaphore = threading.Semaphore(20)  # Max 20 chunks in-flight (~60MB)
        
        # Statistics
        self.stats = {
            'ram_hits': 0,
            'disk_hits': 0,
            'misses': 0,
            'saves': 0,
            'evictions': 0
        }
        
        logger.info(f"[ChunkCache] Client {client_id}: Initialized cache - RAM: {max_ram_chunks} chunks/{max_ram_size_mb}MB, Disk: {cache_dir}")
    
    def _get_chunk_key(self, round_num: int, source_client_id: int, chunk_id: int) -> str:
        """Generate unique chunk key"""
        return f"{round_num}_{source_client_id}_{chunk_id}"
    
    def _get_chunk_filepath(self, round_num: int, source_client_id: int, chunk_id: int) -> str:
        """Generate chunk file path for disk spool"""
        filename = f"client_{source_client_id}_chunk_{chunk_id}_r{round_num}.dat"
        return os.path.join(self.cache_dir, filename)
    
    def _evict_lru_chunks(self, target_size: int):
        """Evict least recently used chunks from RAM to make space"""
        with self.cache_lock:
            while self.ram_cache_size > target_size and self.ram_cache:
                # Remove oldest item (LRU)
                chunk_key, (data_bytes, size, timestamp) = self.ram_cache.popitem(last=False)
                self.ram_cache_size -= size
                self.stats['evictions'] += 1
                logger.debug(f"[ChunkCache] Client {self.client_id}: Evicted chunk {chunk_key} ({size} bytes)")
    
    def save_chunk_data(self, round_num: int, source_client_id: int, chunk_id: int, chunk_data: bytes):
        """
        🚀 Save chunk data to cache system
        
        Args:
            round_num: Round number
            source_client_id: Source client ID
            chunk_id: Chunk identifier
            chunk_data: Raw chunk data as bytes (no automatic serialization for security)
        """
        # 🔧 SECURITY: Only accept bytes, no automatic pickling
        if not isinstance(chunk_data, bytes):
            raise TypeError(f"chunk_data must be bytes, got {type(chunk_data)}. Caller should serialize manually.")
        
        data_bytes = chunk_data
        
        chunk_key = self._get_chunk_key(round_num, source_client_id, chunk_id)
        data_size = len(data_bytes)
        current_time = time.time()
        
        # Add to RAM cache with LRU eviction
        with self.cache_lock:
            # Remove if already exists to update position
            if chunk_key in self.ram_cache:
                old_data, old_size, old_time = self.ram_cache.pop(chunk_key)
                self.ram_cache_size -= old_size
            
            # 🔧 CLARITY: Check if we need to evict to make space
            # Evict until: ram_cache_size + data_size <= max_ram_size
            if self.ram_cache_size + data_size > self.max_ram_size:
                target_size = self.max_ram_size - data_size
                self._evict_lru_chunks(target_size)
            
            # Add new chunk to RAM cache (if it fits)
            if data_size <= self.max_ram_size and len(self.ram_cache) < self.max_ram_chunks:
                self.ram_cache[chunk_key] = (data_bytes, data_size, current_time)
                self.ram_cache_size += data_size
                logger.debug(f"[ChunkCache] Client {self.client_id}: Cached chunk {chunk_key} in RAM ({data_size} bytes)")
        
        # Async spool to disk with backpressure
        # 🔧 MEMORY FIX: Acquire semaphore before submit to limit in-flight writes
        # This blocks if too many writes are pending, preventing unbounded memory growth
        self.spool_semaphore.acquire()
        try:
            future = self.spool_executor.submit(self._spool_to_disk, round_num, source_client_id, chunk_id, data_bytes)
            # Release semaphore when disk write completes (success or failure)
            future.add_done_callback(lambda _: self.spool_semaphore.release())
        except Exception:
            # If submit fails (e.g., executor shutdown), release semaphore immediately
            self.spool_semaphore.release()
            raise

        self.stats['saves'] += 1
        logger.debug(f"[ChunkCache] Client {self.client_id}: Saved chunk {source_client_id}:{chunk_id} r{round_num}")
    
    def _spool_to_disk(self, round_num: int, source_client_id: int, chunk_id: int, data_bytes: bytes):
        """Internal method: Spool chunk data to disk file with atomic writes"""
        filepath = self._get_chunk_filepath(round_num, source_client_id, chunk_id)
        temp_fd = None
        temp_filepath = None
        
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            # 🔧 CONCURRENCY FIX: Use mkstemp for unique temporary file (no race conditions)
            temp_fd, temp_filepath = tempfile.mkstemp(
                suffix='.tmp',
                prefix=f'chunk_{source_client_id}_{chunk_id}_r{round_num}_',
                dir=os.path.dirname(filepath)
            )
            
            # Write to unique temporary file
            with os.fdopen(temp_fd, 'wb') as f:
                temp_fd = None  # fdopen takes ownership of file descriptor
                f.write(data_bytes)
                f.flush()  # Flush to OS buffer
                os.fsync(f.fileno())  # Force sync to disk
            
            # Atomic replace: unique temp file -> final file
            os.replace(temp_filepath, filepath)
            temp_filepath = None  # Successfully moved, don't clean up
            
            logger.debug(f"[ChunkCache] Client {self.client_id}: Atomically spooled chunk {source_client_id}:{chunk_id} r{round_num} to disk ({len(data_bytes)} bytes)")
            
        except Exception as e:
            # Clean up temp file/fd on error
            if temp_fd is not None:
                try:
                    os.close(temp_fd)
                except:
                    pass
            
            if temp_filepath and os.path.exists(temp_filepath):
                try:
                    os.remove(temp_filepath)
                except:
                    pass
                    
            logger.error(f"[ChunkCache] Client {self.client_id}: Failed to spool chunk {source_client_id}:{chunk_id} r{round_num}: {e}")
    
    def get_chunk_data_bytes(self, round_num: int, source_client_id: int, chunk_id: int) -> Optional[bytes]:
        """
        🚀 Get chunk data from cache system as raw bytes (default method)
        
        Returns:
            Raw chunk data bytes or None if not found
            
        Note: Renamed from get_chunk_data for clarity - only returns bytes, no deserialization
        """
        chunk_key = self._get_chunk_key(round_num, source_client_id, chunk_id)
        
        # Try RAM cache first (fastest)
        with self.cache_lock:
            if chunk_key in self.ram_cache:
                data_bytes, size, _ = self.ram_cache.pop(chunk_key)
                # Move to end (most recently used)
                self.ram_cache[chunk_key] = (data_bytes, size, time.time())
                
                self.stats['ram_hits'] += 1
                logger.debug(f"[ChunkCache] Client {self.client_id}: RAM cache hit for {chunk_key}")
                return data_bytes
        
        # Try disk spool (slower but persistent)
        filepath = self._get_chunk_filepath(round_num, source_client_id, chunk_id)
        if os.path.exists(filepath):
            try:
                with open(filepath, 'rb') as f:
                    data_bytes = f.read()
                
                # Add back to RAM cache for future hits
                data_size = len(data_bytes)
                current_time = time.time()
                
                with self.cache_lock:
                    # 🔧 CONCURRENCY FIX: Check if chunk was already added by another thread
                    if chunk_key in self.ram_cache:
                        # Another thread already loaded this chunk, just update LRU position
                        existing_data, existing_size, _ = self.ram_cache.pop(chunk_key)
                        self.ram_cache[chunk_key] = (existing_data, existing_size, time.time())
                        logger.debug(f"[ChunkCache] Client {self.client_id}: Chunk {chunk_key} already in RAM (concurrent load)")
                    else:
                        # Make space if needed - clearer boundary check
                        if self.ram_cache_size + data_size > self.max_ram_size:
                            target_size = self.max_ram_size - data_size
                            self._evict_lru_chunks(target_size)
                        
                        # Add to RAM cache if fits
                        if data_size <= self.max_ram_size and len(self.ram_cache) < self.max_ram_chunks:
                            self.ram_cache[chunk_key] = (data_bytes, data_size, current_time)
                            self.ram_cache_size += data_size
                            logger.debug(f"[ChunkCache] Client {self.client_id}: Added disk chunk {chunk_key} back to RAM ({data_size} bytes)")
                        else:
                            logger.debug(f"[ChunkCache] Client {self.client_id}: Disk chunk {chunk_key} too large for RAM ({data_size} bytes)")
                
                self.stats['disk_hits'] += 1
                logger.debug(f"[ChunkCache] Client {self.client_id}: Disk cache hit for {chunk_key}")
                return data_bytes
                
            except Exception as e:
                logger.error(f"[ChunkCache] Client {self.client_id}: Failed to read disk chunk {chunk_key}: {e}")
                return None
        
        # Not found in either cache
        self.stats['misses'] += 1
        logger.debug(f"[ChunkCache] Client {self.client_id}: Cache miss for {chunk_key}")
        return None
    
    def get_chunk_data(self, round_num: int, source_client_id: int, chunk_id: int) -> Optional[bytes]:
        """
        🚀 Get chunk data from cache system (alias for get_chunk_data_bytes)
        
        Returns raw bytes - caller is responsible for deserialization if needed
        """
        return self.get_chunk_data_bytes(round_num, source_client_id, chunk_id)
    
    def has_chunk(self, round_num: int, source_client_id: int, chunk_id: int) -> bool:
        """Check if chunk exists in cache system"""
        chunk_key = self._get_chunk_key(round_num, source_client_id, chunk_id)
        
        # Check RAM cache
        with self.cache_lock:
            if chunk_key in self.ram_cache:
                return True
        
        # Check disk spool
        filepath = self._get_chunk_filepath(round_num, source_client_id, chunk_id)
        return os.path.exists(filepath)
    
    def cleanup_round(self, current_round: int, keep_rounds: int = 2):
        """Clean up old round data from disk"""
        try:
            # 🔧 CLEANUP FIX: Process all .dat files and extract their rounds
            cutoff_round = max(current_round - keep_rounds , 0)
            removed_files = 0
            
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.dat'):
                    # Extract round from filename format: client_X_chunk_Y_rZ.dat
                    try:
                        if '_r' in filename:
                            round_part = filename.split('_r')[1].split('.')[0]
                            file_round = int(round_part)
                            
                            # Remove files from rounds older than cutoff
                            if file_round < cutoff_round:
                                filepath = os.path.join(self.cache_dir, filename)
                                os.remove(filepath)
                                removed_files += 1
                                logger.debug(f"[ChunkCache] Client {self.client_id}: Removed old round {file_round} file: {filename}")
                    except (ValueError, IndexError) as e:
                        logger.debug(f"[ChunkCache] Client {self.client_id}: Couldn't parse round from filename {filename}: {e}")
                        continue
            
            if removed_files > 0:
                logger.info(f"[ChunkCache] Client {self.client_id}: Cleaned up {removed_files} old chunk files (rounds < {cutoff_round})")
            
            # Clean RAM cache for old rounds
            with self.cache_lock:
                keys_to_remove = []
                for chunk_key in self.ram_cache:
                    try:
                        key_round = int(chunk_key.split('_')[0])
                        if key_round < cutoff_round:
                            keys_to_remove.append(chunk_key)
                    except (ValueError, IndexError):
                        continue
                
                for key in keys_to_remove:
                    _, size, _ = self.ram_cache.pop(key)
                    self.ram_cache_size -= size
                    logger.debug(f"[ChunkCache] Client {self.client_id}: Cleaned up RAM chunk {key}")

            # 🔧 MEMORY FIX: Force garbage collection after clearing RAM cache
            if keys_to_remove:
                gc.collect()
                logger.debug(f"[ChunkCache] Client {self.client_id}: Forced GC after clearing {len(keys_to_remove)} RAM chunks")

        except Exception as e:
            logger.error(f"[ChunkCache] Client {self.client_id}: Error during cleanup: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self.cache_lock:
            ram_chunks = len(self.ram_cache)
            ram_size_mb = self.ram_cache_size / (1024 * 1024)
        
        # Count disk files
        disk_files = 0
        try:
            disk_files = len([f for f in os.listdir(self.cache_dir) if f.endswith('.dat')])
        except:
            pass
        
        hit_rate = 0.0
        total_requests = sum([self.stats['ram_hits'], self.stats['disk_hits'], self.stats['misses']])
        if total_requests > 0:
            hit_rate = (self.stats['ram_hits'] + self.stats['disk_hits']) / total_requests * 100
        
        return {
            'client_id': self.client_id,
            'ram_chunks': ram_chunks,
            'ram_size_mb': round(ram_size_mb, 2),
            'disk_files': disk_files,
            'hit_rate_percent': round(hit_rate, 1),
            **self.stats
        }
    
    def close(self):
        """Shutdown cache system"""
        logger.info(f"[ChunkCache] Client {self.client_id}: Shutting down cache system...")

        # Shutdown async spool executor
        self.spool_executor.shutdown(wait=True)

        # Log final statistics
        stats = self.get_stats()
        logger.info(f"[ChunkCache] Client {self.client_id}: Final stats: {stats}")

        # 🔧 MEMORY LEAK FIX: Clear RAM cache to release chunk data references
        with self.cache_lock:
            cache_count = len(self.ram_cache)
            cache_size_mb = self.ram_cache_size / (1024 * 1024)
            self.ram_cache.clear()
            self.ram_cache_size = 0
            if cache_count > 0:
                logger.info(f"[ChunkCache] Client {self.client_id}: 🔧 Cleared RAM cache - {cache_count} chunks ({cache_size_mb:.2f} MB)")

        # 🔧 MEMORY FIX: Force garbage collection after closing
        gc.collect()
        logger.debug(f"[ChunkCache] Client {self.client_id}: Forced GC after cache shutdown")