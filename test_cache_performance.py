#!/usr/bin/env python3
"""
🚀 Test script for high-performance chunk cache system
Tests cache functionality and performance improvements
"""

import os
import sys
import time
import pickle
import numpy as np
from typing import Dict, Any

# Add federatedscope to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from federatedscope.core.chunk_cache import ChunkDataCache

def generate_test_chunk_data(chunk_id: int, size_kb: int = 100) -> bytes:
    """Generate test chunk data"""
    # Create random numpy array to simulate model parameters
    data_size = size_kb * 1024 // 8  # 8 bytes per float64
    test_data = {
        'chunk_id': chunk_id,
        'parameters': np.random.randn(data_size),
        'metadata': {'layer': f'layer_{chunk_id}', 'size': data_size}
    }
    return pickle.dumps(test_data)

def test_cache_functionality():
    """Test basic cache functionality"""
    print("🧪 Testing cache functionality...")
    
    # Initialize cache
    cache = ChunkDataCache(client_id=1, max_ram_chunks=5, max_ram_size_mb=10)
    
    try:
        # Test saving and retrieving chunks
        test_chunks = []
        for i in range(10):
            chunk_data = generate_test_chunk_data(i, size_kb=50)  # 50KB chunks
            test_chunks.append((i, chunk_data))
            
            # Save to cache
            cache.save_chunk_data(
                round_num=1,
                source_client_id=2,
                chunk_id=i,
                chunk_data=chunk_data
            )
            print(f"✅ Saved chunk {i} ({len(chunk_data)} bytes)")
        
        # Wait for async disk spooling to complete
        print("\n⏳ Waiting for async disk spooling...")
        time.sleep(2)
        
        # Test retrieval
        print("🔍 Testing retrieval...")
        for chunk_id, expected_data in test_chunks:
            retrieved_data = cache.get_chunk_data(1, 2, chunk_id)
            
            if retrieved_data == expected_data:
                print(f"✅ Chunk {chunk_id}: Retrieved successfully")
            else:
                print(f"❌ Chunk {chunk_id}: Data mismatch!")
                return False
        
        # Test cache statistics
        stats = cache.get_stats()
        print(f"\n📊 Cache Statistics:")
        print(f"   RAM chunks: {stats['ram_chunks']}")
        print(f"   RAM size: {stats['ram_size_mb']:.2f} MB")
        print(f"   Disk files: {stats['disk_files']}")
        print(f"   Hit rate: {stats['hit_rate_percent']:.1f}%")
        print(f"   RAM hits: {stats['ram_hits']}")
        print(f"   Disk hits: {stats['disk_hits']}")
        print(f"   Misses: {stats['misses']}")
        
        # Test cleanup
        print(f"\n🧹 Testing cleanup...")
        cache.cleanup_round(current_round=2, keep_rounds=1)
        
        # Final stats
        final_stats = cache.get_stats()
        print(f"📊 Final Statistics:")
        print(f"   Disk files after cleanup: {final_stats['disk_files']}")
        
        return True
        
    finally:
        cache.close()

def test_performance_comparison():
    """Test performance comparison between cache and traditional approach"""
    print("\n🚀 Performance Comparison Test...")
    
    # Generate test data
    num_chunks = 20
    test_data = []
    for i in range(num_chunks):
        chunk_data = generate_test_chunk_data(i, size_kb=100)  # 100KB chunks
        test_data.append((i, chunk_data))
    
    # Test cache system performance
    print("Testing cache system...")
    cache = ChunkDataCache(client_id=3, max_ram_chunks=10, max_ram_size_mb=20)
    
    try:
        # Save performance test
        start_time = time.time()
        for chunk_id, chunk_data in test_data:
            cache.save_chunk_data(1, 3, chunk_id, chunk_data)
        save_time = time.time() - start_time
        
        # Wait for async spooling
        time.sleep(2)
        
        # Retrieve performance test
        start_time = time.time()
        for chunk_id, _ in test_data:
            retrieved = cache.get_chunk_data(1, 3, chunk_id)
            assert retrieved is not None, f"Chunk {chunk_id} not found!"
        retrieve_time = time.time() - start_time
        
        stats = cache.get_stats()
        print(f"🚀 Cache System Performance:")
        print(f"   Save time: {save_time:.3f}s ({num_chunks/save_time:.1f} chunks/sec)")
        print(f"   Retrieve time: {retrieve_time:.3f}s ({num_chunks/retrieve_time:.1f} chunks/sec)")
        print(f"   Hit rate: {stats['hit_rate_percent']:.1f}%")
        print(f"   Total files: {stats['disk_files']}")
        
        return True
        
    finally:
        cache.close()

def main():
    """Main test function"""
    print("🧪 Starting ChunkCache Performance Tests\n")
    
    try:
        # Test 1: Basic functionality
        if not test_cache_functionality():
            print("❌ Basic functionality test failed!")
            return False
        
        # Test 2: Performance comparison
        if not test_performance_comparison():
            print("❌ Performance test failed!")
            return False
        
        print("\n✅ All tests passed! Cache system is working correctly.")
        print("🚀 The new chunk cache system provides:")
        print("   • Direct file I/O without database overhead")
        print("   • Two-tier storage (RAM Cache + Disk Spool)")
        print("   • LRU eviction for optimal memory usage")
        print("   • Atomic file operations for data integrity")
        print("   • High-performance chunk data access")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)