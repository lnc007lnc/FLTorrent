#!/usr/bin/env python3
"""
🚀 Concurrent Access Test: ChunkManager Cache System
Tests concurrent access patterns and cache performance under load
"""

import os
import sys
import tempfile
import shutil
import threading
import time
import numpy as np
import torch
import torch.nn as nn
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to path
sys.path.insert(0, os.path.abspath('.'))

from federatedscope.core.chunk_manager import ChunkManager


class TestModel(nn.Module):
    """Test model for concurrent access"""
    def __init__(self, input_size=50):
        super(TestModel, self).__init__()
        self.fc1 = nn.Linear(input_size, 20)
        self.fc2 = nn.Linear(20, 10)
        self.fc3 = nn.Linear(10, 2)
    
    def forward(self, x):
        x = torch.relu(self.fc1(x))
        x = torch.relu(self.fc2(x))
        return self.fc3(x)


def worker_save_and_load(client_id: int, round_num: int, num_chunks: int, temp_dir: str):
    """Worker function for concurrent save/load operations"""
    try:
        # Change to temp directory
        old_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        # Create ChunkManager
        chunk_manager = ChunkManager(client_id=client_id)
        
        # Create and save model
        model = TestModel()
        
        start_time = time.time()
        chunk_hashes = chunk_manager.save_model_chunks(
            model=model,
            round_num=round_num,
            num_chunks=num_chunks,
            persist_to_db=False
        )
        save_time = time.time() - start_time
        
        # Load chunks
        start_time = time.time()
        loaded_chunks = chunk_manager.load_chunks_by_round(round_num)
        load_time = time.time() - start_time
        
        # Test individual chunk access
        start_time = time.time()
        chunk_access_count = 0
        for chunk_id in range(min(5, num_chunks)):  # Access first 5 chunks
            chunk_result = chunk_manager.get_chunk_by_id(round_num, chunk_id)
            if chunk_result is not None:
                chunk_access_count += 1
        access_time = time.time() - start_time
        
        # Get cache stats
        cache_stats = chunk_manager.chunk_cache.get_stats()
        
        # Cleanup
        chunk_manager.close()
        os.chdir(old_cwd)
        
        return {
            'client_id': client_id,
            'round_num': round_num,
            'chunks_saved': len(chunk_hashes),
            'chunks_loaded': len(loaded_chunks),
            'chunks_accessed': chunk_access_count,
            'save_time': save_time,
            'load_time': load_time,
            'access_time': access_time,
            'cache_hit_rate': cache_stats.get('hit_rate_percent', 0),
            'cache_ram_chunks': cache_stats.get('ram_chunks', 0),
            'success': True
        }
    
    except Exception as e:
        return {
            'client_id': client_id,
            'round_num': round_num,
            'error': str(e),
            'success': False
        }


def test_concurrent_cache_access():
    """Test concurrent cache access with multiple clients and rounds"""
    print("🚀 Testing concurrent cache access and performance...")
    
    # Create temporary directory for testing
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Test parameters
        num_clients = 3
        num_rounds = 2
        num_chunks = 5
        max_workers = 6
        
        print(f"📊 Test setup: {num_clients} clients × {num_rounds} rounds × {num_chunks} chunks")
        print(f"🔧 Concurrent workers: {max_workers}")
        
        # Create tasks for all client/round combinations
        tasks = []
        for client_id in range(1, num_clients + 1):
            for round_num in range(1, num_rounds + 1):
                tasks.append((client_id, round_num, num_chunks, temp_dir))
        
        print(f"📋 Created {len(tasks)} concurrent tasks")
        
        # Execute tasks concurrently
        results = []
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(worker_save_and_load, client_id, round_num, num_chunks, temp_dir): (client_id, round_num)
                for client_id, round_num, num_chunks, temp_dir in tasks
            }
            
            # Collect results
            for future in as_completed(futures):
                client_id, round_num = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    if result['success']:
                        print(f"✅ Client {client_id}, Round {round_num}: "
                              f"Saved {result['chunks_saved']}, "
                              f"Loaded {result['chunks_loaded']}, "
                              f"Hit rate: {result['cache_hit_rate']:.1f}%")
                    else:
                        print(f"❌ Client {client_id}, Round {round_num}: {result['error']}")
                except Exception as e:
                    print(f"❌ Client {client_id}, Round {round_num}: Exception - {e}")
                    results.append({'client_id': client_id, 'round_num': round_num, 'success': False, 'error': str(e)})
        
        total_time = time.time() - start_time
        
        # Analyze results
        successful_results = [r for r in results if r['success']]
        failed_results = [r for r in results if not r['success']]
        
        print(f"\n📈 Performance Analysis:")
        print(f"   Total execution time: {total_time:.2f}s")
        print(f"   Successful operations: {len(successful_results)}/{len(results)}")
        print(f"   Failed operations: {len(failed_results)}")
        
        if successful_results:
            avg_save_time = sum(r['save_time'] for r in successful_results) / len(successful_results)
            avg_load_time = sum(r['load_time'] for r in successful_results) / len(successful_results)
            avg_access_time = sum(r['access_time'] for r in successful_results) / len(successful_results)
            avg_hit_rate = sum(r['cache_hit_rate'] for r in successful_results) / len(successful_results)
            
            print(f"   Average save time: {avg_save_time:.3f}s")
            print(f"   Average load time: {avg_load_time:.3f}s")
            print(f"   Average access time: {avg_access_time:.3f}s")
            print(f"   Average hit rate: {avg_hit_rate:.1f}%")
            
            total_chunks_processed = sum(r['chunks_saved'] + r['chunks_loaded'] + r['chunks_accessed'] 
                                        for r in successful_results)
            print(f"   Total chunks processed: {total_chunks_processed}")
            print(f"   Throughput: {total_chunks_processed/total_time:.1f} chunks/sec")
        
        if failed_results:
            print(f"\n❌ Failed operations:")
            for result in failed_results:
                print(f"   Client {result['client_id']}, Round {result['round_num']}: {result['error']}")
        
        # Success criteria
        success_rate = len(successful_results) / len(results) if results else 0
        performance_acceptable = avg_hit_rate > 80.0 if successful_results else False
        
        print(f"\n🎯 Test Results:")
        print(f"   Success rate: {success_rate:.1%}")
        print(f"   Cache performance: {'✅ Good' if performance_acceptable else '⚠️  Needs improvement'}")
        
        overall_success = success_rate >= 0.8 and len(failed_results) == 0
        
        if overall_success:
            print("🎉 Concurrent cache access test PASSED!")
        else:
            print("❌ Concurrent cache access test FAILED!")
        
        return overall_success
        
    except Exception as e:
        print(f"❌ Test setup failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    success = test_concurrent_cache_access()
    exit(0 if success else 1)