#!/usr/bin/env python3
"""
Test script to verify database optimization improvements
"""

import sys
import os
import sqlite3
import time
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project path
sys.path.append('/oracle-data/dtg/Naicheng_Li/FLTorrent')

try:
    from federatedscope.core.chunk_manager import ChunkManager, create_optimized_connection, db_retry_on_lock
    print("✅ Successfully imported optimized ChunkManager")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

def test_unified_connection():
    """Test unified database connection with PRAGMA settings"""
    print("\n=== Testing Unified Database Connection ===")
    
    db_path = "/tmp/test_unified_connection.db"
    
    # Clean up
    if os.path.exists(db_path):
        os.remove(db_path)
    
    try:
        conn = create_optimized_connection(db_path)
        cursor = conn.cursor()
        
        # Check PRAGMA settings
        pragmas_to_check = [
            "journal_mode", "synchronous", "cache_size", 
            "temp_store", "busy_timeout", "wal_autocheckpoint"
        ]
        
        print("PRAGMA Settings:")
        for pragma in pragmas_to_check:
            cursor.execute(f"PRAGMA {pragma}")
            value = cursor.fetchone()[0]
            print(f"  {pragma}: {value}")
        
        conn.close()
        print("✅ Unified connection test passed")
        
    except Exception as e:
        print(f"❌ Unified connection test failed: {e}")
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)

@db_retry_on_lock(max_retries=3, base_delay=0.1, max_delay=1.0)
def test_concurrent_write(thread_id, iterations=10):
    """Test concurrent database writes with retry decorator"""
    db_path = "/tmp/test_concurrent_writes.db"
    
    try:
        conn = create_optimized_connection(db_path)
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS test_data (
                id INTEGER PRIMARY KEY,
                thread_id INTEGER,
                data TEXT,
                timestamp REAL
            )
        ''')
        
        # Perform writes
        for i in range(iterations):
            cursor.execute('''
                INSERT INTO test_data (thread_id, data, timestamp)
                VALUES (?, ?, ?)
            ''', (thread_id, f"data_{thread_id}_{i}", time.time()))
            
            # Small random delay to increase contention
            time.sleep(random.uniform(0.001, 0.01))
        
        conn.commit()
        conn.close()
        return f"Thread {thread_id}: {iterations} writes completed"
        
    except Exception as e:
        return f"Thread {thread_id}: Failed - {e}"

def test_concurrent_database_access():
    """Test concurrent database access with retry mechanism"""
    print("\n=== Testing Concurrent Database Access ===")
    
    db_path = "/tmp/test_concurrent_writes.db"
    
    # Clean up
    if os.path.exists(db_path):
        os.remove(db_path)
    
    num_threads = 10
    iterations_per_thread = 5
    
    print(f"Testing {num_threads} concurrent threads, {iterations_per_thread} writes each...")
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(test_concurrent_write, i, iterations_per_thread) 
            for i in range(num_threads)
        ]
        
        results = []
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(f"  {result}")
    
    end_time = time.time()
    
    # Verify results
    try:
        conn = create_optimized_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_data")
        total_records = cursor.fetchone()[0]
        conn.close()
        
        expected_records = num_threads * iterations_per_thread
        
        print(f"\n📊 Results:")
        print(f"  Expected records: {expected_records}")
        print(f"  Actual records: {total_records}")
        print(f"  Success rate: {total_records/expected_records*100:.1f}%")
        print(f"  Total time: {end_time - start_time:.2f}s")
        
        if total_records == expected_records:
            print("✅ Concurrent access test passed")
        else:
            print("⚠️ Some records may have been lost")
            
    except Exception as e:
        print(f"❌ Verification failed: {e}")
    
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)

def test_chunk_manager_integration():
    """Test ChunkManager with optimized database access"""
    print("\n=== Testing ChunkManager Integration ===")
    
    try:
        # Create a test ChunkManager instance
        chunk_manager = ChunkManager(client_id=999)  # Test client
        
        print("✅ ChunkManager created successfully")
        
        # Test database connection
        conn = chunk_manager._get_optimized_connection()
        cursor = conn.cursor()
        
        # Check if WAL mode is enabled
        cursor.execute("PRAGMA journal_mode")
        journal_mode = cursor.fetchone()[0]
        print(f"  Journal mode: {journal_mode}")
        
        if journal_mode.upper() == 'WAL':
            print("✅ WAL mode is enabled")
        else:
            print("❌ WAL mode is not enabled")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ ChunkManager integration test failed: {e}")

def main():
    """Run all database optimization tests"""
    print("🚀 Database Optimization Test Suite")
    print("=" * 50)
    
    # Run tests
    test_unified_connection()
    test_concurrent_database_access() 
    test_chunk_manager_integration()
    
    print("\n🎉 Test Suite Completed")

if __name__ == "__main__":
    main()