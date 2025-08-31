#!/usr/bin/env python3
"""
Script to fix all round table references to use per-round database files
"""
import re

def fix_chunk_manager_references():
    with open('/oracle-data/dtg/Naicheng_Li/FLTorrent/federatedscope/core/chunk_manager.py', 'r') as f:
        content = f.read()
    
    # Pattern replacements for per-round database migration
    replacements = [
        # Remove table name variables and f-strings
        (r'metadata_table = f"chunk_metadata_r{round_num}"', ''),
        (r'data_table = f"chunk_data_r{round_num}"', ''),
        (r'bt_table = f"bt_chunks_r{round_num}"', ''),
        (r'session_table = f"bt_sessions_r{round_num}"', ''),
        
        # Replace f-string table references with direct table names
        (r'FROM {metadata_table}', 'FROM chunk_metadata'),
        (r'FROM {data_table}', 'FROM chunk_data'),
        (r'FROM {bt_table}', 'FROM bt_chunks'),
        (r'FROM {session_table}', 'FROM bt_sessions'),
        (r'INTO {metadata_table}', 'INTO chunk_metadata'),
        (r'INTO {data_table}', 'INTO chunk_data'),
        (r'INTO {bt_table}', 'INTO bt_chunks'),
        (r'INTO {session_table}', 'INTO bt_sessions'),
        (r'UPDATE {session_table}', 'UPDATE bt_sessions'),
        (r'idx_{metadata_table}_hash', 'idx_chunk_metadata_hash'),
        (r'idx_{data_table}_hash', 'idx_chunk_data_hash'),
        (r'idx_{bt_table}_hash', 'idx_bt_chunks_hash'),
        
        # Replace connection calls to use round_num parameter
        (r'conn = self\._get_optimized_connection\(\)\s*\n\s*cursor = conn\.cursor\(\)', 
         'conn = self._get_optimized_connection(round_num=round_num)\n        cursor = conn.cursor()'),
        
        # Remove table existence checks since each round has its own DB
        (r'# Check if round tables? exist.*?\n.*?cursor\.execute\("SELECT name FROM sqlite_master.*?\n.*?if not cursor\.fetchone\(\):.*?\n.*?.*?\n.*?return.*?\n', ''),
        (r'cursor\.execute\("SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\?", \(.*?\)\)\s*\n\s*if cursor\.fetchone\(\):', '# Round database exists, proceed'),
        
        # Fix specific function patterns
        (r'metadata_tables = \[f"chunk_metadata_r{round_num}"\]', 'metadata_tables = ["chunk_metadata"]'),
        (r'data_tables = \[f"chunk_data_r{round_num}"\]', 'data_tables = ["chunk_data"]'),
    ]
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    
    # Write back the fixed content
    with open('/oracle-data/dtg/Naicheng_Li/FLTorrent/federatedscope/core/chunk_manager.py', 'w') as f:
        f.write(content)
    
    print("✅ Fixed all round table references in chunk_manager.py")

if __name__ == '__main__':
    fix_chunk_manager_references()