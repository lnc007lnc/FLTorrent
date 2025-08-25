#!/usr/bin/env python3
"""
调试重要度分数获取：检查BitTorrent管理器中的重要度分数传递是否正确
"""

import sqlite3
import os

def debug_importance_scores():
    """调试重要度分数的存储和传递"""
    
    print("🔍 调试重要度分数传递机制")
    print("=" * 60)
    
    # 检查日志文件中的重要度信息
    log_files = [
        "multi_process_test_v2/logs/server.log",
        "multi_process_test_v2/logs/client_1.log",
        "multi_process_test_v2/logs/client_2.log", 
        "multi_process_test_v2/logs/client_3.log"
    ]
    
    for log_file in log_files:
        if os.path.exists(log_file):
            print(f"\n📋 检查日志文件: {log_file}")
            
            # 查找重要度相关的日志
            with open(log_file, 'r') as f:
                lines = f.readlines()
            
            importance_logs = []
            bitfield_logs = []
            have_logs = []
            selection_logs = []
            
            for line in lines:
                if 'importance' in line.lower():
                    importance_logs.append(line.strip())
                if 'bitfield' in line.lower() and ('send' in line.lower() or 'broadcast' in line.lower()):
                    bitfield_logs.append(line.strip())
                if 'have' in line.lower() and ('send' in line.lower() or 'broadcast' in line.lower()):
                    have_logs.append(line.strip())
                if 'selected chunk' in line.lower():
                    selection_logs.append(line.strip())
            
            print(f"  📊 重要度日志: {len(importance_logs)}条")
            if importance_logs:
                for i, log in enumerate(importance_logs[:5]):  # 显示前5条
                    print(f"    {i+1}: {log}")
                if len(importance_logs) > 5:
                    print(f"    ... 还有{len(importance_logs)-5}条")
            
            print(f"  📡 Bitfield日志: {len(bitfield_logs)}条")
            if bitfield_logs:
                for i, log in enumerate(bitfield_logs[:3]):
                    print(f"    {i+1}: {log}")
            
            print(f"  📨 Have日志: {len(have_logs)}条")
            if have_logs:
                for i, log in enumerate(have_logs[:3]):
                    print(f"    {i+1}: {log}")
                    
            print(f"  🎯 选择日志: {len(selection_logs)}条")
            if selection_logs:
                for i, log in enumerate(selection_logs[:3]):
                    print(f"    {i+1}: {log}")
        else:
            print(f"❌ 日志文件不存在: {log_file}")
    
    # 检查数据库中的连接日志
    print(f"\n📡 检查连接日志...")
    connection_log = "connection_logs/connection_messages.jsonl"
    if os.path.exists(connection_log):
        print(f"✅ 连接日志存在: {connection_log}")
        
        import json
        with open(connection_log, 'r') as f:
            lines = f.readlines()
        
        print(f"  📊 连接消息数: {len(lines)}")
        
        # 分析连接消息类型
        message_types = {}
        for line in lines:
            try:
                msg = json.loads(line.strip())
                msg_type = msg.get('message_type', 'unknown')
                message_types[msg_type] = message_types.get(msg_type, 0) + 1
            except:
                pass
        
        print(f"  📋 消息类型分布: {message_types}")
        
        # 查找bitfield和have消息
        bitfield_messages = []
        have_messages = []
        
        for line in lines[-20:]:  # 检查最后20条消息
            try:
                msg = json.loads(line.strip())
                if msg.get('message_type') == 'bitfield':
                    bitfield_messages.append(msg)
                elif msg.get('message_type') == 'have':
                    have_messages.append(msg)
            except:
                pass
        
        print(f"  📡 最近bitfield消息: {len(bitfield_messages)}条")
        if bitfield_messages:
            sample_msg = bitfield_messages[0]
            print(f"    示例: {sample_msg}")
        
        print(f"  📨 最近have消息: {len(have_messages)}条")
        if have_messages:
            sample_msg = have_messages[0]
            print(f"    示例: {sample_msg}")
    else:
        print(f"❌ 连接日志不存在: {connection_log}")
    
    # 总结分析
    print(f"\n{'='*60}")
    print("💡 调试分析总结")
    print("="*60)
    print("需要检查的关键点：")
    print("1. BitTorrent消息中是否包含importance字段")
    print("2. 接收方是否正确解析和保存importance分数")
    print("3. 选择算法是否使用了正确的importance分数")
    print("4. 日志中是否有'Selected chunk ... with importance'的记录")

if __name__ == "__main__":
    debug_importance_scores()