#!/usr/bin/env python3
"""
SSL证书修复脚本 - 用于解决CIFAR-10等外部数据集下载问题
"""

import ssl
import os

def fix_ssl_certificate():
    """
    修复SSL证书验证问题，允许下载外部数据集
    """
    # 方法1: 禁用SSL证书验证（仅用于下载数据集）
    ssl._create_default_https_context = ssl._create_unverified_context
    
    # 方法2: 设置环境变量
    os.environ['CURL_CA_BUNDLE'] = ''
    os.environ['REQUESTS_CA_BUNDLE'] = ''
    
    print("✅ SSL证书验证已禁用，可以下载外部数据集")

if __name__ == "__main__":
    fix_ssl_certificate()