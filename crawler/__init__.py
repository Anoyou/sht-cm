#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crawler 模块 - SHT 爬虫引擎

提供完整的爬虫功能，包括：
- HTML 解析
- 高性能多线程爬取
- 异步并发爬取
- 批量处理
- 防屏蔽机制

向后兼容：
    from crawler import SHT, FastCrawler, BatchProcessor, AsyncSHTCrawler
    from crawler.parser import extract_safeid, extract_and_convert_video_size
"""

# 导出解析函数
from .parser import (
    extract_and_convert_video_size,
    extract_safeid,
    extract_exact_datetime,
    extract_bracket_content
)

# 导出爬虫类
from .fast_crawler import FastCrawler
from .batch_processor import BatchProcessor
from .base import SHTBase

# 导出主爬虫类
from .sync_crawler import SHT

# 导出异步爬虫类
from .async_crawler import AsyncSHTCrawler

__all__ = [
    # 解析函数
    'extract_and_convert_video_size',
    'extract_safeid',
    'extract_exact_datetime',
    'extract_bracket_content',
    
    # 爬虫类
    'FastCrawler',
    'BatchProcessor',
    'SHTBase',
    'SHT',  # 主爬虫类
    'AsyncSHTCrawler',  # 异步爬虫类
]
