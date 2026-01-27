#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全局常量定义 - 统一管理系统中的常量值
"""

# 论坛板块映射 (fid -> 名称)
SECTION_MAP = {
    '2': "国产原创",
    '36': "亚洲无码原创",
    '38': "欧美无码",
    '37': "亚洲有码原创",
    '103': "高清中文字幕",
    '104': "素人有码系列",
    '151': "4K原版",
    '152': "韩国主播",
    '160': "VR视频区",
    '39': "动漫原创",
    '107': "三级写真"
}

# 反向映射 (名称 -> fid)
SECTION_NAME_TO_FID = {v: k for k, v in SECTION_MAP.items()}

# 所有有效的 FID 集合
VALID_FIDS = set(SECTION_MAP.keys())

# 时间范围映射 (用于爬虫过滤)
DATELINE_MAP = {
    '1day': 86400,
    '2day': 172800,
    '3day': 259200,
    '1week': 604800,
    '1month': 2592000,
    '3month': 7776000,
    '6month': 15552000,
    '1year': 31536000,
}

# 爬虫模式
class CrawlerMode:
    SYNC = 'sync'
    THREAD = 'thread'
    ASYNC = 'async'

# 任务状态
class TaskStatus:
    PENDING = 'pending'
    RUNNING = 'running'
    PAUSED = 'paused'
    STOPPED = 'stopped'
    COMPLETED = 'completed'
    ERROR = 'error'
