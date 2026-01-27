#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志缓冲处理器 - 在内存中保留最近的日志用于 Web UI 查看
"""

import logging
import time
import threading
from collections import deque
from typing import Optional

# 全局日志缓冲区（初始默认值，会被配置项覆盖）
logs_buffer = None

# 最后清理时间
_last_cleanup_time = 0

# 清理锁
_cleanup_lock = threading.Lock()

# 清理间隔（秒）- 每小时清理一次
_CLEANUP_INTERVAL = 3600

# 缓冲区清理阈值（达到此大小时触发清理）
_CLEANUP_THRESHOLD = 0.8  # 当缓冲区使用率达到80%时触发清理

def _get_buffer_size() -> int:
    """
    获取日志缓冲区大小配置

    Returns:
        int: 缓冲区大小，范围在1000-100000之间
    """
    try:
        from configuration import config_manager
        size = config_manager.get('LOG_BUFFER_SIZE', 10000)
        # 确保是有效的整数
        return max(1000, min(int(size), 100000))  # 限制在1000-100000之间
    except:
        return 10000  # 默认值

def _should_cleanup() -> bool:
    """
    检查是否需要清理日志缓冲区

    Returns:
        bool: 如果需要清理返回True，否则返回False
    """
    global _last_cleanup_time

    now = time.time()
    buffer = _init_buffer()

    # 检查时间间隔
    if now - _last_cleanup_time < _CLEANUP_INTERVAL:
        return False

    # 检查缓冲区使用率
    if len(buffer) > 0:
        buffer_size = buffer.maxlen or _get_buffer_size()
        usage_rate = len(buffer) / buffer_size
        if usage_rate > _CLEANUP_THRESHOLD:
            return True

    return False

def _cleanup_buffer() -> None:
    """
    清理日志缓冲区，删除旧日志

    清理策略：保留最近的70%日志，删除旧的30%或至少100条
    """
    global _last_cleanup_time

    with _cleanup_lock:
        buffer = _init_buffer()

        if not buffer or len(buffer) == 0:
            return

        buffer_size = buffer.maxlen or _get_buffer_size()
        cleanup_count = max(100, int(buffer_size * 0.3))  # 清理30%或至少100条

        # 保留最近的日志，删除旧日志
        if len(buffer) > cleanup_count:
            for _ in range(cleanup_count):
                buffer.popleft()

        _last_cleanup_time = time.time()

        logger = logging.getLogger(__name__)
        logger.debug(f"[LOG] 日志缓冲区已清理 - 清理了 {cleanup_count} 条，剩余 {len(buffer)} 条")

def _init_buffer() -> deque:
    """
    初始化日志缓冲区（单例模式）

    Returns:
        deque: 日志缓冲区实例
    """
    global logs_buffer
    if logs_buffer is None:
        buffer_size = _get_buffer_size()
        logs_buffer = deque(maxlen=buffer_size)
        logger = logging.getLogger(__name__)
        logger.debug(f"[LOG] 日志缓冲区已初始化 - 大小: {buffer_size} 条")

        # 初始化清理时间
        global _last_cleanup_time
        _last_cleanup_time = time.time()
    return logs_buffer

class LogBufferHandler(logging.Handler):
    """
    日志缓冲处理器 - 捕获日志到内存缓冲区

    继承自 logging.Handler，用于在内存中保留最近的日志记录
    """

    def emit(self, record: logging.LogRecord) -> None:
        """
        发送日志记录到缓冲区

        Args:
            record: 日志记录对象
        """
        try:
            msg = self.format(record)
            buffer = _init_buffer()

            # 检查是否需要清理
            if _should_cleanup():
                _cleanup_buffer()

            buffer.append(msg)
        except Exception:
            pass

# 全局处理器实例
log_buffer_handler = None

def setup_log_buffer_handler() -> Optional['LogBufferHandler']:
    """
    设置日志缓冲处理器（仅设置一次）

    Returns:
        Optional[LogBufferHandler]: 日志缓冲处理器实例，如果已设置则返回None
    """
    global log_buffer_handler

    if log_buffer_handler is not None:
        return log_buffer_handler

    # 初始化缓冲区
    _init_buffer()

    root_logger = logging.getLogger()

    # 检查是否已存在 LogBufferHandler
    buffer_handler_exists = any(isinstance(h, LogBufferHandler) for h in root_logger.handlers)

    if not buffer_handler_exists:
        log_buffer_handler = LogBufferHandler()
        # 使用与start.py相同的格式，避免格式不一致导致的重复日志
        log_buffer_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        root_logger.addHandler(log_buffer_handler)
        logger = logging.getLogger(__name__)
        buffer_size = _get_buffer_size()
        logger.debug(f"[LOG] 日志缓冲处理器已添加 - 缓冲区大小: {buffer_size} 条")

    return log_buffer_handler

def get_logs_buffer() -> deque:
    """
    获取日志缓冲区

    Returns:
        deque: 日志缓冲区实例
    """
    return _init_buffer()

def cleanup_logs_buffer() -> None:
    """
    手动触发日志缓冲区清理

    用于外部主动触发清理操作，例如在配置变更时
    """
    _cleanup_buffer()
