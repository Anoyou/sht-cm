#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同步/异步桥接层 (v1.3.0)
允许在同步代码（如Flask路由）中调用异步函数
"""

import asyncio
import concurrent.futures
from typing import TypeVar, Coroutine, Any
import logging

logger = logging.getLogger(__name__)

T = TypeVar('T')


def run_async(coro: Coroutine[Any, Any, T], timeout: float = 600.0) -> T:
    """
    在同步代码中运行异步函数，增加全局超时保护 (v1.4.7)
    """
    try:
        # 尝试获取当前事件循环
        loop = asyncio.get_running_loop()
        # 如果已在事件循环中，使用新线程
        with concurrent.futures.ThreadPoolExecutor() as pool:
            # 外部线程池增加超时
            return pool.submit(asyncio.run, coro).result(timeout=timeout)
    except (RuntimeError, concurrent.futures.TimeoutError) as e:
        if isinstance(e, concurrent.futures.TimeoutError):
            logger.error(f"🔴 [BRIDGE] 异步任务全局硬超时熔断 (>{timeout}s)")
            raise
        # 没有运行中的循环，直接运行
        try:
            return asyncio.run(asyncio.wait_for(coro, timeout=timeout))
        except asyncio.TimeoutError:
            logger.error(f"🔴 [BRIDGE] 异步任务初始化循环全局崩溃 (>{timeout}s)")
            raise
