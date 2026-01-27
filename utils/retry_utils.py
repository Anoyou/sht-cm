#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTTP请求重试工具 - 提供带指数退避的重试策略
"""

import time
import logging
from typing import Callable, Any, Optional, List, Type
from functools import wraps

logger = logging.getLogger(__name__)


class RetryException(Exception):
    """重试异常基类"""
    pass


class MaxRetriesExceededError(RetryException):
    """超过最大重试次数异常"""
    pass


def retry_on_exception(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable] = None
) -> Callable:
    """
    带指数退避的重试装饰器

    Args:
        max_retries: 最大重试次数
        base_delay: 初始延迟时间（秒）
        max_delay: 最大延迟时间（秒）
        backoff_factor: 退避因子（每次失败后延迟时间乘以此因子）
        exceptions: 需要捕获的异常类型
        on_retry: 每次重试时的回调函数，接收 (attempt, exception) 参数

    Returns:
        装饰后的函数

    Example:
        @retry_on_exception(max_retries=3, base_delay=1.0)
        def fetch_data():
            requests.get('http://example.com')
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    # 最后一次尝试失败，不再重试
                    if attempt >= max_retries:
                        logger.error(
                            f"[RETRY] 达到最大重试次数 {max_retries}，放弃重试 - "
                            f"函数: {func.__name__}, 错误: {e}"
                        )
                        raise MaxRetriesExceededError(
                            f"达到最大重试次数 {max_retries}: {e}"
                        ) from e

                    # 计算退避时间
                    delay = min(base_delay * (backoff_factor ** attempt), max_delay)

                    logger.warning(
                        f"[RETRY] 第 {attempt + 1}/{max_retries} 次尝试失败 - "
                        f"函数: {func.__name__}, 错误: {e}, "
                        f"{delay:.1f}秒后重试..."
                    )

                    # 调用回调函数
                    if on_retry:
                        try:
                            on_retry(attempt + 1, e)
                        except Exception as cb_err:
                            logger.error(f"[RETRY] 回调函数执行失败: {cb_err}")

                    # 等待后重试
                    time.sleep(delay)

            # 理论上不会执行到这里
            raise last_exception

        return wrapper
    return decorator


def retry_request(
    request_func: Callable,
    max_retries: int = 3,
    timeout: int = 30,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    backoff_factor: float = 2.0,
    raise_on_fail: bool = True,
    **kwargs
) -> Optional[Any]:
    """
    重试HTTP请求的便捷函数

    Args:
        request_func: 请求函数（如 requests.get, requests.post）
        max_retries: 最大重试次数
        timeout: 超时时间（秒）
        base_delay: 初始延迟时间（秒）
        max_delay: 最大延迟时间（秒）
        backoff_factor: 退避因子
        raise_on_fail: 所有重试失败后是否抛出异常
        **kwargs: 传递给请求函数的其他参数

    Returns:
        请求响应对象，如果所有重试失败且 raise_on_fail=False 则返回 None

    Example:
        # 使用示例
        response = retry_request(
            requests.get,
            url='http://example.com',
            max_retries=3,
            timeout=10
        )
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            # 设置超时（如果没有指定）
            if 'timeout' not in kwargs:
                kwargs['timeout'] = timeout

            response = request_func(**kwargs)

            # 检查响应状态码
            if hasattr(response, 'status_code'):
                if 200 <= response.status_code < 300:
                    return response
                else:
                    # HTTP错误也可以重试（某些情况下）
                    if attempt < max_retries:
                        raise Exception(
                            f"HTTP {response.status_code}: {response.text[:200]}"
                        )

            return response

        except Exception as e:
            last_exception = e

            # 最后一次尝试失败
            if attempt >= max_retries:
                logger.error(
                    f"[RETRY] HTTP请求达到最大重试次数 {max_retries}，放弃重试 - "
                    f"错误: {e}"
                )
                if raise_on_fail:
                    raise MaxRetriesExceededError(
                        f"HTTP请求失败（重试{max_retries}次）: {e}"
                    ) from e
                return None

            # 计算退避时间
            delay = min(base_delay * (backoff_factor ** attempt), max_delay)

            logger.warning(
                f"[RETRY] HTTP请求第 {attempt + 1}/{max_retries} 次失败 - "
                f"错误: {e}, {delay:.1f}秒后重试..."
            )

            time.sleep(delay)

    # 理论上不会执行到这里
    if raise_on_fail:
        raise last_exception
    return None


# 预定义的重试配置
RETRY_CONFIG = {
    'telegram': {
        'max_retries': 3,
        'timeout': 10,
        'base_delay': 1.0,
        'max_delay': 10.0,
        'backoff_factor': 2.0
    },
    'proxy_check': {
        'max_retries': 2,
        'timeout': 10,
        'base_delay': 1.0,
        'max_delay': 5.0,
        'backoff_factor': 2.0
    },
    'health_check': {
        'max_retries': 2,
        'timeout': 3,
        'base_delay': 0.5,
        'max_delay': 3.0,
        'backoff_factor': 2.0
    },
    'sht2bm_api': {
        'max_retries': 3,
        'timeout': 30,
        'base_delay': 1.0,
        'max_delay': 15.0,
        'backoff_factor': 2.0
    }
}
