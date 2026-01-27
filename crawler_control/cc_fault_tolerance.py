#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
容错和重试管理器 - 处理系统故障和降级

负责：
- 超时检测和处理
- 重试机制（指数退避）
- 降级方案管理
- 故障恢复
"""

import time
import logging
from typing import Callable, Any, Optional, Dict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class FallbackStrategy(Enum):
    """降级策略枚举"""
    MEMORY = "memory"  # 使用内存状态
    LOCAL = "local"  # 使用本地文件
    CACHE = "cache"  # 使用缓存
    NONE = "none"  # 无降级


@dataclass
class RetryConfig:
    """重试配置"""
    max_attempts: int = 3  # 最大重试次数
    initial_delay: float = 0.5  # 初始延迟（秒）
    max_delay: float = 5.0  # 最大延迟（秒）
    exponential_base: float = 2.0  # 指数退避基数
    timeout: float = 10.0  # 超时时间（秒）


class FaultToleranceManager:
    """容错管理器"""
    
    def __init__(self, retry_config: Optional[RetryConfig] = None):
        """
        初始化容错管理器
        
        Args:
            retry_config: 重试配置，默认使用标准配置
        """
        self.retry_config = retry_config or RetryConfig()
        self._fallback_handlers: Dict[str, Callable] = {}
        self._stats = {
            'total_operations': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'retried_operations': 0,
            'fallback_operations': 0,
            'timeout_operations': 0
        }
        logger.info("FaultToleranceManager initialized")
    
    def register_fallback(self, operation_name: str, fallback_handler: Callable) -> bool:
        """
        注册降级处理器
        
        Args:
            operation_name: 操作名称
            fallback_handler: 降级处理函数
            
        Returns:
            bool: 是否成功注册
        """
        try:
            self._fallback_handlers[operation_name] = fallback_handler
            logger.info(f"Registered fallback handler for: {operation_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to register fallback handler: {e}")
            return False
    
    def execute_with_retry(self, 
                          operation: Callable[[], Any],
                          operation_name: str = "unknown",
                          timeout: Optional[float] = None,
                          max_attempts: Optional[int] = None) -> tuple[bool, Any]:
        """
        执行操作并在失败时重试
        
        Args:
            operation: 要执行的操作函数
            operation_name: 操作名称（用于日志）
            timeout: 超时时间（秒），None使用默认配置
            max_attempts: 最大重试次数，None使用默认配置
            
        Returns:
            tuple: (是否成功, 结果或错误)
        """
        self._stats['total_operations'] += 1
        
        timeout = timeout or self.retry_config.timeout
        max_attempts = max_attempts or self.retry_config.max_attempts
        
        start_time = time.time()
        last_error = None
        
        for attempt in range(max_attempts):
            try:
                # 检查是否已经超时
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    self._stats['timeout_operations'] += 1
                    logger.warning(f"Operation '{operation_name}' timed out after {elapsed:.2f}s")
                    return False, f"Timeout after {elapsed:.2f}s"
                
                # 计算剩余时间
                remaining_time = timeout - elapsed
                
                # 执行操作（带超时检测）
                logger.debug(f"Executing '{operation_name}' (attempt {attempt + 1}/{max_attempts})")
                
                # 简单的超时检测：在操作执行前记录时间
                operation_start = time.time()
                result = operation()
                operation_elapsed = time.time() - operation_start
                
                # 检查操作是否超时
                total_elapsed = time.time() - start_time
                if total_elapsed >= timeout:
                    self._stats['timeout_operations'] += 1
                    logger.warning(f"Operation '{operation_name}' exceeded timeout during execution")
                    return False, f"Timeout after {total_elapsed:.2f}s"
                
                # 成功
                self._stats['successful_operations'] += 1
                if attempt > 0:
                    self._stats['retried_operations'] += 1
                    logger.info(f"Operation '{operation_name}' succeeded after {attempt + 1} attempts")
                
                return True, result
                
            except Exception as e:
                last_error = e
                logger.warning(f"Operation '{operation_name}' failed (attempt {attempt + 1}/{max_attempts}): {e}")
                
                # 如果还有重试机会，等待后重试
                if attempt < max_attempts - 1:
                    delay = self._calculate_backoff_delay(attempt)
                    
                    # 确保不会超过总超时时间
                    elapsed = time.time() - start_time
                    remaining_time = timeout - elapsed
                    if remaining_time <= 0:
                        self._stats['timeout_operations'] += 1
                        logger.warning(f"Operation '{operation_name}' timed out before retry")
                        break
                    
                    actual_delay = min(delay, remaining_time)
                    logger.debug(f"Waiting {actual_delay:.2f}s before retry")
                    time.sleep(actual_delay)
        
        # 所有重试都失败
        self._stats['failed_operations'] += 1
        logger.error(f"Operation '{operation_name}' failed after {max_attempts} attempts: {last_error}")
        return False, last_error
    
    def execute_with_fallback(self,
                             operation: Callable[[], Any],
                             operation_name: str,
                             fallback_strategy: FallbackStrategy = FallbackStrategy.MEMORY,
                             timeout: Optional[float] = None) -> tuple[bool, Any, bool]:
        """
        执行操作，失败时使用降级方案
        
        Args:
            operation: 主操作函数
            operation_name: 操作名称
            fallback_strategy: 降级策略
            timeout: 超时时间
            
        Returns:
            tuple: (是否成功, 结果, 是否使用了降级)
        """
        # 先尝试正常执行
        success, result = self.execute_with_retry(operation, operation_name, timeout)
        
        if success:
            return True, result, False
        
        # 如果失败，尝试降级
        if operation_name in self._fallback_handlers:
            try:
                logger.info(f"Executing fallback for '{operation_name}' with strategy: {fallback_strategy.value}")
                fallback_result = self._fallback_handlers[operation_name]()
                self._stats['fallback_operations'] += 1
                logger.info(f"Fallback succeeded for '{operation_name}'")
                return True, fallback_result, True
            except Exception as e:
                logger.error(f"Fallback failed for '{operation_name}': {e}")
                return False, e, True
        else:
            logger.warning(f"No fallback handler registered for '{operation_name}'")
            return False, result, False
    
    def _calculate_backoff_delay(self, attempt: int) -> float:
        """
        计算指数退避延迟
        
        Args:
            attempt: 当前尝试次数（从0开始）
            
        Returns:
            float: 延迟时间（秒）
        """
        delay = self.retry_config.initial_delay * (self.retry_config.exponential_base ** attempt)
        return min(delay, self.retry_config.max_delay)
    
    def check_timeout(self, start_time: float, timeout: Optional[float] = None) -> bool:
        """
        检查操作是否超时
        
        Args:
            start_time: 操作开始时间
            timeout: 超时时间（秒），None使用默认配置
            
        Returns:
            bool: 是否超时
        """
        timeout = timeout or self.retry_config.timeout
        elapsed = time.time() - start_time
        return elapsed >= timeout
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        Returns:
            dict: 统计数据
        """
        stats = self._stats.copy()
        if stats['total_operations'] > 0:
            stats['success_rate'] = (stats['successful_operations'] / stats['total_operations']) * 100
            stats['failure_rate'] = (stats['failed_operations'] / stats['total_operations']) * 100
            stats['fallback_rate'] = (stats['fallback_operations'] / stats['total_operations']) * 100
        else:
            stats['success_rate'] = 0
            stats['failure_rate'] = 0
            stats['fallback_rate'] = 0
        
        return stats
    
    def reset_stats(self):
        """重置统计信息"""
        self._stats = {
            'total_operations': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'retried_operations': 0,
            'fallback_operations': 0,
            'timeout_operations': 0
        }
        logger.info("Statistics reset")


# 全局容错管理器实例
_fault_tolerance_manager = None


def get_fault_tolerance_manager() -> FaultToleranceManager:
    """
    获取全局容错管理器实例（单例模式）
    
    Returns:
        FaultToleranceManager: 容错管理器实例
    """
    global _fault_tolerance_manager
    
    if _fault_tolerance_manager is None:
        _fault_tolerance_manager = FaultToleranceManager()
    
    return _fault_tolerance_manager
