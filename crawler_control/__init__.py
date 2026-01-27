#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫控制模块 - 新的控制信号和状态管理架构

包含：
- 信号队列管理
- 状态协调和状态机
- 事件循环和信号检查
- 控制桥接器
- 资源清理管理
- 容错和重试机制
- 日志记录
"""

# 核心组件
from .cc_signal_queue import SignalQueueManager, Signal
from .cc_state_coordinator import StateCoordinator, CrawlerState, ControlAction
from .cc_state_machine import CrawlerStateMachine
from .cc_event_loop import EnhancedEventLoop
from .cc_control_bridge import CrawlerControlBridge, get_crawler_control_bridge

# 辅助组件
from .cc_resource_cleanup import (
    ResourceCleanupManager, 
    ResourceType, 
    Resource,
    get_resource_cleanup_manager
)
from .cc_fault_tolerance import (
    FaultToleranceManager,
    RetryConfig,
    FallbackStrategy,
    get_fault_tolerance_manager
)
from .cc_logger import (
    ControlLogger,
    LogLevel,
    LogCategory,
    ControlLogEntry,
    get_control_logger
)

__all__ = [
    # 信号管理
    'SignalQueueManager',
    'Signal',
    
    # 状态管理
    'StateCoordinator',
    'CrawlerState',
    'ControlAction',
    'CrawlerStateMachine',
    
    # 事件循环
    'EnhancedEventLoop',
    
    # 控制桥接
    'CrawlerControlBridge',
    'get_crawler_control_bridge',
    
    # 资源管理
    'ResourceCleanupManager',
    'ResourceType',
    'Resource',
    'get_resource_cleanup_manager',
    
    # 容错机制
    'FaultToleranceManager',
    'RetryConfig',
    'FallbackStrategy',
    'get_fault_tolerance_manager',
    
    # 日志记录
    'ControlLogger',
    'LogLevel',
    'LogCategory',
    'ControlLogEntry',
    'get_control_logger',
]
