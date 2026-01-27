#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
控制操作日志记录器 - 专门记录爬虫控制操作的详细日志

负责：
- 记录控制信号的发送和接收
- 记录状态转换和同步
- 记录错误和异常
- 提供结构化的日志查询
"""

import logging
import json
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import deque
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)


class LogLevel(Enum):
    """日志级别枚举"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogCategory(Enum):
    """日志分类枚举"""
    SIGNAL = "signal"  # 信号相关
    STATE = "state"  # 状态相关
    ERROR = "error"  # 错误相关
    PERFORMANCE = "performance"  # 性能相关
    RESOURCE = "resource"  # 资源相关


@dataclass
class ControlLogEntry:
    """控制日志条目"""
    timestamp: datetime
    level: str
    category: str
    operation: str
    message: str
    metadata: Dict[str, Any]
    error_details: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> dict:
        """转换为字典格式"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: dict) -> 'ControlLogEntry':
        """从字典创建日志条目"""
        data = data.copy()
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)


class ControlLogger:
    """控制操作日志记录器"""
    
    def __init__(self, max_entries: int = 1000):
        """
        初始化控制日志记录器
        
        Args:
            max_entries: 最大日志条目数
        """
        self.max_entries = max_entries
        self._logs: deque = deque(maxlen=max_entries)
        self._stats = {
            'total_logs': 0,
            'by_level': {},
            'by_category': {},
            'errors': 0
        }
        logger.info("ControlLogger initialized")
    
    def log_signal_sent(self, signal_type: str, signal_id: str, metadata: Dict[str, Any] = None):
        """
        记录信号发送
        
        Args:
            signal_type: 信号类型
            signal_id: 信号ID
            metadata: 元数据
        """
        entry = ControlLogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO.value,
            category=LogCategory.SIGNAL.value,
            operation="signal_sent",
            message=f"Control signal sent: {signal_type}",
            metadata={
                'signal_type': signal_type,
                'signal_id': signal_id,
                **(metadata or {})
            }
        )
        self._add_entry(entry)
        logger.info(f"Signal sent: {signal_type} (ID: {signal_id})")
    
    def log_signal_received(self, signal_type: str, signal_id: str, 
                           detection_time: float, metadata: Dict[str, Any] = None):
        """
        记录信号接收
        
        Args:
            signal_type: 信号类型
            signal_id: 信号ID
            detection_time: 检测时间（秒）
            metadata: 元数据
        """
        entry = ControlLogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO.value,
            category=LogCategory.SIGNAL.value,
            operation="signal_received",
            message=f"Control signal received: {signal_type}",
            metadata={
                'signal_type': signal_type,
                'signal_id': signal_id,
                'detection_time_ms': detection_time * 1000,
                **(metadata or {})
            }
        )
        self._add_entry(entry)
        logger.info(f"Signal received: {signal_type} (ID: {signal_id}, detection: {detection_time*1000:.2f}ms)")
    
    def log_signal_processed(self, signal_type: str, signal_id: str, 
                            action: str, success: bool, metadata: Dict[str, Any] = None):
        """
        记录信号处理
        
        Args:
            signal_type: 信号类型
            signal_id: 信号ID
            action: 执行的动作
            success: 是否成功
            metadata: 元数据
        """
        level = LogLevel.INFO if success else LogLevel.WARNING
        entry = ControlLogEntry(
            timestamp=datetime.now(),
            level=level.value,
            category=LogCategory.SIGNAL.value,
            operation="signal_processed",
            message=f"Signal processed: {signal_type} -> {action} ({'success' if success else 'failed'})",
            metadata={
                'signal_type': signal_type,
                'signal_id': signal_id,
                'action': action,
                'success': success,
                **(metadata or {})
            }
        )
        self._add_entry(entry)
        logger.info(f"Signal processed: {signal_type} -> {action} ({'success' if success else 'failed'})")
    
    def log_state_transition(self, from_state: str, to_state: str, 
                            trigger: str, metadata: Dict[str, Any] = None):
        """
        记录状态转换
        
        Args:
            from_state: 原状态
            to_state: 新状态
            trigger: 触发原因
            metadata: 元数据
        """
        entry = ControlLogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO.value,
            category=LogCategory.STATE.value,
            operation="state_transition",
            message=f"State transition: {from_state} -> {to_state} (trigger: {trigger})",
            metadata={
                'from_state': from_state,
                'to_state': to_state,
                'trigger': trigger,
                **(metadata or {})
            }
        )
        self._add_entry(entry)
        logger.info(f"State transition: {from_state} -> {to_state}")
    
    def log_state_sync(self, state: str, version: int, metadata: Dict[str, Any] = None):
        """
        记录状态同步
        
        Args:
            state: 状态
            version: 版本号
            metadata: 元数据
        """
        entry = ControlLogEntry(
            timestamp=datetime.now(),
            level=LogLevel.DEBUG.value,
            category=LogCategory.STATE.value,
            operation="state_sync",
            message=f"State synchronized: {state} (version: {version})",
            metadata={
                'state': state,
                'version': version,
                **(metadata or {})
            }
        )
        self._add_entry(entry)
        logger.debug(f"State synchronized: {state} (v{version})")
    
    def log_error(self, operation: str, error: Exception, 
                 context: Dict[str, Any] = None):
        """
        记录错误
        
        Args:
            operation: 操作名称
            error: 异常对象
            context: 上下文信息
        """
        error_details = {
            'error_type': type(error).__name__,
            'error_message': str(error),
            'traceback': traceback.format_exc()
        }
        
        entry = ControlLogEntry(
            timestamp=datetime.now(),
            level=LogLevel.ERROR.value,
            category=LogCategory.ERROR.value,
            operation=operation,
            message=f"Error in {operation}: {str(error)}",
            metadata=context or {},
            error_details=error_details
        )
        self._add_entry(entry)
        self._stats['errors'] += 1
        logger.error(f"Error in {operation}: {error}", exc_info=True)
    
    def log_performance(self, operation: str, duration_ms: float, 
                       metadata: Dict[str, Any] = None):
        """
        记录性能指标
        
        Args:
            operation: 操作名称
            duration_ms: 持续时间（毫秒）
            metadata: 元数据
        """
        level = LogLevel.WARNING if duration_ms > 1000 else LogLevel.DEBUG
        entry = ControlLogEntry(
            timestamp=datetime.now(),
            level=level.value,
            category=LogCategory.PERFORMANCE.value,
            operation=operation,
            message=f"Performance: {operation} took {duration_ms:.2f}ms",
            metadata={
                'duration_ms': duration_ms,
                **(metadata or {})
            }
        )
        self._add_entry(entry)
        if duration_ms > 1000:
            logger.warning(f"Slow operation: {operation} took {duration_ms:.2f}ms")
    
    def log_resource_operation(self, operation: str, resource_type: str, 
                              resource_id: str, metadata: Dict[str, Any] = None):
        """
        记录资源操作
        
        Args:
            operation: 操作类型（register, cleanup, etc.）
            resource_type: 资源类型
            resource_id: 资源ID
            metadata: 元数据
        """
        entry = ControlLogEntry(
            timestamp=datetime.now(),
            level=LogLevel.DEBUG.value,
            category=LogCategory.RESOURCE.value,
            operation=operation,
            message=f"Resource {operation}: {resource_type} ({resource_id})",
            metadata={
                'resource_type': resource_type,
                'resource_id': resource_id,
                **(metadata or {})
            }
        )
        self._add_entry(entry)
        logger.debug(f"Resource {operation}: {resource_type} ({resource_id})")
    
    def _add_entry(self, entry: ControlLogEntry):
        """添加日志条目"""
        self._logs.append(entry)
        self._stats['total_logs'] += 1
        
        # 更新统计
        level = entry.level
        self._stats['by_level'][level] = self._stats['by_level'].get(level, 0) + 1
        
        category = entry.category
        self._stats['by_category'][category] = self._stats['by_category'].get(category, 0) + 1
    
    def get_logs(self, 
                level: Optional[str] = None,
                category: Optional[str] = None,
                operation: Optional[str] = None,
                limit: Optional[int] = None) -> List[ControlLogEntry]:
        """
        获取日志条目
        
        Args:
            level: 过滤日志级别
            category: 过滤日志分类
            operation: 过滤操作类型
            limit: 限制返回数量
            
        Returns:
            list: 日志条目列表
        """
        logs = list(self._logs)
        
        # 过滤
        if level:
            logs = [log for log in logs if log.level == level]
        if category:
            logs = [log for log in logs if log.category == category]
        if operation:
            logs = [log for log in logs if log.operation == operation]
        
        # 限制数量
        if limit:
            logs = logs[-limit:]
        
        return logs
    
    def get_recent_errors(self, limit: int = 10) -> List[ControlLogEntry]:
        """
        获取最近的错误日志
        
        Args:
            limit: 限制返回数量
            
        Returns:
            list: 错误日志列表
        """
        return self.get_logs(level=LogLevel.ERROR.value, limit=limit)
    
    def get_signal_history(self, limit: int = 20) -> List[ControlLogEntry]:
        """
        获取信号历史
        
        Args:
            limit: 限制返回数量
            
        Returns:
            list: 信号日志列表
        """
        return self.get_logs(category=LogCategory.SIGNAL.value, limit=limit)
    
    def get_state_history(self, limit: int = 20) -> List[ControlLogEntry]:
        """
        获取状态历史
        
        Args:
            limit: 限制返回数量
            
        Returns:
            list: 状态日志列表
        """
        return self.get_logs(category=LogCategory.STATE.value, limit=limit)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        Returns:
            dict: 统计数据
        """
        return {
            **self._stats,
            'current_entries': len(self._logs),
            'max_entries': self.max_entries
        }
    
    def clear_logs(self):
        """清除所有日志"""
        self._logs.clear()
        self._stats = {
            'total_logs': 0,
            'by_level': {},
            'by_category': {},
            'errors': 0
        }
        logger.info("Control logs cleared")
    
    def export_logs(self, filepath: str, format: str = 'json') -> bool:
        """
        导出日志到文件
        
        Args:
            filepath: 文件路径
            format: 导出格式（json或text）
            
        Returns:
            bool: 是否成功导出
        """
        try:
            logs = [log.to_dict() for log in self._logs]
            
            with open(filepath, 'w', encoding='utf-8') as f:
                if format == 'json':
                    json.dump(logs, f, indent=2, ensure_ascii=False)
                else:
                    for log in logs:
                        f.write(f"{log['timestamp']} [{log['level']}] {log['category']}: {log['message']}\n")
            
            logger.info(f"Logs exported to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to export logs: {e}")
            return False


# 全局控制日志记录器实例
_control_logger = None


def get_control_logger() -> ControlLogger:
    """
    获取全局控制日志记录器实例（单例模式）
    
    Returns:
        ControlLogger: 控制日志记录器实例
    """
    global _control_logger
    
    if _control_logger is None:
        _control_logger = ControlLogger()
    
    return _control_logger
