#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
信号队列管理器 - 管理控制信号的队列和传递

负责：
- 发送控制信号到队列
- 获取待处理的信号
- 确认信号已处理
- 清除所有待处理信号
- 记录信号操作日志
"""

import json
import time
import uuid
import os
import threading
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    """控制信号数据模型"""
    id: str
    type: str  # 'stop', 'pause', 'resume'
    timestamp: datetime
    payload: dict
    priority: int
    processed: bool = False
    acknowledged: bool = False
    
    def to_dict(self) -> dict:
        """转换为字典格式"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Signal':
        """从字典创建Signal对象"""
        data = data.copy()
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)


class SignalQueueManager:
    """信号队列管理器"""
    
    # 信号优先级定义
    SIGNAL_PRIORITIES = {
        'stop': 1,      # 最高优先级
        'pause': 2,     # 中等优先级
        'resume': 3     # 最低优先级
    }
    
    def __init__(self, redis_client=None, use_memory_fallback=True, signal_file=None):
        """
        初始化信号队列管理器

        Args:
            redis_client: Redis客户端，如果为None则使用文件队列
            use_memory_fallback: 当Redis不可用时是否降级到文件队列
            signal_file: 信号队列文件路径（可选）
        """
        self.redis = redis_client
        self.use_memory_fallback = use_memory_fallback
        self.queue_key = "crawler:signals"
        self.processed_key = "crawler:signals:processed"

        # 添加文件锁，防止并发读写
        self._file_lock = threading.Lock()

        # 设置信号队列文件路径
        if signal_file:
            self.signal_file = signal_file
        else:
            # 使用配置管理器获取路径
            from configuration import Config
            self.signal_file = Config.get_path('signal_queue')

        # 内存队列作为降级方案（实例级别）
        # 注意：在非Redis模式下，实际数据存储在文件中，内存队列仅用作缓存
        self._memory_queue: List[Signal] = []
        self._memory_processed: Dict[str, Signal] = {}

        # 检查Redis连接
        self._redis_available = self._check_redis_connection()

        # 如果不使用Redis，从文件加载信号队列
        if not self._redis_available:
            self._load_signals_from_file()

        logger.info(f"SignalQueueManager initialized, Redis available: {self._redis_available}, Signal file: {self.signal_file}")
    
    def _check_redis_connection(self) -> bool:
        """检查Redis连接是否可用"""
        if not self.redis:
            return False
        
        try:
            self.redis.ping()
            return True
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}")
            return False
    
    def _get_signal_priority(self, signal_type: str) -> int:
        """获取信号优先级"""
        return self.SIGNAL_PRIORITIES.get(signal_type, 999)
    
    def send_signal(self, signal_type: str, payload: dict = None) -> str:
        """
        发送控制信号
        
        Args:
            signal_type: 信号类型 ('stop', 'pause', 'resume')
            payload: 信号载荷数据
            
        Returns:
            str: 信号ID
        """
        if payload is None:
            payload = {}
        
        signal_id = str(uuid.uuid4())
        priority = self._get_signal_priority(signal_type)
        
        signal = Signal(
            id=signal_id,
            type=signal_type,
            timestamp=datetime.now(),
            payload=payload,
            priority=priority,
            processed=False,
            acknowledged=False
        )
        
        try:
            if self._redis_available:
                self._send_signal_redis(signal)
            else:
                self._send_signal_memory(signal)
            
            logger.info(f"Signal sent: {signal_type} (ID: {signal_id})")
            return signal_id
            
        except Exception as e:
            logger.error(f"Failed to send signal {signal_type}: {e}")
            if self.use_memory_fallback and self._redis_available:
                # Redis失败时降级到内存队列
                logger.warning("Falling back to memory queue")
                self._redis_available = False
                return self.send_signal(signal_type, payload)
            raise
    
    def _send_signal_redis(self, signal: Signal):
        """使用Redis发送信号"""
        signal_data = json.dumps(signal.to_dict())
        # 使用有序集合，按优先级和时间戳排序
        score = signal.priority * 1000000 + int(signal.timestamp.timestamp())
        self.redis.zadd(self.queue_key, {signal_data: score})
    
    def _send_signal_memory(self, signal: Signal):
        """使用内存队列发送信号（文件持久化模式）"""
        # 不仅添加到内存，还要持久化到文件
        self._memory_queue.append(signal)
        # 按优先级排序
        self._memory_queue.sort(key=lambda s: (s.priority, s.timestamp))
        # 持久化到文件
        self._persist_signals_to_file()

    def _load_signals_from_file(self):
        """ 从文件加载信号队列（带文件锁）"""
        if not os.path.exists(self.signal_file):
            logger.debug(f"Signal file not found: {self.signal_file}")
            return

        with self._file_lock:  # 使用文件锁防止并发读写
            try:
                with open(self.signal_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # 加载待处理信号
                if 'pending' in data:
                    self._memory_queue = []
                    for signal_dict in data['pending']:
                        try:
                            signal = Signal.from_dict(signal_dict)
                            self._memory_queue.append(signal)
                        except Exception as e:
                            logger.error(f"Failed to parse signal from file: {e}")

                # 加载已处理信号
                if 'processed' in data:
                    self._memory_processed = {}
                    for signal_dict in data['processed']:
                        try:
                            signal = Signal.from_dict(signal_dict)
                            self._memory_processed[signal.id] = signal
                        except Exception as e:
                            logger.error(f"Failed to parse processed signal from file: {e}")

                # 频繁调用，不打印日志避免刷屏

            except Exception as e:
                logger.error(f"Failed to load signals from file: {e}")

    def _persist_signals_to_file(self):
        """将信号队列持久化到文件（带文件锁）"""
        with self._file_lock:  # 使用文件锁防止并发写入
            try:
                # 确保目录存在
                signal_file_path = Path(self.signal_file)
                signal_file_path.parent.mkdir(parents=True, exist_ok=True)

                # 准备数据
                data = {
                    'pending': [s.to_dict() for s in self._memory_queue],
                    'processed': [s.to_dict() for s in self._memory_processed.values()],
                    'updated_at': datetime.now().isoformat()
                }

                # 原子写入
                temp_file = f"{self.signal_file}.tmp"
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

                # 原子替换
                os.replace(temp_file, self.signal_file)

                logger.debug(f"Persisted {len(self._memory_queue)} pending and {len(self._memory_processed)} processed signals to file")
                return True

            except Exception as e:
                logger.error(f"Failed to persist signals to file: {e}")
                return False
    
    def get_pending_signals(self) -> List[Signal]:
        """
        获取待处理的信号
        
        Returns:
            List[Signal]: 按优先级排序的待处理信号列表
        """
        try:
            if self._redis_available:
                return self._get_pending_signals_redis()
            else:
                return self._get_pending_signals_memory()
                
        except Exception as e:
            logger.error(f"Failed to get pending signals: {e}")
            if self.use_memory_fallback and self._redis_available:
                # Redis失败时降级到内存队列
                logger.warning("Falling back to memory queue for reading")
                self._redis_available = False
                return self.get_pending_signals()
            return []
    
    def _get_pending_signals_redis(self) -> List[Signal]:
        """从Redis获取待处理信号"""
        # 获取所有信号，按分数（优先级+时间戳）排序
        signal_data_list = self.redis.zrange(self.queue_key, 0, -1)
        
        signals = []
        for signal_data in signal_data_list:
            try:
                signal_dict = json.loads(signal_data)
                signal = Signal.from_dict(signal_dict)
                if not signal.processed:
                    signals.append(signal)
            except Exception as e:
                logger.error(f"Failed to parse signal data: {e}")
                # 移除损坏的数据
                self.redis.zrem(self.queue_key, signal_data)
        
        return signals
    
    def _get_pending_signals_memory(self) -> List[Signal]:
        """从内存队列获取待处理信号（文件持久化模式）"""
        # 修复：每次都从文件重新加载，确保多进程一致性
        self._load_signals_from_file()
        return [s for s in self._memory_queue if not s.processed]
    
    def acknowledge_signal(self, signal_id: str):
        """
        确认信号已处理
        
        Args:
            signal_id: 信号ID
        """
        try:
            if self._redis_available:
                self._acknowledge_signal_redis(signal_id)
            else:
                self._acknowledge_signal_memory(signal_id)
            
            logger.info(f"Signal acknowledged: {signal_id}")
            
        except Exception as e:
            logger.error(f"Failed to acknowledge signal {signal_id}: {e}")
    
    def _acknowledge_signal_redis(self, signal_id: str):
        """在Redis中确认信号"""
        # 查找并移动信号到已处理队列
        signal_data_list = self.redis.zrange(self.queue_key, 0, -1)
        
        for signal_data in signal_data_list:
            try:
                signal_dict = json.loads(signal_data)
                if signal_dict['id'] == signal_id:
                    # 标记为已确认
                    signal_dict['acknowledged'] = True
                    signal_dict['processed'] = True
                    
                    # 移动到已处理队列
                    processed_data = json.dumps(signal_dict)
                    self.redis.hset(self.processed_key, signal_id, processed_data)
                    
                    # 从待处理队列移除
                    self.redis.zrem(self.queue_key, signal_data)
                    break
            except Exception as e:
                logger.error(f"Failed to process signal data during acknowledgment: {e}")
    
    def _acknowledge_signal_memory(self, signal_id: str):
        """在内存队列中确认信号（文件持久化模式）"""
        # 修复：先从文件加载最新状态
        self._load_signals_from_file()

        for signal in self._memory_queue:
            if signal.id == signal_id:
                signal.acknowledged = True
                signal.processed = True
                self._memory_processed[signal_id] = signal
                break

        # 修复：持久化到文件
        self._persist_signals_to_file()
    
    def clear_signals(self):
        """清除所有待处理信号"""
        try:
            if self._redis_available:
                self.redis.delete(self.queue_key)
            else:
                self._memory_queue.clear()
                # 修复：同时清除文件
                self._persist_signals_to_file()

            logger.info("All pending signals cleared")

        except Exception as e:
            logger.error(f"Failed to clear signals: {e}")
    
    def get_signal_count(self) -> int:
        """获取待处理信号数量"""
        try:
            if self._redis_available:
                return self.redis.zcard(self.queue_key)
            else:
                # 从文件加载最新状态
                self._load_signals_from_file()
                return len([s for s in self._memory_queue if not s.processed])
        except Exception as e:
            logger.error(f"Failed to get signal count: {e}")
            return 0
    
    def get_processed_signals(self, limit: int = 100) -> List[Signal]:
        """
        获取已处理的信号历史

        Args:
            limit: 返回的最大信号数量

        Returns:
            List[Signal]: 已处理的信号列表
        """
        try:
            if self._redis_available:
                signal_data_dict = self.redis.hgetall(self.processed_key)
                signals = []
                for signal_data in list(signal_data_dict.values())[-limit:]:
                    try:
                        signal_dict = json.loads(signal_data)
                        signals.append(Signal.from_dict(signal_dict))
                    except Exception as e:
                        logger.error(f"Failed to parse processed signal: {e}")
                return signals
            else:
                # 从文件加载最新状态
                self._load_signals_from_file()
                return list(self._memory_processed.values())[-limit:]
        except Exception as e:
            logger.error(f"Failed to get processed signals: {e}")
            return []
    
    def health_check(self) -> Dict[str, Any]:
        """
        健康检查
        
        Returns:
            dict: 健康状态信息
        """
        return {
            'redis_available': self._redis_available,
            'pending_signals': self.get_signal_count(),
            'memory_fallback_enabled': self.use_memory_fallback,
            'queue_type': 'redis' if self._redis_available else 'memory'
        }