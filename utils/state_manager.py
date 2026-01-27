#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一状态管理器 - 多客户端实时同步
实现版本化状态跟踪和增强轮询机制
"""

import time
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict
from cache_manager import cache_manager, CacheKeys

logger = logging.getLogger(__name__)


@dataclass
class StateVersion:
    """状态版本信息"""
    version: int = 0
    timestamp: float = field(default_factory=time.time)
    changes: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            'version': self.version,
            'timestamp': self.timestamp,
            'changes': self.changes
        }


@dataclass
class UnifiedCrawlState:
    """统一爬虫状态 - 包含所有状态信息"""
    # 基础状态
    is_crawling: bool = False
    is_paused: bool = False
    should_stop: bool = False
    message: str = '空闲'

    # 进度信息
    sections_total: int = 0
    sections_done: int = 0
    current_section: str = ''
    current_page: int = 0
    current_section_pages: int = 0
    current_section_processed: int = 0
    max_pages: int = 0
    processed_pages: int = 0
    estimated_total_pages: int = 0
    progress_percent: float = 0.0

    # 统计信息
    total_saved: int = 0
    total_skipped: int = 0
    total_failed: int = 0
    current_section_saved: int = 0
    current_section_skipped: int = 0

    # 页码概念区分字段
    current_page_actual: int = 0  # 实际论坛页码
    max_pages_actual: int = 0  # 实际板块最大页数
    current_page_task: int = 0  # 任务进度页码
    max_pages_task: int = 0  # 本次任务总页数

    # 时间信息
    start_time: Optional[float] = None
    last_update_time: float = field(default_factory=time.time)

    # 版本信息
    version: int = 0
    
    def to_dict(self) -> Dict:
        """转换为字典格式"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'UnifiedCrawlState':
        """从字典创建状态对象"""
        # 过滤掉不存在的字段
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered_data)


class StateManager:
    """统一状态管理器"""
    
    def __init__(self):
        self._state = UnifiedCrawlState()
        self._lock = threading.RLock()
        self._version = 0
        self._subscribers = []  # 状态变更订阅者
        
    def get_state(self, client_version: int = 0) -> Dict[str, Any]:
        """获取状态信息
        
        Args:
            client_version: 客户端当前版本号，用于增量更新
            
        Returns:
            包含状态信息和版本信息的字典
        """
        with self._lock:
            current_version = self._state.version
            
            # 如果客户端版本是最新的，只返回版本信息
            if client_version >= current_version:
                return {
                    'version': current_version,
                    'timestamp': self._state.last_update_time,
                    'has_changes': False
                }
            
            # 返回完整状态信息
            state_dict = self._state.to_dict()
            state_dict['has_changes'] = True
            
            return state_dict
    
    def update_state(self, updates: Dict[str, Any], source: str = 'unknown') -> bool:
        """更新状态信息
        
        Args:
            updates: 要更新的状态字段
            source: 更新来源标识
            
        Returns:
            是否有实际更新
        """
        with self._lock:
            has_changes = False
            changes = []
            
            # 记录变更的字段
            for key, new_value in updates.items():
                if hasattr(self._state, key):
                    old_value = getattr(self._state, key)
                    if old_value != new_value:
                        setattr(self._state, key, new_value)
                        changes.append(key)
                        has_changes = True
            
            if has_changes:
                # 更新版本和时间戳
                self._version += 1
                self._state.version = self._version
                self._state.last_update_time = time.time()
                
                # 同步到缓存
                self._sync_to_cache()
                
                # 通知订阅者
                self._notify_subscribers(changes, source)
                
                logger.debug(f"状态更新 [v{self._version}] 来源:{source} 变更:{changes}")
            
            return has_changes
    
    def sync_from_legacy_sources(self):
        """从传统状态源同步数据"""
        try:
            # 从缓存获取传统状态
            crawl_status = cache_manager.shared_get(CacheKeys.CRAWL_STATUS) or {}
            crawl_progress = cache_manager.shared_get(CacheKeys.CRAWL_PROGRESS) or {}
            crawl_control = cache_manager.shared_get(CacheKeys.CRAWL_CONTROL) or {}
            
            # 合并状态信息
            updates = {}
            
            # 从 crawl_status 同步
            if crawl_status:
                updates.update({
                    'is_crawling': crawl_status.get('is_crawling', False),
                    'is_paused': crawl_status.get('is_paused', False),
                    'message': crawl_status.get('message', '空闲')
                })
            
            # 从 crawl_progress 同步
            if crawl_progress:
                updates.update({
                    'sections_total': crawl_progress.get('sections_total', 0),
                    'sections_done': crawl_progress.get('sections_done', 0),
                    'current_section': crawl_progress.get('current_section', ''),
                    'current_page': crawl_progress.get('current_page', 0),
                    'current_section_pages': crawl_progress.get('current_section_pages', 0),
                    'current_section_processed': crawl_progress.get('current_section_processed', 0),
                    'max_pages': crawl_progress.get('max_pages', 0),
                    'processed_pages': crawl_progress.get('processed_pages', 0),
                    'estimated_total_pages': crawl_progress.get('estimated_total_pages', 0),
                    'progress_percent': crawl_progress.get('progress_percent', 0.0),
                    'total_saved': crawl_progress.get('total_saved', 0),
                    'total_skipped': crawl_progress.get('total_skipped', 0),
                    'total_failed': crawl_progress.get('total_failed', 0),
                    'current_section_saved': crawl_progress.get('current_section_saved', 0),
                    'current_section_skipped': crawl_progress.get('current_section_skipped', 0),
                    'start_time': crawl_progress.get('start_time')
                })
            
            # 从 crawl_control 同步
            if crawl_control:
                updates.update({
                    'should_stop': crawl_control.get('stop', False),
                    'is_paused': crawl_control.get('paused', False)
                })
            
            # 批量更新状态
            if updates:
                self.update_state(updates, source='legacy_sync')
                
        except Exception as e:
            logger.debug(f"从传统状态源同步失败: {e}")
    
    def _sync_to_cache(self):
        """同步到缓存"""
        try:
            state_dict = self._state.to_dict()
            cache_manager.shared_set(CacheKeys.CRAWL_UNIFIED_STATE, state_dict)
        except Exception as e:
            logger.debug(f"同步到缓存失败: {e}")
    
    def _sync_to_legacy_cache(self):
        """同步到传统缓存格式（向后兼容）"""
        try:
            # 构建传统格式的状态
            crawl_status = {
                'is_crawling': self._state.is_crawling,
                'is_paused': self._state.is_paused,
                'message': self._state.message
            }

            crawl_progress = {
                'sections_total': self._state.sections_total,
                'sections_done': self._state.sections_done,
                'current_section': self._state.current_section,
                'current_page': self._state.current_page,
                'current_section_pages': self._state.current_section_pages,
                'current_section_processed': self._state.current_section_processed,
                'max_pages': self._state.max_pages,
                'processed_pages': self._state.processed_pages,
                'estimated_total_pages': self._state.estimated_total_pages,
                'progress_percent': self._state.progress_percent,
                'total_saved': self._state.total_saved,
                'total_skipped': self._state.total_skipped,
                'total_failed': self._state.total_failed,
                'current_section_saved': self._state.current_section_saved,
                'current_section_skipped': self._state.current_section_skipped,
                'start_time': self._state.start_time,
                # 页码概念区分字段
                'current_page_actual': self._state.current_page_actual,
                'max_pages_actual': self._state.max_pages_actual,
                'current_page_task': self._state.current_page_task,
                'max_pages_task': self._state.max_pages_task,
            }

            crawl_control = {
                'stop': self._state.should_stop,
                'paused': self._state.is_paused
            }

            # 更新传统缓存
            cache_manager.shared_set(CacheKeys.CRAWL_STATUS, crawl_status)
            cache_manager.shared_set(CacheKeys.CRAWL_PROGRESS, crawl_progress)
            cache_manager.shared_set(CacheKeys.CRAWL_CONTROL, crawl_control)

        except Exception as e:
            logger.debug(f"同步到传统缓存失败: {e}")
    
    def _notify_subscribers(self, changes: List[str], source: str):
        """通知状态变更订阅者"""
        for callback in self._subscribers:
            try:
                callback(changes, source, self._state)
            except Exception as e:
                logger.debug(f"通知订阅者失败: {e}")
    
    def subscribe(self, callback):
        """订阅状态变更"""
        self._subscribers.append(callback)
    
    def unsubscribe(self, callback):
        """取消订阅状态变更"""
        if callback in self._subscribers:
            self._subscribers.remove(callback)
    
    def reset_state(self):
        """重置状态"""
        with self._lock:
            self._state = UnifiedCrawlState()
            self._version += 1
            self._state.version = self._version
            self._sync_to_cache()
            logger.info("状态已重置")


# 全局状态管理器实例
_state_manager = None
_manager_lock = threading.Lock()


def get_state_manager() -> StateManager:
    """获取全局状态管理器实例"""
    global _state_manager
    if _state_manager is None:
        with _manager_lock:
            if _state_manager is None:
                _state_manager = StateManager()
    return _state_manager


def get_unified_state(client_version: int = 0) -> Dict[str, Any]:
    """获取统一状态（便捷函数）"""
    return get_state_manager().get_state(client_version)


def update_unified_state(updates: Dict[str, Any], source: str = 'unknown') -> bool:
    """更新统一状态（便捷函数）"""
    return get_state_manager().update_state(updates, source)


def sync_from_legacy() -> None:
    """从传统状态源同步（便捷函数）"""
    get_state_manager().sync_from_legacy_sources()