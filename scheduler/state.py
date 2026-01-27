#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫状态管理模块 - 单一数据源的状态管理
集成统一状态管理器，保持向后兼容
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict

logger = logging.getLogger(__name__)


@dataclass
class CrawlState:
    """爬虫状态管理 - 单一数据源（保持向后兼容）"""
    is_crawling: bool = False
    is_paused: bool = False
    should_stop: bool = False
    message: str = '空闲'
    last_crawl_time: Optional[datetime] = None

    # 进度信息
    sections_total: int = 0
    sections_done: int = 0
    current_section: str = ''
    current_page: int = 0
    max_pages: int = 0
    total_saved: int = 0
    total_skipped: int = 0

    # 页码概念区分字段
    current_page_actual: int = 0  # 实际论坛页码
    max_pages_actual: int = 0  # 实际板块最大页数
    current_page_task: int = 0  # 任务进度页码
    max_pages_task: int = 0  # 本次任务总页数
    
    def to_dict(self) -> Dict:
        return {
            'is_crawling': self.is_crawling,
            'is_paused': self.is_paused,
            'message': self.message,
            'progress': {
                'sections_total': self.sections_total,
                'sections_done': self.sections_done,
                'current_section': self.current_section,
                'current_page': self.current_page,
                'max_pages': self.max_pages,
                'total_saved': self.total_saved,
                'total_skipped': self.total_skipped,
                # 页码概念区分字段
                'current_page_actual': self.current_page_actual,
                'max_pages_actual': self.max_pages_actual,
                'current_page_task': self.current_page_task,
                'max_pages_task': self.max_pages_task,
            }
        }
    
    def sync_to_cache(self):
        """同步到共享缓存（使用新的状态管理器）"""
        try:
            from utils.state_manager import update_unified_state

            # 转换为统一状态格式
            updates = {
                'is_crawling': self.is_crawling,
                'is_paused': self.is_paused,
                'should_stop': self.should_stop,
                'message': self.message,
                'sections_total': self.sections_total,
                'sections_done': self.sections_done,
                'current_section': self.current_section,
                'current_page': self.current_page,
                'max_pages': self.max_pages,
                'total_saved': self.total_saved,
                'total_skipped': self.total_skipped,
                # 页码概念区分字段
                'current_page_actual': self.current_page_actual,
                'max_pages_actual': self.max_pages_actual,
                'current_page_task': self.current_page_task,
                'max_pages_task': self.max_pages_task,
            }

            update_unified_state(updates, source='crawl_state')

        except Exception as e:
            logger.debug(f"同步到统一状态管理器失败: {e}")
            # 回退到传统方式
            from cache_manager import cache_manager, CacheKeys
            cache_manager.shared_set(CacheKeys.CRAWL_STATE, self.to_dict())
    
    @classmethod
    def from_cache(cls) -> 'CrawlState':
        """从缓存恢复（优先使用统一状态管理器）"""
        try:
            from utils.state_manager import get_unified_state

            # 从统一状态管理器获取
            state_data = get_unified_state()
            if state_data.get('has_changes', True):
                return cls(
                    is_crawling=state_data.get('is_crawling', False),
                    is_paused=state_data.get('is_paused', False),
                    should_stop=state_data.get('should_stop', False),
                    message=state_data.get('message', '空闲'),
                    sections_total=state_data.get('sections_total', 0),
                    sections_done=state_data.get('sections_done', 0),
                    current_section=state_data.get('current_section', ''),
                    current_page=state_data.get('current_page', 0),
                    max_pages=state_data.get('max_pages', 0),
                    total_saved=state_data.get('total_saved', 0),
                    total_skipped=state_data.get('total_skipped', 0),
                    # 页码概念区分字段
                    current_page_actual=state_data.get('current_page_actual', 0),
                    max_pages_actual=state_data.get('max_pages_actual', 0),
                    current_page_task=state_data.get('current_page_task', 0),
                    max_pages_task=state_data.get('max_pages_task', 0),
                )
        except Exception as e:
            logger.debug(f"从统一状态管理器恢复失败: {e}")

        # 回退到传统方式
        try:
            from cache_manager import cache_manager, CacheKeys
            data = cache_manager.shared_get(CacheKeys.CRAWL_STATE)
            if data:
                return cls(**data)
        except Exception as e:
            logger.debug(f"从传统缓存恢复失败: {e}")

        return cls()


# 全局单例
_crawl_state = None


def get_crawl_state() -> CrawlState:
    global _crawl_state
    if _crawl_state is None:
        _crawl_state = CrawlState.from_cache()
    return _crawl_state


def sync_crawl_state():
    """将当前内存中的爬虫状态和进度同步到共享存储 (核心同步逻辑)"""
    try:
        # 优先使用统一状态管理器
        from utils.state_manager import update_unified_state, sync_from_legacy

        # 先从传统源同步到统一状态管理器
        sync_from_legacy()

        # 从 Flask app.config 或全局变量获取状态
        updates = {}

        try:
            from flask import current_app

            status = current_app.config.get('CRAWL_STATUS')
            progress = current_app.config.get('CRAWL_PROGRESS')
            control = current_app.config.get('CRAWL_CONTROL')

        except:
            # 回退到共享缓存（多进程安全）
            try:
                from cache_manager import cache_manager, CacheKeys
                status = cache_manager.shared_get(CacheKeys.CRAWL_STATUS) or None
                progress = cache_manager.shared_get(CacheKeys.CRAWL_PROGRESS) or None
                control = cache_manager.shared_get(CacheKeys.CRAWL_CONTROL) or None
            except:
                status = progress = control = None

        # 构建更新数据
        if status:
            updates.update({
                'is_crawling': status.get('is_crawling', False),
                'is_paused': status.get('is_paused', False),
                'message': status.get('message', '空闲')
            })

        if progress:
            updates.update({
                'sections_total': progress.get('sections_total', 0),
                'sections_done': progress.get('sections_done', 0),
                'current_section': progress.get('current_section', ''),
                'current_page': progress.get('current_page', 0),
                'current_section_pages': progress.get('current_section_pages', 0),
                'current_section_processed': progress.get('current_section_processed', 0),
                'max_pages': progress.get('max_pages', 0),
                'processed_pages': progress.get('processed_pages', 0),
                'estimated_total_pages': progress.get('estimated_total_pages', 0),
                'progress_percent': progress.get('progress_percent', 0.0),
                'total_saved': progress.get('total_saved', 0),
                'total_skipped': progress.get('total_skipped', 0),
                'total_failed': progress.get('total_failed', 0),
                'current_section_saved': progress.get('current_section_saved', 0),
                'current_section_skipped': progress.get('current_section_skipped', 0),
                'start_time': progress.get('start_time'),
                # 页码概念区分字段
                'current_page_actual': progress.get('current_page_actual', 0),       # 实际论坛页码
                'current_page_task': progress.get('current_page_task', 0),           # 任务进度页码
                'max_pages_actual': progress.get('max_pages_actual', 0),             # 实际板块最大页
                'max_pages_task': progress.get('max_pages_task', 0),                 # 本次任务总页数
            })

        if control:
            updates.update({
                'should_stop': control.get('stop', False),
                'is_paused': control.get('paused', False)
            })

        # 更新统一状态
        if updates:
            update_unified_state(updates, source='sync_crawl_state')

        # 同步进度到状态协调器 - 关键修复！
        try:
            from crawler_control.cc_control_bridge import get_crawler_control_bridge
            bridge = get_crawler_control_bridge()

            if progress:
                # 构建progress字典
                progress_data = {
                    'sections_total': progress.get('sections_total', 0),
                    'sections_done': progress.get('sections_done', 0),
                    'current_section': progress.get('current_section', ''),
                    'current_page': progress.get('current_page', 0),
                    'current_section_pages': progress.get('current_section_pages', 0),
                    'current_section_processed': progress.get('current_section_processed', 0),
                    'max_pages': progress.get('max_pages', 0),
                    'processed_pages': progress.get('processed_pages', 0),
                    'estimated_total_pages': progress.get('estimated_total_pages', 0),
                    'progress_percent': progress.get('progress_percent', 0.0),
                    'total_saved': progress.get('total_saved', 0),
                    'total_skipped': progress.get('total_skipped', 0),
                    'total_failed': progress.get('total_failed', 0),
                    'current_section_saved': progress.get('current_section_saved', 0),
                    'current_section_skipped': progress.get('current_section_skipped', 0),
                    # 页码概念区分字段
                    'current_page_actual': progress.get('current_page_actual', 0),       # 实际论坛页码
                    'current_page_task': progress.get('current_page_task', 0),           # 任务进度页码
                    'max_pages_actual': progress.get('max_pages_actual', 0),             # 实际板块最大页
                    'max_pages_task': progress.get('max_pages_task', 0),                 # 本次任务总页数
                }
                bridge.coordinator.update_progress(progress_data)
        except Exception as e:
            logger.debug(f"同步进度到状态协调器失败: {e}")

    except Exception as e:
        logger.debug(f"统一状态同步失败，回退到传统方式: {e}")

        # 传统同步方式（向后兼容）
        try:
            from flask import current_app
            from cache_manager import cache_manager

            # 优先从 Flask app.config 获取 (如果有上下文)
            try:
                status = current_app.config.get('CRAWL_STATUS')
                progress = current_app.config.get('CRAWL_PROGRESS')
                control = current_app.config.get('CRAWL_CONTROL')
            except:
                # 回退到共享缓存（多进程安全）
                from cache_manager import cache_manager, CacheKeys
                status = cache_manager.shared_get(CacheKeys.CRAWL_STATUS)
                progress = cache_manager.shared_get(CacheKeys.CRAWL_PROGRESS)
                control = cache_manager.shared_get(CacheKeys.CRAWL_CONTROL)

            if status:
                cache_manager.shared_set(CacheKeys.CRAWL_STATUS, status)
            if progress:
                cache_manager.shared_set(CacheKeys.CRAWL_PROGRESS, progress)
            if control:
                cache_manager.shared_set(CacheKeys.CRAWL_CONTROL, control)

        except Exception as e2:
            logger.debug(f"传统状态同步也失败: {e2}")