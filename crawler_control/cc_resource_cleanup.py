#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
资源清理管理器 - 管理爬虫资源的清理和释放

负责：
- 跟踪活动资源（连接、文件句柄、临时文件等）
- 执行资源清理（关闭连接、释放句柄、删除临时文件）
- 处理异常情况下的强制清理
- 防止资源泄漏

重要说明：
- DATABASE_CONNECTION: 只关闭数据库连接，不删除数据库或表
- NETWORK_CONNECTION: 只关闭网络连接，不影响远程服务
- FILE_HANDLE: 只关闭文件句柄，不删除持久化文件
- TEMP_FILE: 删除临时文件（如缓存、中间结果）
- MEMORY_CACHE: 清空内存缓存
- THREAD: 停止后台线程

数据持久化：
- 爬取的数据已经保存到数据库，不会被清理
- 只清理本次任务的临时资源和连接
"""

import logging
import threading
import time
from typing import Dict, List, Any, Callable, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class ResourceType(Enum):
    """资源类型枚举"""
    NETWORK_CONNECTION = "network_connection"
    DATABASE_CONNECTION = "database_connection"
    FILE_HANDLE = "file_handle"
    TEMP_FILE = "temp_file"
    THREAD = "thread"
    MEMORY_CACHE = "memory_cache"


@dataclass
class Resource:
    """资源数据模型"""
    id: str
    type: ResourceType
    name: str
    cleanup_func: Callable
    created_at: datetime = field(default_factory=datetime.now)
    critical: bool = True  # 是否为关键资源
    metadata: Dict[str, Any] = field(default_factory=dict)


class ResourceCleanupManager:
    """资源清理管理器"""
    
    def __init__(self):
        """初始化资源清理管理器"""
        self._resources: Dict[str, Resource] = {}
        self._lock = threading.Lock()
        self._cleanup_stats = {
            'total_registered': 0,
            'total_cleaned': 0,
            'failed_cleanups': 0,
            'last_cleanup_time': None
        }
        logger.info("ResourceCleanupManager initialized")
    
    def register_resource(self, resource_id: str, resource_type: ResourceType,
                         name: str, cleanup_func: Callable,
                         critical: bool = True, metadata: Dict = None) -> bool:
        """
        注册需要清理的资源
        
        Args:
            resource_id: 资源唯一标识
            resource_type: 资源类型
            name: 资源名称
            cleanup_func: 清理函数
            critical: 是否为关键资源
            metadata: 资源元数据
            
        Returns:
            bool: 是否成功注册
        """
        try:
            with self._lock:
                if resource_id in self._resources:
                    logger.warning(f"Resource {resource_id} already registered")
                    return False
                
                resource = Resource(
                    id=resource_id,
                    type=resource_type,
                    name=name,
                    cleanup_func=cleanup_func,
                    critical=critical,
                    metadata=metadata or {}
                )
                
                self._resources[resource_id] = resource
                self._cleanup_stats['total_registered'] += 1
                
                logger.debug(f"Registered resource: {resource_id} ({resource_type.value})")
                return True
                
        except Exception as e:
            logger.error(f"Failed to register resource {resource_id}: {e}")
            return False
    
    def unregister_resource(self, resource_id: str) -> bool:
        """
        取消注册资源（资源已被正常清理）
        
        Args:
            resource_id: 资源ID
            
        Returns:
            bool: 是否成功取消注册
        """
        try:
            with self._lock:
                if resource_id in self._resources:
                    del self._resources[resource_id]
                    logger.debug(f"Unregistered resource: {resource_id}")
                    return True
                return False
        except Exception as e:
            logger.error(f"Failed to unregister resource {resource_id}: {e}")
            return False
    
    def cleanup_resource(self, resource_id: str) -> bool:
        """
        清理单个资源
        
        Args:
            resource_id: 资源ID
            
        Returns:
            bool: 是否成功清理
        """
        try:
            with self._lock:
                if resource_id not in self._resources:
                    logger.warning(f"Resource {resource_id} not found")
                    return False
                
                resource = self._resources[resource_id]
            
            # 在锁外执行清理函数，避免死锁
            logger.info(f"Cleaning up resource: {resource_id} ({resource.type.value})")
            
            try:
                resource.cleanup_func()
                
                with self._lock:
                    if resource_id in self._resources:
                        del self._resources[resource_id]
                    self._cleanup_stats['total_cleaned'] += 1
                
                logger.info(f"Successfully cleaned up resource: {resource_id}")
                return True
                
            except Exception as cleanup_error:
                logger.error(f"Cleanup function failed for {resource_id}: {cleanup_error}")
                self._cleanup_stats['failed_cleanups'] += 1
                
                # 即使清理失败，也从注册表中移除
                with self._lock:
                    if resource_id in self._resources:
                        del self._resources[resource_id]
                
                return False
                
        except Exception as e:
            logger.error(f"Error cleaning up resource {resource_id}: {e}")
            return False
    
    def cleanup_all(self, force: bool = False, critical_only: bool = False) -> Dict[str, Any]:
        """
        清理所有资源
        
        Args:
            force: 是否强制清理（忽略错误继续）
            critical_only: 是否只清理关键资源
            
        Returns:
            dict: 清理结果统计
        """
        logger.info(f"Starting cleanup_all (force={force}, critical_only={critical_only})")
        
        start_time = time.time()
        results = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'duration': 0
        }
        
        # 获取资源列表的副本
        with self._lock:
            resources_to_clean = list(self._resources.values())
        
        # 过滤资源
        if critical_only:
            resources_to_clean = [r for r in resources_to_clean if r.critical]
        
        results['total'] = len(resources_to_clean)
        
        # 按类型分组清理（确保清理顺序）
        cleanup_order = [
            ResourceType.THREAD,
            ResourceType.NETWORK_CONNECTION,
            ResourceType.DATABASE_CONNECTION,
            ResourceType.FILE_HANDLE,
            ResourceType.TEMP_FILE,
            ResourceType.MEMORY_CACHE
        ]
        
        for resource_type in cleanup_order:
            type_resources = [r for r in resources_to_clean if r.type == resource_type]
            
            for resource in type_resources:
                try:
                    success = self.cleanup_resource(resource.id)
                    if success:
                        results['success'] += 1
                    else:
                        results['failed'] += 1
                        if not force:
                            logger.warning(f"Cleanup failed for {resource.id}, stopping (force=False)")
                            break
                except Exception as e:
                    logger.error(f"Exception during cleanup of {resource.id}: {e}")
                    results['failed'] += 1
                    if not force:
                        break
        
        results['duration'] = time.time() - start_time
        self._cleanup_stats['last_cleanup_time'] = datetime.now()
        
        logger.info(f"Cleanup completed: {results}")
        return results
    
    def cleanup_by_type(self, resource_type: ResourceType) -> int:
        """
        按类型清理资源
        
        Args:
            resource_type: 资源类型
            
        Returns:
            int: 清理的资源数量
        """
        logger.info(f"Cleaning up resources of type: {resource_type.value}")
        
        with self._lock:
            resources_to_clean = [
                r.id for r in self._resources.values()
                if r.type == resource_type
            ]
        
        cleaned_count = 0
        for resource_id in resources_to_clean:
            if self.cleanup_resource(resource_id):
                cleaned_count += 1
        
        logger.info(f"Cleaned up {cleaned_count} resources of type {resource_type.value}")
        return cleaned_count
    
    def cleanup_non_critical(self) -> int:
        """
        清理非关键资源（用于长时间暂停）
        
        Returns:
            int: 清理的资源数量
        """
        logger.info("Cleaning up non-critical resources")
        
        with self._lock:
            non_critical_ids = [
                r.id for r in self._resources.values()
                if not r.critical
            ]
        
        cleaned_count = 0
        for resource_id in non_critical_ids:
            if self.cleanup_resource(resource_id):
                cleaned_count += 1
        
        logger.info(f"Cleaned up {cleaned_count} non-critical resources")
        return cleaned_count
    
    def get_active_resources(self) -> List[Dict[str, Any]]:
        """
        获取活动资源列表
        
        Returns:
            list: 活动资源信息列表
        """
        with self._lock:
            return [
                {
                    'id': r.id,
                    'type': r.type.value,
                    'name': r.name,
                    'critical': r.critical,
                    'created_at': r.created_at.isoformat(),
                    'metadata': r.metadata
                }
                for r in self._resources.values()
            ]
    
    def get_resource_count(self) -> Dict[str, int]:
        """
        获取资源数量统计
        
        Returns:
            dict: 按类型统计的资源数量
        """
        with self._lock:
            counts = {}
            for resource in self._resources.values():
                type_name = resource.type.value
                counts[type_name] = counts.get(type_name, 0) + 1
            
            counts['total'] = len(self._resources)
            return counts
    
    def get_cleanup_stats(self) -> Dict[str, Any]:
        """
        获取清理统计信息
        
        Returns:
            dict: 清理统计
        """
        stats = self._cleanup_stats.copy()
        stats['active_resources'] = len(self._resources)
        if stats['last_cleanup_time']:
            stats['last_cleanup_time'] = stats['last_cleanup_time'].isoformat()
        return stats
    
    def force_cleanup_all(self) -> Dict[str, Any]:
        """
        强制清理所有资源（异常情况下使用）
        
        Returns:
            dict: 清理结果
        """
        logger.warning("Force cleanup initiated")
        return self.cleanup_all(force=True, critical_only=False)


# 全局资源清理管理器实例
_resource_cleanup_manager = None
_manager_lock = threading.Lock()


def get_resource_cleanup_manager() -> ResourceCleanupManager:
    """
    获取全局资源清理管理器实例（单例模式）
    
    Returns:
        ResourceCleanupManager: 资源清理管理器实例
    """
    global _resource_cleanup_manager
    
    if _resource_cleanup_manager is None:
        with _manager_lock:
            if _resource_cleanup_manager is None:
                _resource_cleanup_manager = ResourceCleanupManager()
    
    return _resource_cleanup_manager
