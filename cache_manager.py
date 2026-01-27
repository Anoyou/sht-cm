#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
缓存管理器 - 提供内存缓存和Redis缓存支持
优化频繁查询的性能，减少数据库压力
"""

import os
import json
import time
import logging
from functools import wraps
from typing import Any, Optional, Union, Dict
from configuration import Config

logger = logging.getLogger(__name__)

class MemoryCache:
    """内存缓存实现 - 简单的LRU缓存"""
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 300):
        self.cache: Dict[str, Dict] = {}
        self.access_times: Dict[str, float] = {}
        self.max_size = max_size
        self.default_ttl = default_ttl
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        if key not in self.cache:
            return None
        
        item = self.cache[key]
        if time.time() > item['expire_at']:
            self.delete(key)
            return None
            
        self.access_times[key] = time.time()
        return item['value']
    
    def set(self, key: str, value: Any, ttl: int = None):
        """设置缓存值"""
        if len(self.cache) >= self.max_size:
            # 驱逐最久未使用的
            oldest_key = min(self.access_times, key=self.access_times.get)
            self.delete(oldest_key)
            
        expire_at = time.time() + (ttl or self.default_ttl)
        self.cache[key] = {'value': value, 'expire_at': expire_at}
        self.access_times[key] = time.time()
    
    def delete(self, key: str):
        """删除缓存值"""
        self.cache.pop(key, None)
        self.access_times.pop(key, None)
    
    def clear(self):
        """清空缓存"""
        self.cache.clear()
        self.access_times.clear()

class RedisCache:
    """Redis 缓存实现"""
    
    def __init__(self, redis_url: str, default_ttl: int = 300):
        try:
            import redis
            self.redis = redis.from_url(redis_url, decode_responses=True)
            self.default_ttl = default_ttl
            self.available = True
            logger.info(f"Redis 缓存已连接: {redis_url}")
        except Exception as e:
            logger.warning(f"Redis 连接失败，将降级到内存缓存: {e}")
            self.available = False
    
    def get(self, key: str) -> Optional[Any]:
        if not self.available: return None
        try:
            val = self.redis.get(key)
            return json.loads(val) if val else None
        except Exception as e:
            logger.error(f"Redis 获取失败: {e}")
            return None
            
    def set(self, key: str, value: Any, ttl: int = None):
        if not self.available: return
        try:
            self.redis.setex(
                key, 
                ttl or self.default_ttl, 
                json.dumps(value, ensure_ascii=False)
            )
        except Exception as e:
            logger.error(f"Redis 设置失败: {e}")
            
    def delete(self, key: str):
        if not self.available: return
        try:
            self.redis.delete(key)
        except Exception as e:
            logger.error(f"Redis 删除失败: {e}")

    def clear(self):
        if not self.available: return
        try:
            self.redis.flushdb()
        except Exception as e:
            logger.error(f"Redis 清空失败: {e}")

class FileCache:
    """文件系统缓存实现 - 用于多进程间的状态同步 (Fallback)"""
    
    def __init__(self, cache_dir: str = None, default_ttl: int = 300):
        if not cache_dir:
            # 默认使用数据目录下的 cache 文件夹
            base_dir = os.path.dirname(os.path.abspath(__file__))
            cache_dir = os.path.join(base_dir, 'data', 'cache')
            
        self.cache_dir = cache_dir
        self.default_ttl = default_ttl
        os.makedirs(self.cache_dir, exist_ok=True)
        
    def _get_path(self, key: str) -> str:
        # 移除非法字符
        safe_key = "".join([c for c in key if c.isalnum() or c in ('-', '_')]).rstrip()
        return os.path.join(self.cache_dir, f"{safe_key}.json")

    def get(self, key: str) -> Optional[Any]:
        path = self._get_path(key)
        if not os.path.exists(path):
            return None
        
        try:
            # 检查有效期
            mtime = os.path.getmtime(path)
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            expire_at = data.get('_expire_at', 0)
            if expire_at > 0 and time.time() > expire_at:
                return None
                
            return data.get('value')
        except:
            return None
            
    def set(self, key: str, value: Any, ttl: int = None):
        path = self._get_path(key)
        expire_at = time.time() + (ttl or self.default_ttl)
        data = {
            'value': value,
            '_expire_at': expire_at,
            '_updated_at': time.time()
        }
        
        temp_path = path + ".tmp"
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
            os.replace(temp_path, path)
        except Exception as e:
            logger.error(f"FileCache 设置失败: {e}")

    def delete(self, key: str):
        path = self._get_path(key)
        if os.path.exists(path):
            try:
                os.remove(path)
            except:
                pass

    def clear(self):
        """清空缓存目录"""
        import shutil
        if os.path.exists(self.cache_dir):
            shutil.rmtree(self.cache_dir)
            os.makedirs(self.cache_dir, exist_ok=True)

class CacheManager:
    """统一缓存管理器"""
    
    def __init__(self):
        self._memory = MemoryCache()
        self._redis = None
        self._init_redis()
        # 初始化文件缓存作为多进程 fallback
        base_dir = os.path.dirname(os.path.abspath(__file__))
        cache_dir = os.path.join(base_dir, 'data', 'cache')
        self._file = FileCache(cache_dir=cache_dir)
        self.stats = {'hits': 0, 'misses': 0}
        
    def _init_redis(self):
        redis_url = Config.REDIS_URL
        if redis_url:
            self._redis = RedisCache(redis_url)
            
    def get(self, key: str) -> Optional[Any]:
        val = None
        # 优先尝试 Redis
        if self._redis and self._redis.available:
            val = self._redis.get(key)
            
        # 降级到内存
        if val is None:
            val = self._memory.get(key)
            
        if val is not None:
            self.stats['hits'] += 1
        else:
            self.stats['misses'] += 1
        return val
        
    def set(self, key: str, value: Any, ttl: int = None):
        self._memory.set(key, value, ttl)
        if self._redis and self._redis.available:
            self._redis.set(key, value, ttl)
            
    def delete(self, key: str):
        self._memory.delete(key)
        if self._redis and self._redis.available:
            self._redis.delete(key)
            
    def clear(self):
        self._memory.clear()
        self._file.clear()
        if self._redis and self._redis.available:
            self._redis.clear()

    def shared_get(self, key: str) -> Optional[Any]:
        """获取多进程间共享的状态 (Redis > File)"""
        if self._redis and self._redis.available:
            return self._redis.get(key)
        return self._file.get(key)

    def shared_set(self, key: str, value: Any, ttl: int = 3600):
        """设置多进程间共享的状态 (Redis & File)"""
        if self._redis and self._redis.available:
            self._redis.set(key, value, ttl)
        # 无论是否有 Redis，都同步一份到文件，确保绝对可靠
        self._file.set(key, value, ttl)

    def get_stats(self):
        """获取命中率统计"""
        total = self.stats['hits'] + self.stats['misses']
        hit_rate = (self.stats['hits'] / total * 100) if total > 0 else 0
        return {
            'hits': self.stats['hits'],
            'misses': self.stats['misses'],
            'hit_rate': f"{hit_rate:.1f}%",
            'memory_keys': len(self._memory.cache),
            'redis_available': self._redis.available if self._redis else False,
            'cache_type': 'Redis' if self._redis and self._redis.available else 'Memory'
        }

    def cleanup_expired(self):
        """手动触发内存缓存清理（Redis 自动处理）"""
        # 内存缓存通过 get() 时主动清理，这里可以执行额外的整理逻辑
        pass

# 全局单例
cache_manager = CacheManager()

def cache_result(key_prefix: str, ttl: int = 300):
    """缓存装饰器"""
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            # 生成简单的缓存键
            arg_str = ":".join([str(a) for a in args])
            kwarg_str = ":".join([f"{k}={v}" for k, v in kwargs.items()])
            key = f"{key_prefix}:{arg_str}:{kwarg_str}"
            
            cached = cache_manager.get(key)
            if cached is not None:
                return cached
                
            result = f(*args, **kwargs)
            cache_manager.set(key, result, ttl)
            return result
        return wrapper
    return decorator

def cache_key(*parts) -> str:
    """生成标准化的缓存键"""
    return ":".join(str(part) for part in parts if part is not None)

# 预定义的缓存键前缀
class CacheKeys:
    """
    缓存键常量
    使用统一命名空间：{module}:{key}
    """
    # 基础数据
    STATS = "data:stats"
    CATEGORIES = "data:categories"
    RESOURCES = "data:resources"

    # 搜索相关
    SEARCH = "search:results"

    # 健康检查
    HEALTH = "health:status"

    # 配置相关
    CONFIG = "app:config"
    VERIFICATION = "app:verification"

    # 任务相关
    TASKS = "task:queue"

    # 爬虫状态
    CRAWL_STATUS = "crawl:status"
    CRAWL_PROGRESS = "crawl:progress"
    CRAWL_CONTROL = "crawl:control"
    CRAWL_STATE = "crawl:state"
    CRAWL_UNIFIED_STATE = "crawl:unified_state"
    IS_CRAWLING = "crawl:is_running"
    IS_PAUSED = "crawl:is_paused"

    @classmethod
    def build(cls, *parts) -> str:
        """
        构建标准化的缓存键

        Args:
            *parts: 键的各个部分

        Returns:
            标准化的缓存键，格式为 part1:part2:part3

        Example:
            CacheKeys.build("search", "category", "news", "page", 1)
            # 返回: "search:category:news:page:1"
        """
        return ":".join(str(part) for part in parts if part is not None)