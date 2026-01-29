#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一配置管理模块
合并了原有的 config.py, config_manager.py 和 crawl_config_manager.py
"""

import os
import json
import logging
import time
import threading
import copy
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse

# 全局版本号
__version__ = "1.5.6"

# 配置日志
logger = logging.getLogger(__name__)

class UnifiedConfigManager:
    """
    统一配置管理器 - 单例模式
    管理所有应用配置和爬虫配置，处理持久化和自动迁移
    """
    _instance = None
    _lock = threading.RLock()
    _pid = None  # 记录进程 ID，用于检测 fork

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(UnifiedConfigManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        # 检测是否是新进程
        current_pid = os.getpid()
        if self._initialized and self._pid == current_pid:
            return

        # 新进程或首次初始化
        if self._initialized:
            logger.warning(f"检测到进程变化 {self._pid} -> {current_pid}，重新初始化配置管理器")
            self._initialized = False

        self._initialized = True
        self._pid = current_pid

        # 简单内存缓存，减少IO
        self._get_cache = {}
        self._last_cache_clear = time.time()

        # 确定路径（更严格的 Docker 检测）
        # Docker 环境通常有以下特征之一：
        # 1. /.dockerenv 文件存在
        # 2. /proc/1/cgroup 包含 docker 字符串
        # 3. 环境变量包含容器标记
        has_docker_env = os.path.exists('/.dockerenv') or 'docker' in os.environ.get('PATH', '').lower()
        has_app_data = os.path.exists('/app/data')
        self.is_docker = has_docker_env and has_app_data

        if self.is_docker:
            self.data_dir = '/app/data'
            self.config_dir = '/app/data/config'
            self.config_file = '/app/data/config/config.json'
            # 旧文件路径用于迁移
            self.old_app_config_file = '/app/data/config/app_config.json'
            self.old_crawl_config_file = '/app/data/crawl_config.json'
            self.log_dir = '/app/data/logs'
            self.db_path = '/app/data/sht.db'
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.data_dir = os.path.join(base_dir, 'data')
            self.config_dir = os.path.join(self.data_dir, 'config')
            self.config_file = os.path.join(self.config_dir, 'config.json')
            # 旧文件路径用于迁移
            self.old_app_config_file = os.path.join(self.config_dir, 'app_config.json')
            self.old_crawl_config_file = os.path.join(self.data_dir, 'crawl_config.json')
            self.log_dir = os.path.join(self.data_dir, 'logs')
            self.db_path = os.path.join(self.data_dir, 'sht.db')

        # 确保目录存在
        os.makedirs(self.config_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # 默认配置结构
        self.default_config = {
            # --- 应用配置 ---
            'app': {
                'LOG_LEVEL': 'INFO',
                'LOG_BUFFER_SIZE': 10000,  # Web日志缓冲区大小（条数）
                'PROXY': '',
                'BYPASS_URL': '',
                'FLARE_SOLVERR_URL': '',
                'TG_BOT_TOKEN': '',
                'TG_NOTIFY_CHAT_ID': '',
                'ITEMS_PER_PAGE': 20,
                'MONITORING_ENABLED': True,
                'MONITORING_INTERVAL': 600,
                'CACHE_DEFAULT_TTL': 300,
                'CACHE_STATS_TTL': 300,
                'CACHE_CATEGORIES_TTL': 600,
                'WEB_BASE_URL': 'http://localhost:5000',
                'SECRET_KEY': 'sht-default-secret-key',
                'ALLOWED_HOSTS': ['*'],
                'TZ_OFFSET_HOURS': 0,
                'AUTO_CRAWL_ENABLED': False,
                'AUTO_CRAWL_TIME': '03:00',
                'CRAWLER_MODE': 'async',  # 爬虫模式 - 'sync'(串行) / 'thread'(多线程) / 'async'(异步)
                'CRAWLER_MAX_CONCURRENCY': 20,  # 异步模式最大并发数
                'CRAWLER_THREAD_COUNT': 10,     # 多线程模式线程数
                'CRAWLER_ASYNC_DELAY_MIN': 0.5,  # 异步模式最小随机延迟(秒)
                'CRAWLER_ASYNC_DELAY_MAX': 1.5,  # 异步模式最大随机延迟(秒)
                'CRAWLER_SYNC_DELAY_MIN': 0.3,  # 同步模式最小随机延迟(秒)
                'CRAWLER_SYNC_DELAY_MAX': 0.8,   # 同步模式最大随机延迟(秒)
                'HEARTBEAT_INTERVAL': 60,  # 心跳通知间隔(秒)
                'SAFE_MODE': False,  # 安全模式开关，开启后资源卡片图片被模糊遮罩
                'GLOBAL_ERROR_THRESHOLD': 300,  # 全局错误阈值，超过此值任务自动停止
            },
            # --- 路径配置 (集中管理硬编码路径) ---
            'paths': {
                'data_dir': self.data_dir,
                'config_dir': self.config_dir,
                'log_dir': self.log_dir,
                'db_path': self.db_path,
                'failed_db_path': os.path.join(self.data_dir, 'failed_tids.db'),
                'log_file': os.path.join(self.log_dir, 'app.log'),
                'summary_json': os.path.join(self.log_dir, 'summary.json'),
                'crawl_options': os.path.join(self.log_dir, 'crawl_options.json'),
                'crawler_state': os.path.join(self.config_dir, 'crawler_state.json'),
                'signal_queue': os.path.join(self.config_dir, 'signal_queue.json'),
                'task_lock_dir': self.config_dir,
                'task_progress': os.path.join(self.config_dir, 'task_progress.json'),
                'telegram_templates': os.path.join(self.config_dir, 'telegram_templates.py')
            },
            # --- 爬虫配置 ---
            'crawler': {
                "selected_forums": [],
                "date_mode": "1day",
                "date_value": "",
                "max_pages": 3,
                "crawl_options": {
                    "delay": 1,
                    "timeout": 30,
                    "retry_count": 3
                },
                "last_update": 0,
                "version": "1.1",
                "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1"
            }
        }
        
        # 加载配置
        self._config = self._load_or_migrate_config()
        self._last_mtime = self._get_file_mtime()
        self._last_reload_check = time.time()
        
    def _get_file_mtime(self) -> float:
        """获取配置文件修改时间"""
        try:
            if os.path.exists(self.config_file):
                return os.path.getmtime(self.config_file)
        except:
            pass
        return 0.0

    def _check_reload(self):
        """检查并重新加载配置（跨进程同步）"""
        # 移除 1秒节流，确保变更立即可见 (os.path.getmtime 是极快的 fstat 调用)
        with self._lock:
            current_mtime = self._get_file_mtime()
            # 使用 != 而不是 >，防止某些文件系统或时间同步导致的边缘问题
            if current_mtime != getattr(self, '_last_mtime', 0):
                logger.debug(f"检测到配置文件已变更，正在重新加载 ({current_mtime} != {self._last_mtime})")
                new_config = self._load_or_migrate_config()
                if new_config:
                    self._config = new_config
                    self._last_mtime = current_mtime
                    # 重新应用关键环境变量 (特别是 CRAWLER_MODE 等)
                    self._sync_env_vars()
                    # 清除缓存
                    self._get_cache.clear()
        
        self._last_reload_check = time.time()

    def _sync_env_vars(self):
        """同步配置到环境变量"""
        if 'app' in self._config:
            for k in ['LOG_LEVEL', 'PROXY', 'CRAWLER_MODE']:
                v = self._config['app'].get(k)
                if v is not None:
                    os.environ[k] = str(v).lower() if k == 'CRAWLER_MODE' else str(v)
    def _load_or_migrate_config(self) -> Dict[str, Any]:
        """加载配置，如果不存在则尝试从旧文件迁移"""
        # 1. 如果新配置文件存在，直接加载
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    saved_config = json.load(f)

                # 验证配置结构
                if not isinstance(saved_config, dict):
                    logger.error(
                        f"配置文件格式错误: {self.config_file}, "
                        f"期望 dict 类型，实际 {type(saved_config).__name__}, "
                        f"将使用默认配置"
                    )
                    return copy.deepcopy(self.default_config)

                # 使用深拷贝合并默认配置（确保结构完整且独立）
                merged_config = copy.deepcopy(self.default_config)

                # 检查是否有缺失的键
                missing_keys = []
                for section in ['app', 'crawler']:
                    if section in self.default_config:
                        for key in self.default_config[section]:
                            if section not in saved_config or key not in saved_config[section]:
                                missing_keys.append(f"{section}.{key}")

                # 递归更新配置（保存的配置覆盖默认配置）
                try:
                    if 'app' in saved_config:
                        merged_config['app'].update(saved_config['app'])
                    if 'crawler' in saved_config:
                        merged_config['crawler'].update(saved_config['crawler'])
                except (TypeError, AttributeError) as e:
                    logger.error(
                        f"配置合并失败: {e}, "
                        f"将使用默认配置"
                    )
                    return copy.deepcopy(self.default_config)

                logger.info(f"已加载配置文件: {self.config_file}")

                # 如果有缺失的键，保存配置文件以补全
                if missing_keys:
                    logger.info(f"检测到 {len(missing_keys)} 个缺失的配置键，自动补全: {', '.join(missing_keys)}")
                    self._save_config_data(merged_config)
                    logger.info("✓ 配置文件已更新，补全缺失的键")

                return merged_config
            except json.JSONDecodeError as e:
                logger.error(
                    f"配置文件JSON解析失败: {e}, "
                    f"文件: {self.config_file}, 将使用默认配置"
                )
                # 备份损坏的配置文件
                backup_file = self.config_file + '.corrupted'
                try:
                    import shutil
                    shutil.copy2(self.config_file, backup_file)
                    logger.warning(f"已备份损坏的配置文件到: {backup_file}")
                except Exception as backup_err:
                    logger.error(f"备份配置文件失败: {backup_err}")
                return copy.deepcopy(self.default_config)
            except Exception as e:
                logger.error(
                    f"加载配置文件失败: {e}, "
                    f"将使用默认配置"
                )
                return copy.deepcopy(self.default_config)

        # 2. 尝试迁移旧配置
        logger.info("未发现新配置文件，检查旧配置进行迁移...")
        migrated_config = copy.deepcopy(self.default_config)
        migration_happened = False

        # 迁移 app_config.json
        if os.path.exists(self.old_app_config_file):
            try:
                with open(self.old_app_config_file, 'r', encoding='utf-8') as f:
                    old_app_config = json.load(f)

                # 验证旧配置格式
                if isinstance(old_app_config, dict):
                    # 映射旧键值
                    for k, v in old_app_config.items():
                        if k in migrated_config['app']:
                            migrated_config['app'][k] = v

                    logger.info(f"已迁移应用配置: {self.old_app_config_file}")
                    migration_happened = True
                else:
                    logger.warning(
                        f"旧应用配置文件格式错误: {self.old_app_config_file}, "
                        f"期望 dict 类型，实际 {type(old_app_config).__name__}, "
                        f"跳过迁移"
                    )
            except json.JSONDecodeError as e:
                logger.warning(f"迁移应用配置失败（JSON解析错误）: {e}")
            except Exception as e:
                logger.warning(f"迁移应用配置失败: {e}")

        # 迁移 crawl_config.json
        if os.path.exists(self.old_crawl_config_file):
            try:
                with open(self.old_crawl_config_file, 'r', encoding='utf-8') as f:
                    old_crawl_config = json.load(f)

                # 验证旧配置格式
                if isinstance(old_crawl_config, dict):
                    # 映射旧键值
                    for k, v in old_crawl_config.items():
                        if k in migrated_config['crawler']:
                            migrated_config['crawler'][k] = v

                    logger.info(f"已迁移爬虫配置: {self.old_crawl_config_file}")
                    migration_happened = True
                else:
                    logger.warning(
                        f"旧爬虫配置文件格式错误: {self.old_crawl_config_file}, "
                        f"期望 dict 类型，实际 {type(old_crawl_config).__name__}, "
                        f"跳过迁移"
                    )
            except json.JSONDecodeError as e:
                logger.warning(f"迁移爬虫配置失败（JSON解析错误）: {e}")
            except Exception as e:
                logger.warning(f"迁移爬虫配置失败: {e}")
                
        # 如果发生了迁移或文件不存在，保存新配置
        self._save_config_data(migrated_config)
        return migrated_config

    def _save_config_data(self, config_data: Dict[str, Any]) -> bool:
        """保存配置数据到文件 (原子写入)"""
        temp_file = self.config_file + ".tmp"
        try:
            # 1. 写入临时文件
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, ensure_ascii=False, indent=2)
            
            # 2. 修改权限 (可选，确保一致性)
            try:
                os.chmod(temp_file, 0o644)
            except:
                pass
                
            # 3. 原子重命名
            os.replace(temp_file, self.config_file)
            
            # 4. 更新本地 mtime，防止自触发重新加载
            with self._lock:
                self._last_mtime = os.path.getmtime(self.config_file)
                self._last_reload_check = time.time()
                
            logger.debug(f"配置已保存并同步: {self.config_file}")
            return True
        except Exception as e:
            logger.error(f"保存配置文件失败: {e}")
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
            return False

    def save(self) -> bool:
        """保存当前配置到文件"""
        with self._lock:
            return self._save_config_data(self._config)

    # --- 通用获取/设置方法 ---
    
    def get(self, key: str, default: Any = None, section: str = 'app') -> Any:
        """
        获取配置值
        优先级: 内存/文件配置 > 环境变量 > 默认值
        （Docker 和本地环境统一逻辑：配置文件优先，环境变量作为初始默认值）
        """
        self._check_reload()

        # 检查缓存 TTL (例如 60秒清除一次)
        now = time.time()
        if now - getattr(self, '_last_cache_clear', 0) > 60:
             self._get_cache.clear()
             self._last_cache_clear = now

        # 尝试从缓存获取
        cache_key = f"{section}:{key}:{default}"
        if cache_key in self._get_cache:
            return self._get_cache[cache_key]

        # 1. 优先从内存/文件配置获取（允许 Web 界面覆盖）
        file_val = self._config.get(section, {}).get(key)
        if file_val is not None and file_val != '':
            return file_val

        # 2. 如果文件没有，尝试从环境变量获取 (仅针对 app 配置)
        if section == 'app':
            env_val = os.environ.get(key)
            if env_val is not None and env_val != '':
                default_val = self.default_config['app'].get(key)
                result = env_val

                if default_val is not None:
                    try:
                        if isinstance(default_val, bool):
                            result = env_val.lower() in ('true', '1', 'yes', 'on')
                        elif isinstance(default_val, int):
                            result = int(env_val)
                        elif isinstance(default_val, float):
                            result = float(env_val)
                        elif isinstance(default_val, list):
                            result = [item.strip() for item in env_val.split(',') if item.strip()]
                    except (ValueError, TypeError, AttributeError) as e:
                        logger.warning(
                            f"[CONFIG] 环境变量类型转换失败: {key}={env_val}, "
                            f"期望类型: {type(default_val).__name__}, 错误: {e}"
                        )
                        result = env_val

                return result

        # 最后尝试环境变量
        if section == 'app':
            env_val = os.environ.get(key)
            if env_val is not None:
                default_val = self.default_config['app'].get(key)
                result = env_val

                if default_val is not None:
                    try:
                        if isinstance(default_val, bool):
                            result = env_val.lower() in ('true', '1', 'yes', 'on')
                        elif isinstance(default_val, int):
                            result = int(env_val)
                        elif isinstance(default_val, float):
                            result = float(env_val)
                        elif isinstance(default_val, list):
                            result = [item.strip() for item in env_val.split(',') if item.strip()]
                    except (ValueError, TypeError, AttributeError) as e:
                        logger.warning(
                            f"[CONFIG] 环境变量类型转换失败: {key}={env_val}, "
                            f"期望类型: {type(default_val).__name__}, 错误: {e}, "
                            f"使用默认值: {default_val}"
                        )
                        result = default_val
                else:
                    result = env_val

                return result

        self._get_cache[cache_key] = default
        return default

    def set(self, key: str, value: Any, section: str = 'app') -> bool:
        """设置配置值并保存"""
        with self._lock:
            if section not in self._config:
                self._config[section] = {}
            
            self._config[section][key] = value
            
            # 如果是 app 配置，同时也设置环境变量 (临时生效)
            if section == 'app' and isinstance(value, (str, int, float, bool)):
                # 特殊处理：确保 CRAWLER_MODE 始终是小写字符串
                if key == 'CRAWLER_MODE':
                    if isinstance(value, bool):
                         # 如果错误地传入了 boolean，转换为默认值
                         os.environ[key] = 'async'
                         logger.warning(f"⚠️ [CONFIG] CRAWLER_MODE 接收到 boolean 值，已转换为默认值 'async'")
                    else:
                         # 确保是小写字符串
                         os.environ[key] = str(value).lower()
                else:
                    os.environ[key] = str(value)
            
            # 特殊处理：更新 timestamp
            if section == 'crawler':
                self._config['crawler']['last_update'] = time.time()
                
            return self.save()
            
    def update(self, data: Dict[str, Any], section: str = 'app') -> bool:
        """批量更新配置"""
        with self._lock:
            if section not in self._config:
                self._config[section] = {}

            # 记录配置变更
            changed_keys = []
            for k, v in data.items():
                old_val = self._config[section].get(k)
                if old_val != v:
                    changed_keys.append(f"{k}: {old_val} -> {v}")

            self._config[section].update(data)

            # 如果是 app 配置，同步环境变量
            if section == 'app':
                for k, v in data.items():
                    # 特殊处理：确保 CRAWLER_MODE 始终是小写字符串
                    if k == 'CRAWLER_MODE':
                        if isinstance(v, bool):
                             # 如果错误地传入了 boolean，转换为默认值
                             os.environ[k] = 'async'
                        else:
                             # 确保是小写字符串
                             os.environ[k] = str(v).lower()
                    else:
                        os.environ[k] = str(v)

            if section == 'crawler':
                self._config['crawler']['last_update'] = time.time()

            # 记录配置变更日志
            if changed_keys:
                logger.debug(f"[CONFIG] 配置变更 ({section}): {', '.join(changed_keys)}")

            return self.save()

    # --- 专用接口 (兼容原有 Manager) ---

    # 1. 兼容 ConfigManager
    def get_app_config(self, key: str, default: Any = None) -> Any:
        return self.get(key, default, section='app')
        
    def set_app_config(self, key: str, value: Any) -> bool:
        return self.set(key, value, section='app')

    def get_all_app_config(self) -> Dict[str, Any]:
        """获取所有应用配置（合并环境变量）"""
        self._check_reload()
        config = self._config['app'].copy()
        # 覆盖环境变量
        for k in config.keys():
            val = self.get(k, config[k], section='app')
            config[k] = val
        return config

    def apply_log_level(self):
        """应用日志等级"""
        log_level = self.get('LOG_LEVEL', 'INFO').upper()
        try:
            numeric_level = getattr(logging, log_level, logging.INFO)
            logging.getLogger().setLevel(numeric_level)
            # 设置所有 handler
            for handler in logging.getLogger().handlers:
                handler.setLevel(numeric_level)
            logger.info(f"日志等级已应用: {log_level}")
        except Exception as e:
            logger.error(f"应用日志等级失败: {e}")

    # 2. 兼容 CrawlConfigManager
    def get_crawl_config(self, key: str = None) -> Any:
        self._check_reload()
        if key:
            return self._config['crawler'].get(key)
        return self._config['crawler']

    def set_crawl_config(self, key: str = None, value: Any = None, data: Dict[str, Any] = None) -> bool:
        if data:
            return self.update(data, section='crawler')
        if key and value is not None:
            # 兼容旧逻辑的特殊键名处理
            if key == 'SELECTED_FORUMS':
                key = 'selected_forums'
            return self.set(key.lower(), value, section='crawler')
        return False
        
    def get_crawl_summary(self) -> Dict[str, Any]:
        cfg = self._config['crawler']
        last_update = cfg.get("last_update", 0)
        last_update_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_update)) if last_update > 0 else "从未更新"

        return {
            "selected_forums_count": len(cfg.get("selected_forums", [])),
            "selected_forums": cfg.get("selected_forums", []),
            "date_mode": cfg.get("date_mode", "1day"),
            "date_value": cfg.get("date_value", ""),
            "page_mode": cfg.get("page_mode", "fixed"),  # 添加 page_mode
            "max_pages": cfg.get("max_pages", 3),
            "smart_limit": cfg.get("smart_limit", 500),  # 添加 smart_limit
            "crawl_options": cfg.get("crawl_options", {}),
            "last_update": last_update_str,
            "config_exists": os.path.exists(self.config_file)
        }

# 导出 config_manager 实例供其他模块使用
config_manager = UnifiedConfigManager()


class Config:
    """
    Flask 应用配置类
    兼容原有的 Config 类接口，底层代理到 config_manager
    """
    # 获取管理器实例（避免重复初始化）
    _mgr = config_manager
    
    # 版本号
    VERSION = __version__
    
    # --- 静态/只读配置 ---
    IS_DOCKER = _mgr.is_docker
    DEFAULT_DB_PATH = _mgr.db_path

    # 构造安全的 SQLite URI
    if os.environ.get('DATABASE_URL'):
        SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')
    else:
        db_path_abs = os.path.abspath(_mgr.db_path)
        # SQLite URI 格式：sqlite:///path/to/file.db（三个斜杠）
        # 如果路径包含空格或特殊字符，需要进行 URL 编码
        from urllib.parse import quote
        if ' ' in db_path_abs or any(ord(c) > 127 for c in db_path_abs):
            encoded_path = quote(db_path_abs)
            SQLALCHEMY_DATABASE_URI = f'sqlite:///{encoded_path}'
        else:
            SQLALCHEMY_DATABASE_URI = f'sqlite:///{db_path_abs}'

    logger.debug(f"🛠️ [DB-CONFIG] 数据库 URI: {SQLALCHEMY_DATABASE_URI}")

    # SQLite 配置，使用 NullPool 避免连接池问题
    # SQLite 不需要连接池，使用 NullPool 可以避免文件锁定问题
    from sqlalchemy.pool import NullPool
    SQLALCHEMY_ENGINE_OPTIONS = {
        'poolclass': NullPool,
        'echo': False
    }
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Dateline 映射 (静态常量)
    DATELINE_MAP = {
        '1day': 86400,
        '2day': 172800,
        '3day': 259200,
        '1week': 604800,
        '1month': 2592000,
        '3month': 7776000,
        '6month': 15552000,
        '1year': 31536000,
        'thisyear': 0,
        'all': 0
    }
    
    # 路径配置
    LOG_DIR = _mgr.log_dir

    @staticmethod
    def get_path(key: str) -> str:
        """
        获取路径配置

        Args:
            key: 路径键名 (例如 'summary_json', 'crawler_state')

        Returns:
            str: 路径值
        """
        mgr = Config._mgr
        # 从 paths 配置中获取
        return mgr.get(key, '', section='paths')
    
    # --- 动态映射属性 ---
    # Flask app.config.from_object 读取的是类属性的值
    # 因此这里在模块加载时直接读取当前配置
    # 注意：这意味着如果配置在运行时改变，Flask应用内的配置不会自动更新（这符合Flask通常的行为）
    
    LOG_LEVEL = _mgr.get('LOG_LEVEL', 'INFO')
    SECRET_KEY = _mgr.get('SECRET_KEY')
    ITEMS_PER_PAGE = int(_mgr.get('ITEMS_PER_PAGE', 20))
    TG_BOT_TOKEN = _mgr.get('TG_BOT_TOKEN')
    REDIS_URL = os.environ.get('REDIS_URL')
    
    # 监控配置
    MONITORING_ENABLED = _mgr.get('MONITORING_ENABLED', True)
    MONITORING_INTERVAL = int(_mgr.get('MONITORING_INTERVAL', 1800))
    TIMEZONE_OFFSET_HOURS = int(_mgr.get('TZ_OFFSET_HOURS', 0))
    
    # 代理配置
    PROXY = _mgr.get('PROXY')
    BYPASS_URL = _mgr.get('BYPASS_URL')
    FLARE_SOLVERR_URL = _mgr.get('FLARE_SOLVERR_URL')
    TG_NOTIFY_CHAT_ID = _mgr.get('TG_NOTIFY_CHAT_ID')
    
    # 静态配置
    VERSION = __version__
    
    # 兼容性代理方法，如果代码中有 Config.get_config_summary() 这种调用
    @classmethod
    def get_config_summary(cls):
        """获取配置摘要"""
        mgr = cls._mgr
        return {
            'version': __version__,
            'is_docker': cls.IS_DOCKER,
            'database_type': 'SQLite',
            'redis_enabled': bool(cls.REDIS_URL),
            'telegram_enabled': bool(mgr.get('TG_BOT_TOKEN')),
            'proxy_enabled': bool(mgr.get('PROXY')),
            'bypass_enabled': bool(mgr.get('BYPASS_URL')),
            'flaresolverr_enabled': bool(mgr.get('FLARE_SOLVERR_URL')),
            'monitoring_enabled': mgr.get('MONITORING_ENABLED'),
            'log_level': mgr.get('LOG_LEVEL'),
            'timezone_offset': mgr.get('TZ_OFFSET_HOURS')
        }

    @classmethod
    def validate_config(cls):
        """验证关键配置"""
        if not cls.SQLALCHEMY_DATABASE_URI:
            raise ValueError("未配置数据库连接 URI")
        # 其他关键配置验证可以在此添加

# 为了保持导入兼容性，重新导出
# 注意：这只是为了过渡，最终应该直接使用 config_manager
# crawl_config_manager = config_manager # 接口不完全一致，需要适配器
# 这里先不定义 crawl_config_manager，而是在原文件中修改引用

