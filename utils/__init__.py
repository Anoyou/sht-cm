#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
共享工具模块 - 提供日志配置、数据库锁重试装饰器及其他通用辅助函数
旨在减少模块间的代码冗余，提高系统整体的健壮性
"""

import logging
import os
import time
import sqlite3
from functools import wraps
from contextlib import contextmanager
from typing import Callable, TypeVar, Tuple, Optional, Generator, Any
from flask import Flask

try:
    from sqlalchemy.exc import OperationalError as SAOperationalError
except ImportError:
    SAOperationalError = None

logger = logging.getLogger(__name__)

# 强制在模块加载时静默冗余日志 (httpx, telegram)
for _logger in ['httpx', 'httpcore', 'telegram', 'telegram.ext']:
    logging.getLogger(_logger).setLevel(logging.WARNING)

# TypeVar for decorator type hints
F = TypeVar('F', bound=Callable[..., Any])

def retry_on_lock(max_retries: int = 3, initial_delay: float = 0.1, backoff_factor: float = 2) -> Callable[[F], F]:
    """
    数据库锁定重试装饰器

    自动处理 sqlite3.OperationalError 和 sqlalchemy.exc.OperationalError
    当遇到 "database is locked" 错误时进行指数退避重试
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # 检查是否是数据库锁定错误
                    is_lock_error = False
                    error_msg = str(e).lower()

                    # 检查 SQLite 错误
                    if isinstance(e, sqlite3.OperationalError) and "database is locked" in error_msg:
                        is_lock_error = True

                    # 检查 SQLAlchemy 错误
                    elif SAOperationalError and isinstance(e, SAOperationalError) and "database is locked" in error_msg:
                        is_lock_error = True

                    if is_lock_error and attempt < max_retries - 1:
                        logger.warning(f"数据库锁定，重试 {attempt + 1}/{max_retries}: {func.__name__}")

                        # 尝试回滚事务（如果存在）
                        # 检测 args[0] 是否是类实例且有 db.session 属性 (针对 SQLAlchemy)
                        if len(args) > 0 and hasattr(args[0], 'session') and hasattr(args[0].session, 'rollback'):
                             try:
                                 args[0].session.rollback()
                             except:
                                 pass
                        # 全局 db session 回滚 (针对 Flask-SQLAlchemy)
                        try:
                            from models import db
                            if db.session:
                                db.session.rollback()
                        except:
                            pass

                        time.sleep(delay)
                        delay *= backoff_factor
                        continue

                    # 如果不是锁定错误或重试耗尽，抛出异常
                    raise e
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Flask应用单例
_flask_app_instance = None

def get_flask_app() -> 'Flask':
    """
    获取Flask应用实例（单例模式）

    Returns:
        Flask应用实例
    """
    global _flask_app_instance
    if _flask_app_instance is None:
        import importlib
        logger.debug("创建新的Flask应用实例")
        app_module = importlib.import_module('app')
        get_app_instance = getattr(app_module, 'get_app_instance')
        _flask_app_instance = get_app_instance(
            enable_background_services=False,
            enable_task_manager=False
        )
    return _flask_app_instance

def get_flask_app_context() -> 'Flask':
    """
    获取Flask应用实例（用于爬虫）

    支持爬虫模式：设置 CRAWLER_MODE 环境变量可以放宽配置验证

    Returns:
        Flask应用实例（调用方需要自己调用 .app_context()）
    """
    # 设置爬虫模式环境变量，放宽配置验证（如果需要）
    # 不再强制设置 CRAWLER_MODE,应该尊重用户配置
    # if not os.environ.get('CRAWLER_MODE'):
    #     os.environ['CRAWLER_MODE'] = 'true'

    # 返回 app 实例，不是上下文对象
    return get_flask_app()

def reset_flask_app() -> None:
    """重置Flask应用实例（主要用于测试）"""
    global _flask_app_instance
    _flask_app_instance = None
    logger.debug("Flask应用实例已重置")

@contextmanager
def db_session_context() -> Generator[Any, None, None]:
    """
    统一的数据库会话上下文管理器

    自动处理提交、回滚和关闭操作

    使用示例:
        with db_session_context() as session:
            resources = session.query(Resource).all()
    """
    from models import db

    app = get_flask_app()
    with app.app_context():
        try:
            yield db.session
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise
        finally:
            db.session.close()

def get_database_paths() -> Tuple[str, str]:
    """
    获取数据库文件路径

    Returns:
        tuple: (main_db_path, failed_db_path)
    """
    from configuration import Config
    main_db = Config.get_path('db_path')
    failed_db = Config.get_path('failed_db_path')
    return main_db, failed_db

def setup_logging(log_level: str = 'INFO') -> None:
    """
    设置统一的日志配置

    Args:
        log_level: 日志级别（DEBUG, INFO, WARNING, ERROR）
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 设置第三方库的日志级别
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)

    # 深度压制冗余日志 (httpx, telegram, httpcore)
    for logger_name in [
        'httpx', 'httpcore', 'httpcore.http11', 'httpcore.connection',
        'telegram', 'telegram.ext', 'telegram.ext.ExtBot', 'telegram.ext.Updater',
        'crawler_control'
    ]:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    # 大幅减少SQLAlchemy日志输出
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.orm').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.dialects').setLevel(logging.WARNING)
