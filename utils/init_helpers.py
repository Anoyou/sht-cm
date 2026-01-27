#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化辅助函数 - 数据库初始化和后台服务启动
"""

import os
import logging
import threading


def start_sht2bm_background():
    """在后台启动 SHT2BM API 服务"""
    def run_sht2bm():
        try:
            import time
            # 等待主应用完全启动
            time.sleep(5)

            logger = logging.getLogger('sht2bm_background')
            logger.info(f"[API] SHT2BM API 服务已通过Blueprint集成")
            # SHT2BM 已改为 Blueprint 方式集成（见 sht2bm_adapter.py）
            # 不再需要单独启动服务或健康检查
            # 健康检查端点: /api/bt/health

        except Exception as e:
            logger = logging.getLogger('sht2bm_background')
            logger.error(f"✗ [API] SHT2BM后台启动失败: {e}")

    # 创建后台线程
    sht2bm_thread = threading.Thread(target=run_sht2bm, daemon=True, name='SHT2BM-Service')
    sht2bm_thread.start()

    return sht2bm_thread


def init_db_data(app):
    """初始化数据库数据 - 优化版本，避免重复初始化"""
    from models import db, Category
    from sqlalchemy import inspect, text

    logger = logging.getLogger(__name__)

    # 使用线程锁避免多进程同时初始化
    if not hasattr(init_db_data, '_lock'):
        init_db_data._lock = threading.Lock()

    with init_db_data._lock:
        # 检查是否已经初始化过
        if hasattr(init_db_data, '_initialized') and init_db_data._initialized:
            logger.debug(f"[DB] 数据库已初始化，跳过重复初始化")
            return True

        with app.app_context():
            # 确保数据目录存在（从数据库路径获取）
            from configuration import Config
            db_path = app.config.get('SQLALCHEMY_DATABASE_URI', '').replace('sqlite:///', '')
            if db_path:
                data_dir = os.path.dirname(db_path)
                if data_dir and not os.path.exists(data_dir):
                    os.makedirs(data_dir, exist_ok=True)
                    logger.info(f"✓ [DB] 创建数据目录: {data_dir}")

            # 创建数据库表并执行自愈迁移
            try:
                db.create_all()
                logger.debug(f"[DB] 基础数据库结构检查完成")

                # --- 数据库增量迁移补丁 ---
                # 解决从旧版本升级时缺少新增字段的问题 (v1.0.8 升级适配)
                # 显式使用独立连接进行结构修改，避免事务冲突
                with db.engine.connect() as conn:
                    inspector = inspect(db.engine)
                    existing_tables = inspector.get_table_names()

                    # 1. 修复 category 表
                    if 'category' in existing_tables:
                        result = conn.execute(text("PRAGMA table_info(category)"))
                        cols = [row[1] for row in result.fetchall()]

                        # 核心补齐字段清单
                        category_migrations = [
                            ('description', 'VARCHAR(500)', "''"),
                            ('is_active', 'BOOLEAN', '1'),
                            ('resource_count', 'INTEGER', '0'),
                            ('total_topics', 'INTEGER', '0'),
                            ('total_pages', 'INTEGER', '0'),
                            ('last_updated', 'DATETIME', None),
                            ('display_order', 'INTEGER', '0'),
                            ('created_at', 'DATETIME', 'CURRENT_TIMESTAMP')
                        ]

                        for col_name, col_type, default in category_migrations:
                            if col_name not in cols:
                                try:
                                    logger.info(f"[DB] 数据库自愈: 补齐字段 category.{col_name}")
                                    default_sql = f" DEFAULT {default}" if default is not None else ""
                                    conn.execute(text(f"ALTER TABLE category ADD COLUMN {col_name} {col_type}{default_sql}"))
                                    conn.commit()
                                except Exception as e:
                                    if "already exists" not in str(e):
                                        logger.warning(f"! [DB] 无法补齐字段 category.{col_name}: {e}")

                    # 2. 修复 resource 表
                    if 'resource' in existing_tables:
                        result = conn.execute(text("PRAGMA table_info(resource)"))
                        cols = [row[1] for row in result.fetchall()]
                        if 'created_at' not in cols:
                            try:
                                logger.info("[DB] 数据库自愈: 补齐字段 resource.created_at")
                                conn.execute(text("ALTER TABLE resource ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP"))
                                conn.commit()
                            except Exception as e:
                                if "already exists" not in str(e):
                                    logger.warning(f"! [DB] 无法补齐字段 resource.created_at: {e}")

                    # 3. 修复 failed_tid 表
                    if 'failed_tid' in existing_tables:
                        result = conn.execute(text("PRAGMA table_info(failed_tid)"))
                        cols = [row[1] for row in result.fetchall()]

                        failed_tid_migrations = [
                            ('detail_url', 'VARCHAR(500)', "''"),
                            ('failure_reason', 'VARCHAR(500)', "''"),
                            ('retry_count', 'INTEGER', '0'),
                            ('status', 'VARCHAR(20)', "'pending'"),
                            ('created_at', 'DATETIME', 'CURRENT_TIMESTAMP'),
                            ('updated_at', 'DATETIME', 'CURRENT_TIMESTAMP')
                        ]

                        for col_name, col_type, default in failed_tid_migrations:
                            if col_name not in cols:
                                try:
                                    logger.info(f"[DB] 数据库自愈: 补齐字段 failed_tid.{col_name}")
                                    default_sql = f" DEFAULT {default}" if default is not None else ""
                                    conn.execute(text(f"ALTER TABLE failed_tid ADD COLUMN {col_name} {col_type}{default_sql}"))
                                    conn.commit()
                                except Exception as e:
                                    if "already exists" not in str(e):
                                        logger.warning(f"! [DB] 无法补齐字段 failed_tid.{col_name}: {e}")

                logger.info(f"✓ [DB] 数据库结构自愈巡检完成")
            except Exception as e:
                logger.error(f"✗ [DB] 数据库初始化失败: {e}")
                # 不中断主程序启动

            # 初始化分类数据 - 简化版本，减少日志输出
            from constants import SECTION_MAP

            try:
                # 检查是否已有分类数据
                existing_count = Category.query.count()
                if existing_count >= len(SECTION_MAP):
                    logger.debug(f"[DB] 分类数据已存在({existing_count}个)，跳过初始化")
                    init_db_data._initialized = True
                    return True

                # 批量查询现有分类
                existing_categories = Category.query.all()
                existing_fids = {cat.fid for cat in existing_categories}

                # 批量插入缺失的分类
                new_categories = []
                for fid, name in SECTION_MAP.items():
                    if fid not in existing_fids:
                        new_categories.append(Category(fid=fid, name=name))

                if new_categories:
                    db.session.add_all(new_categories)
                    db.session.commit()
                    logger.info(f"✓ [DB] 新增分类: {len(new_categories)}个")

                logger.info(f"✓ [DB] 分类数据初始化完成")
                init_db_data._initialized = True
                return True
            except Exception as e:
                logger.error(f"✗ [DB] 分类数据初始化失败: {e}")
                db.session.rollback()
                return False
