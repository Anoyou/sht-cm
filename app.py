#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主应用程序入口 - 整合 Web UI、API 路由、后台任务管理和蓝图注册
v1.3.0 - 模块化架构，使用 Flask Blueprint 组织代码、支持异步多线程爬虫、支持 PWA
"""

import os
import logging
from flask import Flask
from configuration import Config
from models import db
from cache_manager import cache_manager

# ==================== 全局变量定义 ====================

# 爬虫状态控制（这些变量会被存储在 app.config 中供 Blueprint 访问）
CRAWL_STATUS = {
    'is_crawling': False,
    'last_crawl_time': None,
    'message': '空闲'
}

CRAWL_CONTROL = {
    'paused': False,
    'stop': False
}

CRAWL_PROGRESS = {
    'sections_total': 0,
    'sections_done': 0,
    'current_section': '',
    'current_section_pages': 0,
    'current_section_processed': 0,
    'current_page': 0,
    'max_pages': 0,
    'processed_pages': 0,
    'total_saved': 0,
    'total_skipped': 0,
    'current_section_saved': 0,
    'current_section_skipped': 0,
    'estimated_total_pages': 0
}

# 日志和文件路径 - 使用 Config 统一管理，避免硬编码
LOG_DIR = Config.get_path('log_dir')
LOG_FILE = Config.get_path('log_file')
SUMMARY_FILE = Config.get_path('summary_json')
OPTIONS_FILE = Config.get_path('crawl_options')


# ==================== 应用工厂函数 ====================

def create_app(enable_background_services: bool = True, enable_task_manager: bool = True):
    """创建并配置 Flask 应用"""
    app = Flask(__name__)
    logger = logging.getLogger(__name__)

    # 基本配置
    app.config['SQLALCHEMY_DATABASE_URI'] = Config.SQLALCHEMY_DATABASE_URI
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['JSON_AS_ASCII'] = False
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = Config.SQLALCHEMY_ENGINE_OPTIONS

    # 安全配置：SECRET_KEY 必须由环境变量或配置文件提供
    secret_key = Config.SECRET_KEY
    if not secret_key or secret_key == 'sht-default-secret-key':
        logger.warning(
            "⚠️ [SECURITY] SECRET_KEY 使用默认值或未配置，"
            "这可能导致安全问题！请通过环境变量或配置文件设置 SECRET_KEY"
        )
    app.config['SECRET_KEY'] = secret_key or 'dev-secret-key-change-in-production'

    # 存储全局状态变量到 app.config（供 Blueprint 访问）
    app.config['CRAWL_STATUS'] = CRAWL_STATUS
    app.config['CRAWL_CONTROL'] = CRAWL_CONTROL
    app.config['CRAWL_PROGRESS'] = CRAWL_PROGRESS
    app.config['LOG_DIR'] = LOG_DIR
    app.config['LOG_FILE'] = LOG_FILE
    app.config['SUMMARY_FILE'] = SUMMARY_FILE
    app.config['OPTIONS_FILE'] = OPTIONS_FILE

    # 初始化日志系统
    setup_logging(app)

    # 初始化数据库
    db.init_app(app)

    # 判定当前环境：隔离 Flask Reloader 的父进程
    is_worker_process = os.environ.get('WERKZEUG_RUN_MAIN') == 'true'
    is_reloader_parent = not is_worker_process and (os.environ.get('FLASK_DEBUG') == '1' or app.debug)
    
    # 路径安全处理 (针对 338MB 大文件及中文路径)
    db_uri = app.config.get('SQLALCHEMY_DATABASE_URI', '')
    if 'sqlite' in db_uri:
        # 解析出原始路径，重新构建带编码的 URI
        raw_path = db_uri.replace('sqlite:///', '').split('?')[0]
        from urllib.parse import quote
        # 确保去掉可能残留的多余斜杠并重新 quote
        clean_path = os.path.abspath(raw_path)
        app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{clean_path}?timeout=60"
        logger.debug(f"📍 数据库 URI 已校正: {app.config['SQLALCHEMY_DATABASE_URI']}")

    with app.app_context():
        # 如果是调试模式下的父进程，绝对不初始化数据库
        if not is_reloader_parent:
            logger.info("🔧 真正工作进程正在初始化 IO 资源...")
            try:
                db.create_all()
                from utils.init_helpers import init_db_data
                init_db_data(app)
                
                # 任务和后台启动
                register_blueprints(app)
                if enable_background_services:
                    start_background_services(app)
                if enable_task_manager:
                    start_task_manager(app)
                logger.info(f"✓ [APP] Flask 应用初始化完成")
            except Exception as e:
                logger.error(f"❌ 数据库挂载失败 [路径风险]: {e}")
                if is_worker_process: raise e
        else:
            # 父进程（Reloader）仅加载路由
            logger.debug("⏭️ Reloader 父进程跳过 IO 占用")
            register_blueprints(app)

    return app


def start_task_manager(app):
    """启动任务管理器"""
    logger = logging.getLogger(__name__)

    try:
        from task_manager import start_task_manager as start_tm
        start_tm(app)
        logger.info("[TASK] 任务管理器已启动")
    except Exception as e:
        logger.warning(f"! [TASK] 任务管理器启动失败: {e}")


def setup_logging(app):
    """配置日志系统"""
    from utils.logging_handler import setup_log_buffer_handler

    # 确保日志目录存在
    os.makedirs(LOG_DIR, exist_ok=True)

    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, Config.LOG_LEVEL, logging.INFO))

    # 清除现有的处理器
    root_logger.handlers.clear()

    # 文件处理器
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    root_logger.addHandler(file_handler)

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    )
    root_logger.addHandler(console_handler)

    # 日志缓冲处理器（用于 Web UI 显示）
    setup_log_buffer_handler()

    # 显式压制三方库冗余日志 (确保在 root_logger 重新配置后生效)
    for _logger in ['httpx', 'httpcore', 'telegram', 'telegram.ext', 'urllib3', 'crawler_control']:
        logging.getLogger(_logger).setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.info(f"[LOG] 日志系统初始化完成 - 日志级别: {Config.LOG_LEVEL} (三方库已静默)")


def register_blueprints(app):
    """注册所有 Blueprint"""
    logger = logging.getLogger(__name__)

    # 导入 Blueprint
    from blueprints.pages import pages_bp
    from blueprints.api_core import api_core_bp
    from blueprints.api_crawl import api_crawl_bp
    from blueprints.api_tasks import api_tasks_bp
    from blueprints.api_state import api_state
    from sht2bm_adapter import sht2bm_bp

    # 注册 Blueprint
    app.register_blueprint(pages_bp)
    app.register_blueprint(api_core_bp)
    app.register_blueprint(api_crawl_bp)
    app.register_blueprint(api_tasks_bp)
    app.register_blueprint(api_state)  # 状态同步 API
    app.register_blueprint(sht2bm_bp, url_prefix='/api')  # SHT2BM 使用 /api/bt 路径

    logger.info("[BLUEPRINT] 所有蓝图已注册：pages, api_core, api_crawl, api_tasks, api_state, sht2bm")


def start_background_services(app):
    """启动后台服务"""
    logger = logging.getLogger(__name__)

    # 启动 SHT2BM 后台服务
    from utils.init_helpers import start_sht2bm_background
    start_sht2bm_background()

    logger.info("[SERVICE] 后台服务已启动")
    logger.info("[SERVICE] SHT2BM API 已作为 Blueprint 集成 - 访问路径: /api/bt/*")


# ==================== 应用实例创建 ====================

app = None

# 兼容性：导出全局变量供其他模块使用
crawl_status = CRAWL_STATUS
crawl_control = CRAWL_CONTROL
crawl_progress = CRAWL_PROGRESS


def get_app_instance(enable_background_services: bool = True, enable_task_manager: bool = True) -> Flask:
    """获取或创建 Flask 应用实例"""
    global app
    if app is None:
        app = create_app(
            enable_background_services=enable_background_services,
            enable_task_manager=enable_task_manager
        )
    return app


# ==================== 全局应用实例 (供 gunicorn 使用) ====================

# 注意：由 start.py 手动调用，模块加载时不自动创建，防止双重初始化
# gunicorn 依然可以从这里导入，因为它会按需执行
if os.environ.get('SERVER_SOFTWARE', '').startswith('gunicorn'):
    app = get_app_instance()
else:
    app = None


# ==================== 主入口 ====================

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    app = get_app_instance()
    port = 5001  # 使用5001端口避免与macOS AirPlay冲突

    # 根据环境决定是否开启 debug 模式
    # Docker 部署默认关闭 debug，本地开发可以开启
    debug_mode = not Config.IS_DOCKER  # Docker 环境默认为 False
    # 也可以通过环境变量 DEBUG=True 强制开启
    if os.environ.get('DEBUG', '').lower() in ('true', '1', 'yes'):
        debug_mode = True

    logger.info("="*60)
    logger.info("🚀 SHT 资源聚合系统启动")
    logger.info(f"📦 版本: {Config.VERSION}")
    logger.info(f"🔧 环境: {'Docker容器' if Config.IS_DOCKER else '本地开发'}")
    logger.info(f"🌐 地址: http://0.0.0.0:{port}")
    logger.info(f"🐛 Debug模式: {'开启' if debug_mode else '关闭'}")
    logger.info("="*60)

    app.run(
        host='0.0.0.0',
        port=port,
        debug=debug_mode,
        threaded=True
    )
