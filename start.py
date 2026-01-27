#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SHT资源聚合系统启动脚本
提供统一的启动入口和参数配置
"""

import os
import sys
import logging
import argparse
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def setup_logging(log_level='INFO'):
    """设置日志配置"""
    # 优先使用环境变量中的日志等级，然后是传入的参数
    env_log_level = os.environ.get('LOG_LEVEL', '').upper()
    if env_log_level and env_log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
        log_level = env_log_level

    # 确保日志目录存在 - 使用配置管理器
    from configuration import Config
    log_dir = Config.get_path('log_dir')
    os.makedirs(log_dir, exist_ok=True)

    # 配置日志格式
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # 清除现有的处理器，避免重复
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 配置根日志记录器
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                os.path.join(log_dir, 'app.log'),
                encoding='utf-8'
            )
        ],
        force=True  # 强制重新配置
    )

    # 设置第三方库的日志级别
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)

    # 深度压制三方库及模块初始化冗余日志 (httpx, telegram, crawler_control)
    for logger_name in [
        'httpx', 'httpcore', 'telegram', 'telegram.ext', 'crawler_control', 'urllib3'
    ]:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    # 大幅减少SQLAlchemy日志输出
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.orm').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.dialects').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.pool.impl').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.orm.path_registry').setLevel(logging.WARNING)

def start_web_server(host='0.0.0.0', port=5000, debug=False, workers=4):
    """启动Web服务器"""
    if debug:
        # 开发模式
        # 注意：禁用 use_reloader 避免 fork 导致的配置管理器问题
        os.environ['FLASK_DEBUG'] = '0'  # 禁用 debug 标志，但保留调试输出
        from app import get_app_instance
        app = get_app_instance(enable_background_services=True, enable_task_manager=True)
        app.run(host=host, port=port, debug=False, use_reloader=False)
    else:
        # 生产模式使用gunicorn
        try:
            import gunicorn.app.wsgiapp as wsgi

            # 配置gunicorn参数
            sys.argv = [
                'gunicorn',
                '--bind', f'{host}:{port}',
                '--workers', str(workers),
                '--worker-class', 'sync',
                '--timeout', '120',
                '--keep-alive', '5',
                '--max-requests', '1000',
                '--max-requests-jitter', '100',
                '--preload',
                '--access-logfile', '-',
                '--error-logfile', '-',
                'app:app'
            ]

            wsgi.run()
        except ImportError:
            logging.warning("gunicorn未安装，使用Flask开发服务器")
            from app import create_app
            app = create_app()
            app.run(host=host, port=port, debug=False, threaded=True)

def start_telegram_bot():
    """启动Telegram机器人"""
    from bot import main as bot_main
    bot_main()

def start_all_services(host='0.0.0.0', port=5000, debug=False, workers=4):
    """启动所有服务 (Web + 自动后台任务)"""
    logger = logging.getLogger(__name__)
    logger.info("🚀 启动 SHT 整合服务 (Web + Bot + Monitor)...")
    logger.info("提示: 后台任务由 TaskManager 自动管理")

    # 启动 Web 服务器，它在初始化时会拉起 TaskManager
    start_web_server(host=host, port=port, debug=debug, workers=workers)


def start_crawler():
    """启动一次性爬虫任务"""
    from scheduler.core import run_crawling_task
    run_crawling_task()

def run_maintenance():
    """运行一次性数据库维护"""
    from maintenance_tools import DatabaseMaintenance
    maintenance = DatabaseMaintenance()
    maintenance.run_full_maintenance()

def show_system_info():
    """显示系统信息"""
    from configuration import Config
    from health import monitor
    from utils import get_flask_app

    print("\nSHT资源聚合系统信息")
    print("=" * 50)

    # 配置信息
    config_summary = Config.get_config_summary()
    print(f"版本: {config_summary.get('version', 'Unknown')}")
    print(f"环境: {'Docker' if config_summary.get('is_docker') else '本地'}")
    print(f"数据库: {config_summary.get('database_type', 'SQLite')}")

    # 系统状态
    try:
        flask_app = get_flask_app()
        with flask_app.app_context():
            health = monitor.get_summary()
            print(f"状态: {health['status']}")
        print(f"健康评分: {health['score']}/100")

        if health['issues']:
            print("注意项:")
            for issue in health['issues']:
                print(f"  - {issue}")
    except Exception as e:
        print(f"获取实时状态失败: {e}")

    print("=" * 50 + "\n")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='SHT资源聚合系统启动脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python start.py web                    # 启动主服务 (含 Web, Bot, Monitor)
  python start.py crawler                # 运行一次性爬虫
  python start.py maintenance            # 运行一次性维护
  python start.py info                   # 显示系统信息
        """
    )

    # 子命令
    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    # Web服务器命令 (现在是主命令)
    web_parser = subparsers.add_parser('web', help='启动主服务器 (含 Web, Bot, Monitor)')
    web_parser.add_argument('--host', default='0.0.0.0', help='监听地址')
    web_parser.add_argument('--port', type=int, default=5000, help='监听端口')
    web_parser.add_argument('--debug', action='store_true', help='开启开发调试模式')
    web_parser.add_argument('--workers', type=int, default=4, help='生产模式下的工作进程数')

    # 全部服务别名
    subparsers.add_parser('all', help='启动所有服务 (web 的别名)')

    # 机器人独立启动 (用于调试)
    subparsers.add_parser('bot', help='独立启动机器人 (调试用)')

    # 爬虫任务
    subparsers.add_parser('crawler', help='运行一次性爬虫任务')

    # 维护任务
    subparsers.add_parser('maintenance', help='运行一次性数据库维护')

    # 信息查询
    subparsers.add_parser('info', help='查询系统当前状态信息')

    # 全局参数
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='设置全局日志级别')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # 设置日志
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)

    try:
        if args.command in ['web', 'all']:
            mode = "主服务" if args.command == 'web' else "全量集成服务"
            logger.info(f"正在准备启动 {mode}...")
            start_all_services(
                host=getattr(args, 'host', '0.0.0.0'),
                port=getattr(args, 'port', 5000),
                debug=getattr(args, 'debug', False),
                workers=getattr(args, 'workers', 4)
            )
        elif args.command == 'bot':
            logger.info("独立启动 Telegram 机器人...")
            start_telegram_bot()
        elif args.command == 'crawler':
            logger.info("启动一次性爬虫任务...")
            start_crawler()
        elif args.command == 'maintenance':
            logger.info("执行数据库维护...")
            run_maintenance()
        elif args.command == 'info':
            show_system_info()

    except KeyboardInterrupt:
        logger.info("收到退出信号 (Ctrl+C)，正在安全关闭服务...")
    except Exception as e:
        logger.error(f"启动过程中遇到致命错误: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        sys.exit(1)

if __name__ == '__main__':
    main()