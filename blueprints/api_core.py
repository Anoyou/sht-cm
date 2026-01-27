#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
核心API Blueprint - 处理系统、配置、资源、日志和维护相关的API
"""

import os
import json
import math
import logging
import time
import pprint
from datetime import datetime, timezone
from flask import Blueprint, jsonify, request, current_app, Response
from sqlalchemy import func, or_, and_

from models import db, Resource, Category
from configuration import Config
from cache_manager import cache_manager, CacheKeys
from utils.logging_handler import logs_buffer
from utils.api_response import (
    success_response,
    error_response,
    ErrorCode,
    missing_parameter_response,
    invalid_parameter_response,
    not_found_response,
    operation_failed_response,
    log_api_call,
    log_error_with_traceback
)
from utils.validators import (
    PaginationValidator,
    DateValidator,
    StringValidator,
    RequestParams
)
from services.resource_service import UnifiedService

api_core_bp = Blueprint('api_core', __name__, url_prefix='/api')

# 日志文件路径（从 current_app.config 动态获取）
logger = logging.getLogger(__name__)

# 日志版本号（用于防止缓存）
_log_version = 0


# ==================== 系统API ====================

@api_core_bp.route('/stats')
def api_stats():
    """获取统计数据 - 优化缓存策略"""
    # 尝试从缓存获取（延长缓存时间到15分钟）
    cache_key = CacheKeys.STATS
    cached_stats = cache_manager.get(cache_key)
    if cached_stats:
        return jsonify(cached_stats)

    # 计算统计数据
    stats = Resource.get_statistics()

    # 缓存结果（15分钟 = 900秒）
    cache_manager.set(cache_key, stats, ttl=900)

    return jsonify(stats)


@api_core_bp.route('/version')
def api_version():
    """获取系统版本"""
    return jsonify({'version': Config.VERSION})


# ==================== 配置API ====================

@api_core_bp.route('/config')
def api_config():
    """获取配置状态（不返回敏感信息）"""
    start_time = time.time()

    try:
        config = current_app.config

        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/config',
            params={},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        response_data = {
            'tg_bot_token_set': bool(config.get('TG_BOT_TOKEN')),
            'proxy_set': bool(config.get('PROXY')),
            'bypass_url_set': bool(config.get('BYPASS_URL')),
            'flare_solverr_url_set': bool(config.get('FLARE_SOLVERR_URL'))
        }

        response = jsonify(response_data)
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        return success_response(
            data=response_data,
            message='获取配置状态成功'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/config'},
            message='获取配置状态失败'
        )

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/config',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='获取配置状态失败',
            details=str(e)
        )


@api_core_bp.route('/config/set', methods=['POST'])
def api_config_set():
    """设置系统配置API - 支持持久化配置"""
    start_time = time.time()
    try:
        from configuration import config_manager

        data = request.get_json(silent=True) or {}
        keys = [
            'TG_BOT_TOKEN', 'TG_NOTIFY_CHAT_ID', 'PROXY', 'BYPASS_URL',
            'FLARE_SOLVERR_URL', 'LOG_LEVEL', 'LOG_BUFFER_SIZE', 'SAFE_MODE',
            'CRAWLER_MODE', 'CRAWLER_MAX_CONCURRENCY', 'CRAWLER_THREAD_COUNT',
            'CRAWLER_ASYNC_DELAY_MIN', 'CRAWLER_ASYNC_DELAY_MAX',
            'CRAWLER_SYNC_DELAY_MIN', 'CRAWLER_SYNC_DELAY_MAX',
            'HEARTBEAT_INTERVAL', 'GLOBAL_ERROR_THRESHOLD',
            'AUTO_CRAWL_ENABLED', 'AUTO_CRAWL_TIME'
        ]

        # 更新配置
        config_updates = {}
        for k in keys:
            if k in data:
                val = data.get(k)
                # 对于SAFE_MODE，存储为布尔值
                if k == 'SAFE_MODE':
                    config_updates[k] = bool(val)  # 直接转换为布尔值
                # 对于AUTO_CRAWL_ENABLED，也存储为布尔值
                elif k == 'AUTO_CRAWL_ENABLED':
                    config_updates[k] = bool(val)
                # 对于CRAWLER_MODE，确保是有效的字符串值
                elif k == 'CRAWLER_MODE':
                    # 确保值是字符串类型，且是有效的模式
                    str_val = str(val) if val else 'async'
                    if str_val.lower() in ('async', 'thread', 'sync'):
                        config_updates[k] = str_val.lower()
                    else:
                        config_updates[k] = 'async'  # 无效值使用默认值
                # 整数类型处理
                elif k in ('LOG_BUFFER_SIZE', 'CRAWLER_MAX_CONCURRENCY', 'CRAWLER_THREAD_COUNT', 'HEARTBEAT_INTERVAL'):
                    try:
                        num_val = int(val)
                        if k == 'LOG_BUFFER_SIZE':
                            config_updates[k] = max(1000, min(num_val, 100000))
                        elif k == 'CRAWLER_MAX_CONCURRENCY':
                            config_updates[k] = max(1, min(num_val, 100))
                        elif k == 'CRAWLER_THREAD_COUNT':
                            config_updates[k] = max(1, min(num_val, 50))
                        elif k == 'HEARTBEAT_INTERVAL':
                            config_updates[k] = max(10, min(num_val, 600))  # 限制在10-600秒
                    except (ValueError, TypeError):
                        pass # 保持默认或跳过无效值
                # 浮点数延迟处理
                elif k.endswith('_DELAY_MIN') or k.endswith('_DELAY_MAX'):
                    try:
                        config_updates[k] = float(val)
                    except (ValueError, TypeError):
                        pass
                else:
                    config_updates[k] = val or ''

        # 批量更新配置（会同时更新环境变量和配置文件）
        if config_updates:
            success = config_manager.update(config_updates, section='app')
            if not success:
                logger.error(f"✗ [CONFIG] 配置保存到文件失败")

                log_api_call(
                    logger,
                    method='POST',
                    endpoint='/api/config/set',
                    params={'config_updates': config_updates},
                    status='error',
                    response_code=500,
                    duration_ms=(time.time() - start_time) * 1000,
                    error='配置保存失败'
                )

                return operation_failed_response('配置保存失败')
            else:
                logger.info(f"✓ [CONFIG] 配置已成功保存到文件")

        # 如果修改了日志等级，立即应用
        if 'LOG_LEVEL' in data:
            config_manager.apply_log_level()
            logger.info(f"✓ [CONFIG] 日志等级已更新: {data['LOG_LEVEL']}")
            logger.debug("这是一条DEBUG级别的测试日志 - 如果你看到这条日志说明DEBUG等级已生效")

        # 如果修改了日志缓冲区大小，提示需要重启
        if 'LOG_BUFFER_SIZE' in data:
            logger.info(f"✓ [CONFIG] 日志缓冲区大小已更新: {data['LOG_BUFFER_SIZE']} 条")
            logger.info(f"💡 [CONFIG] 日志缓冲区大小将在下次重启后生效")

        # 如果修改了爬虫模式，记录日志
        if 'CRAWLER_MODE' in data:
            mode = data['CRAWLER_MODE']
            mode_name = {
                'async': '异步模式 (高性能)',
                'thread': '多线程模式',
                'sync': '串行模式'
            }.get(mode, mode)
            logger.info(f"✓ [CONFIG] 爬虫模式已更新: {mode_name}")
            logger.info(f"💡 [CONFIG] 下次爬取任务将使用 {mode} 模式")
            logger.info(f"📝 [CONFIG] 配置已保存到文件，重启后依然生效")
        
        # 如果修改了定时爬取配置，立即更新任务管理器
        if 'AUTO_CRAWL_ENABLED' in data or 'AUTO_CRAWL_TIME' in data:
            try:
                # 任务管理器已停用，仅记录日志
                auto_enabled = config_manager.get('AUTO_CRAWL_ENABLED', False)
                auto_time = config_manager.get('AUTO_CRAWL_TIME', '03:00')
                logger.info(f"✓ [CONFIG] 每日定时爬取配置已更新: {'开启' if auto_enabled else '关闭'} @ {auto_time}")
                logger.info(f"💡 [CONFIG] 注意: 任务管理器已停用，定时爬取功能需要通过其他方式实现")
            except Exception as te:
                logger.error(f"✗ [CONFIG] 更新定时任务配置失败: {te}")

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/config/set',
            params={'keys': list(data.keys()) if data else []},
            status='success',
            response_code=200,
            duration_ms=(time.time() - start_time) * 1000
        )

        return success_response(
            message='配置更新成功'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/config/set'},
            message='配置更新失败'
        )

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/config/set',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='配置更新失败',
            details=str(e)
        )


@api_core_bp.route('/crawl/stop', methods=['POST'])
def api_crawl_stop():
    """停止爬虫任务"""
    start_time = time.time()

    try:
        from scheduler.utils import stop_crawling_task

        # 强制停止参数
        force = request.json.get('force', False) if request.is_json else False

        success, message = stop_crawling_task(force=force)

        duration_ms = (time.time() - start_time) * 1000

        if success:
            logger.info(f"✅ [API] 停止爬虫成功: {message}")

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/stop',
                params={'force': force},
                status='success',
                response_code=200,
                duration_ms=duration_ms
            )

            return success_response(
                message=message,
                force_used=force
            )
        else:
            logger.warning(f"⚠️ [API] 停止爬虫失败: {message}")

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/stop',
                params={'force': force},
                status='error',
                response_code=400,
                duration_ms=duration_ms,
                error=message
            )

            return error_response(
                code=ErrorCode.BUSINESS_ERROR,
                message=message
            )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/crawl/stop'},
            message='停止爬虫异常'
        )

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/crawl/stop',
            params={'force': request.json.get('force') if request.is_json else None},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='停止爬虫异常',
            details=str(e)
        )


@api_core_bp.route('/crawl/pause', methods=['POST'])
def api_crawl_pause():
    """暂停爬虫任务"""
    start_time = time.time()

    try:
        from scheduler.utils import pause_crawling_task
        success, message = pause_crawling_task()
        if success:
            logger.info(f"✅ [API] 暂停爬虫成功: {message}")

            duration_ms = (time.time() - start_time) * 1000

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/pause',
                params={},
                status='success',
                response_code=200,
                duration_ms=duration_ms
            )

            return success_response(
                message=message
            )
        else:
            duration_ms = (time.time() - start_time) * 1000

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/pause',
                params={},
                status='error',
                response_code=400,
                duration_ms=duration_ms,
                error=message
            )

            return error_response(
                code=ErrorCode.BUSINESS_ERROR,
                message=message
            )
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/crawl/pause'},
            message='暂停爬虫异常'
        )

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/crawl/pause',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='暂停爬虫异常',
            details=str(e)
        )


@api_core_bp.route('/crawl/resume', methods=['POST'])
def api_crawl_resume():
    """恢复爬虫任务"""
    start_time = time.time()

    try:
        from scheduler.utils import resume_crawling_task
        success, message = resume_crawling_task()
        if success:
            logger.info(f"✅ [API] 恢复爬虫成功: {message}")

            duration_ms = (time.time() - start_time) * 1000

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/resume',
                params={},
                status='success',
                response_code=200,
                duration_ms=duration_ms
            )

            return success_response(
                message=message
            )
        else:
            duration_ms = (time.time() - start_time) * 1000

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/resume',
                params={},
                status='error',
                response_code=400,
                duration_ms=duration_ms,
                error=message
            )

            return error_response(
                code=ErrorCode.BUSINESS_ERROR,
                message=message
            )
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/crawl/resume'},
            message='恢复爬虫异常'
        )

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/crawl/resume',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='恢复爬虫异常',
            details=str(e)
        )


@api_core_bp.route('/config/values')
def api_config_values():
    """返回可安全展示的配置值，用于前端表单预填"""
    try:
        from configuration import config_manager

        # 获取安全模式设置（处理多种类型）
        safe_mode_val = config_manager.get('SAFE_MODE', False)
        # 兼容处理：可能是布尔值、字符串或其他类型
        if isinstance(safe_mode_val, bool):
            safe_mode = safe_mode_val
        elif isinstance(safe_mode_val, str):
            safe_mode = safe_mode_val.lower() in ('true', '1', 'yes')
        else:
            safe_mode = bool(safe_mode_val)

        response = jsonify({
            'PROXY': config_manager.get('PROXY', ''),
            'BYPASS_URL': config_manager.get('BYPASS_URL', ''),
            'FLARE_SOLVERR_URL': config_manager.get('FLARE_SOLVERR_URL', ''),
            'TG_BOT_TOKEN': '***已设置***' if config_manager.get('TG_BOT_TOKEN', '') else '',
            'TG_BOT_TOKEN_SET': bool(config_manager.get('TG_BOT_TOKEN', '')),
            'TG_NOTIFY_CHAT_ID': config_manager.get('TG_NOTIFY_CHAT_ID', ''),
            'LOG_LEVEL': config_manager.get('LOG_LEVEL', 'INFO'),
            'LOG_BUFFER_SIZE': config_manager.get('LOG_BUFFER_SIZE', 10000),
            'SAFE_MODE': safe_mode,
            'CRAWLER_MODE': config_manager.get('CRAWLER_MODE', 'async'),
            'CRAWLER_MAX_CONCURRENCY': config_manager.get('CRAWLER_MAX_CONCURRENCY', 20),
            'CRAWLER_THREAD_COUNT': config_manager.get('CRAWLER_THREAD_COUNT', 10),
            'CRAWLER_ASYNC_DELAY_MIN': config_manager.get('CRAWLER_ASYNC_DELAY_MIN', 0.5),
            'CRAWLER_ASYNC_DELAY_MAX': config_manager.get('CRAWLER_ASYNC_DELAY_MAX', 1.5),
            'CRAWLER_SYNC_DELAY_MIN': config_manager.get('CRAWLER_SYNC_DELAY_MIN', 0.3),
            'CRAWLER_SYNC_DELAY_MAX': config_manager.get('CRAWLER_SYNC_DELAY_MAX', 0.8),
            'HEARTBEAT_INTERVAL': config_manager.get('HEARTBEAT_INTERVAL', 60),
            'GLOBAL_ERROR_THRESHOLD': config_manager.get('GLOBAL_ERROR_THRESHOLD', 300),
            'AUTO_CRAWL_ENABLED': config_manager.get('AUTO_CRAWL_ENABLED', False),
            'AUTO_CRAWL_TIME': config_manager.get('AUTO_CRAWL_TIME', '03:00'),
            # 增加爬虫特定配置，便于多端同步
            'CRAWL_FORUMS': config_manager.get_crawl_config('selected_forums') or [],
            'CRAWL_DATE_MODE': config_manager.get_crawl_config('date_mode') or 'all',
            'CRAWL_PAGE_MODE': config_manager.get_crawl_config('page_mode') or 'fixed',
            'CRAWL_MAX_PAGES': config_manager.get_crawl_config('max_pages') or 3,
            'CRAWL_SMART_LIMIT': config_manager.get_crawl_config('smart_limit') or 500
        })
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        return response
    except Exception as e:
        logger.error(f"✗ [CONFIG] 获取配置值失败: {e}")

        safe_mode_raw = current_app.config.get('SAFE_MODE')
        if safe_mode_raw is None:
            safe_mode_raw = os.environ.get('SAFE_MODE', False)
        if isinstance(safe_mode_raw, bool):
            safe_mode = safe_mode_raw
        elif isinstance(safe_mode_raw, str):
            safe_mode = safe_mode_raw.lower() in ('true', '1', 'yes')
        else:
            safe_mode = bool(safe_mode_raw)

        response = jsonify({
            'PROXY': current_app.config.get('PROXY') or os.environ.get('PROXY') or '',
            'BYPASS_URL': current_app.config.get('BYPASS_URL') or os.environ.get('BYPASS_URL') or '',
            'FLARE_SOLVERR_URL': current_app.config.get('FLARE_SOLVERR_URL') or os.environ.get('FLARE_SOLVERR_URL') or '',
            'TG_BOT_TOKEN': '***已设置***' if (current_app.config.get('TG_BOT_TOKEN') or os.environ.get('TG_BOT_TOKEN')) else '',
            'TG_BOT_TOKEN_SET': bool(current_app.config.get('TG_BOT_TOKEN') or os.environ.get('TG_BOT_TOKEN')),
            'TG_NOTIFY_CHAT_ID': current_app.config.get('TG_NOTIFY_CHAT_ID') or os.environ.get('TG_NOTIFY_CHAT_ID') or '',
            'LOG_LEVEL': current_app.config.get('LOG_LEVEL') or os.environ.get('LOG_LEVEL') or 'INFO',
            'LOG_BUFFER_SIZE': current_app.config.get('LOG_BUFFER_SIZE') or int(os.environ.get('LOG_BUFFER_SIZE', 10000)),
            'SAFE_MODE': safe_mode,
            'CRAWLER_MODE': current_app.config.get('CRAWLER_MODE') or os.environ.get('CRAWLER_MODE') or 'async',
            'CRAWLER_MAX_CONCURRENCY': current_app.config.get('CRAWLER_MAX_CONCURRENCY') or int(os.environ.get('CRAWLER_MAX_CONCURRENCY', 20)),
            'CRAWLER_THREAD_COUNT': current_app.config.get('CRAWLER_THREAD_COUNT') or int(os.environ.get('CRAWLER_THREAD_COUNT', 10)),
            'CRAWLER_ASYNC_DELAY_MIN': current_app.config.get('CRAWLER_ASYNC_DELAY_MIN') or float(os.environ.get('CRAWLER_ASYNC_DELAY_MIN', 0.5)),
            'CRAWLER_ASYNC_DELAY_MAX': current_app.config.get('CRAWLER_ASYNC_DELAY_MAX') or float(os.environ.get('CRAWLER_ASYNC_DELAY_MAX', 1.5)),
            'CRAWLER_SYNC_DELAY_MIN': current_app.config.get('CRAWLER_SYNC_DELAY_MIN') or float(os.environ.get('CRAWLER_SYNC_DELAY_MIN', 0.3)),
            'CRAWLER_SYNC_DELAY_MAX': current_app.config.get('CRAWLER_SYNC_DELAY_MAX') or float(os.environ.get('CRAWLER_SYNC_DELAY_MAX', 0.8)),
            'HEARTBEAT_INTERVAL': current_app.config.get('HEARTBEAT_INTERVAL') or int(os.environ.get('HEARTBEAT_INTERVAL', 60)),
            'GLOBAL_ERROR_THRESHOLD': current_app.config.get('GLOBAL_ERROR_THRESHOLD') or int(os.environ.get('GLOBAL_ERROR_THRESHOLD', 300)),
            'AUTO_CRAWL_ENABLED': current_app.config.get('AUTO_CRAWL_ENABLED') or os.environ.get('AUTO_CRAWL_ENABLED') in ('true', '1', 'yes'),
            'AUTO_CRAWL_TIME': current_app.config.get('AUTO_CRAWL_TIME') or os.environ.get('AUTO_CRAWL_TIME') or '03:00',
            'CRAWL_FORUMS': [],
            'CRAWL_DATE_MODE': 'all',
            'CRAWL_PAGE_MODE': 'fixed',
            'CRAWL_MAX_PAGES': 3,
            'CRAWL_SMART_LIMIT': 500
        })
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        return response


@api_core_bp.route('/config/telegram-templates', methods=['GET'])
def api_telegram_templates_get():
    """获取 Telegram 通知模板（原始内容 + 结构化配置 + 占位符说明）"""
    start_time = time.time()
    try:
        import scheduler.notifier as notifier

        def extract_placeholders(text: str) -> str:
            if not text:
                return ''
            start = text.find('占位符说明')
            if start == -1:
                return ''
            end = text.find('TEMPLATES =', start)
            section = text[start:end] if end != -1 else text[start:]
            lines = []
            for line in section.splitlines():
                stripped = line.lstrip()
                if stripped.startswith('#'):
                    cleaned = stripped[1:]
                    if cleaned.startswith(' '):
                        cleaned = cleaned[1:]
                    lines.append(cleaned.rstrip())
                elif stripped.startswith('"""') or stripped.startswith("'''"):
                    continue
                else:
                    lines.append(line.rstrip())
            return "\n".join(lines).strip()

        path = notifier._resolve_templates_path()
        notifier._ensure_templates_file(path)

        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()

        templates = notifier.load_telegram_templates()
        placeholders = extract_placeholders(content)

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/config/telegram-templates',
            params={"path": path},
            status='success',
            response_code=200,
            duration_ms=(time.time() - start_time) * 1000
        )

        response, status = success_response(
            data={"content": content, "templates": templates, "placeholders": placeholders, "path": path},
            message='获取模板成功'
        )
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        return response, status
    except Exception as e:
        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/config/telegram-templates'},
            message='获取模板失败'
        )
        log_api_call(
            logger,
            method='GET',
            endpoint='/api/config/telegram-templates',
            params={},
            status='error',
            response_code=500,
            duration_ms=(time.time() - start_time) * 1000,
            error=str(e)
        )
        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='获取模板失败',
            details=str(e)
        )


@api_core_bp.route('/config/telegram-templates', methods=['POST'])
def api_telegram_templates_set():
    """保存或重置 Telegram 通知模板"""
    start_time = time.time()
    try:
        import scheduler.notifier as notifier

        def render_templates_py(path: str, templates: dict) -> str:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    base = f.read()
            except Exception:
                base = notifier._build_default_templates_py()

            if 'TEMPLATES =' not in base:
                base = notifier._build_default_templates_py()

            prefix = base.split('TEMPLATES =', 1)[0]
            pretty = pprint.pformat(templates, width=120, sort_dicts=False)
            return f"{prefix}TEMPLATES = {pretty}\n"

        path = notifier._resolve_templates_path()
        notifier._ensure_templates_file(path)

        data = request.get_json(silent=True) or {}
        action = (data.get('action') or '').strip()

        if action == 'reset':
            if path.endswith('.py'):
                content = notifier._build_default_templates_py()
            else:
                content = json.dumps(notifier.DEFAULT_TEMPLATES, ensure_ascii=False, indent=2)

            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/config/telegram-templates',
                params={'action': 'reset'},
                status='success',
                response_code=200,
                duration_ms=(time.time() - start_time) * 1000
            )

            return success_response(
                data={"content": content},
                message='模板已恢复默认'
            )

        templates = data.get('templates')
        if isinstance(templates, dict):
            if path.endswith('.json'):
                content = json.dumps(templates, ensure_ascii=False, indent=2)
            else:
                content = render_templates_py(path, templates)

            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/config/telegram-templates',
                params={'action': 'save', 'mode': 'structured'},
                status='success',
                response_code=200,
                duration_ms=(time.time() - start_time) * 1000
            )

            return success_response(
                data={"content": content},
                message='模板已保存'
            )

        content = data.get('content')
        if not isinstance(content, str):
            return invalid_parameter_response('content', '模板内容必须是字符串')
        if not content.strip():
            return invalid_parameter_response('content', '模板内容不能为空')

        if path.endswith('.json'):
            try:
                parsed = json.loads(content)
                content = json.dumps(parsed, ensure_ascii=False, indent=2)
            except Exception:
                return invalid_parameter_response('content', 'JSON 格式不正确')

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/config/telegram-templates',
            params={'action': 'save', 'mode': 'raw'},
            status='success',
            response_code=200,
            duration_ms=(time.time() - start_time) * 1000
        )

        return success_response(
            data={"content": content},
            message='模板已保存'
        )

    except Exception as e:
        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/config/telegram-templates'},
            message='保存模板失败'
        )
        log_api_call(
            logger,
            method='POST',
            endpoint='/api/config/telegram-templates',
            params={},
            status='error',
            response_code=500,
            duration_ms=(time.time() - start_time) * 1000,
            error=str(e)
        )
        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='保存模板失败',
            details=str(e)
        )


@api_core_bp.route('/config/test-telegram', methods=['POST'])
def api_test_telegram():
    """测试 Telegram Bot 配置是否正确"""
    try:
        data = request.get_json() or {}

        # 如果前端没有传入，则从配置读取
        token = data.get('token', '').strip()
        chat_id = data.get('chat_id', '').strip()

        if not token or not chat_id:
            from configuration import config_manager
            if not token:
                token = config_manager.get('TG_BOT_TOKEN', '')
            if not chat_id:
                chat_id = config_manager.get('TG_NOTIFY_CHAT_ID', '')

        if not token:
            return jsonify({
                'status': 'error',
                'message': 'Bot Token 未配置'
            }), 400

        if not chat_id:
            return jsonify({
                'status': 'error',
                'message': 'Chat ID 未配置'
            }), 400

        # 尝试发送测试消息
        import requests
        from datetime import datetime
        from utils.retry_utils import retry_request, RETRY_CONFIG

        test_message = f"🤖 Telegram Bot 测试消息\n\n✅ 配置正常！\n\n🕐 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n来自: SHT 资源聚合系统"

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': test_message,
            'parse_mode': 'HTML'
        }

        # 增加超时时间，使用代理（如果有配置）
        proxy_url = config_manager.get('PROXY', '')
        proxies = {'http': proxy_url, 'https': proxy_url} if proxy_url else None

        config = RETRY_CONFIG['telegram']
        response = retry_request(
            requests.post,
            url=url,
            json=payload,
            proxies=proxies,
            raise_on_fail=False,
            **config
        )

        if response:
            try:
                result = response.json()
                if result.get('ok'):
                    logger.info(f"✓ [TELEGRAM] 测试消息发送成功 - Chat ID: {chat_id}")
                    return jsonify({
                        'status': 'success',
                        'message': '测试消息发送成功！请检查您的 Telegram'
                    })
                else:
                    error_msg = result.get('description', '未知错误')
                    logger.warning(f"✗ [TELEGRAM] 测试消息发送失败: {error_msg}")
                    return jsonify({
                        'status': 'error',
                        'message': f'发送失败: {error_msg}'
                    }), 400
            except Exception as e:
                logger.error(f"✗ [TELEGRAM] 解析响应失败: {e}")
                return jsonify({
                    'status': 'error',
                    'message': f'解析响应失败: {str(e)}'
                }), 500
        else:
            logger.error("✗ [TELEGRAM] 测试消息发送失败（所有重试均失败）")
            return jsonify({
                'status': 'error',
                'message': '发送失败：请检查网络连接或代理配置'
            }), 500

    except Exception as e:
        logger.error(f"✗ [TELEGRAM] 测试失败: {e}")
        return jsonify({
            'status': 'error',
            'message': f'测试失败: {str(e)}'
        }), 500


# ==================== 资源和分类API ====================

@api_core_bp.route('/resources')
def api_resources():
    """获取资源列表，支持多种筛选条件"""
    start_time = time.time()

    try:
        # 使用统一的参数验证器
        page, per_page, error = RequestParams.get_pagination_params()
        if error:
            return error

        category, error = RequestParams.get_category_params()
        if error:
            return error

        search, error = RequestParams.get_search_params(min_length=2, max_length=200)
        if error:
            return error

        date_start, date_end, error = RequestParams.get_date_range_params()
        if error:
            return error

        incomplete_type = request.args.get('incomplete_type')

        logger.info(
            f'[API] 资源筛选 - 分类: {category or "全部"}, '
            f'搜索: {search or "无"}, '
            f'残缺: {incomplete_type or "无"}, '
            f'日期: {date_start or "无"} 到 {date_end or "无"}, '
            f'页码: {page}, 每页: {per_page}'
        )

        # 使用统一服务层获取资源
        result_data = UnifiedService.resource_service.get_resources_with_filters(
            page=page,
            per_page=per_page,
            category=category,
            search=search,
            date_start=date_start,
            date_end=date_end,
            incomplete_type=incomplete_type
        )

        # 构建返回数据
        response_data = {
            'resources': result_data['resources'],
            'total': result_data['total'],
            'pages': result_data['pages'],
            'current_page': result_data['current_page'],
            'per_page': result_data['per_page'],
            'has_next': result_data['has_next'],
            'has_prev': result_data['has_prev'],
            'filters': {
                'category': category,
                'search': search,
                'date_start': date_start,
                'date_end': date_end
            }
        }

        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/resources',
            params={'page': page, 'per_page': per_page, 'category': category, 'search': search},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            data=response_data,
            message=f'获取资源列表成功，共 {result_data["total"]} 条记录'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/resources', 'params': request.args.to_dict()},
            message='筛选资源失败'
        )

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/resources',
            params=request.args.to_dict(),
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message=f'筛选资源失败: {str(e)}',
            details=str(e)
        )


@api_core_bp.route('/resources/batch-recycle', methods=['POST'])
def api_resources_batch_recycle():
    """批量资源回炉重造 (支持手动选择和全量匹配筛选)"""
    start_time = time.time()
    try:
        data = request.get_json() or {}
        tids = data.get('tids', [])
        all_matching = data.get('all_matching', False)
        
        from models import FailedTID, Resource
        recycled_count = 0
        
        target_resources = []
        
        if all_matching:
            # 策略：根据当前所有筛选条件，找出所有匹配的资源（不分页）
            filters = data.get('filters', {})
            logger.info(f"[API] 触发全量回炉，条件: {filters}")
            
            # 使用现有服务逻辑获取查询对象（稍作修改以不限制分页）
            query = Resource.query
            
            # 重复应用筛选逻辑 (这里为了高性能，我们直接操作 query)
            if filters.get('category') and filters['category'] != 'all':
                query = query.filter(Resource.section == filters['category'])
            
            inc = filters.get('incomplete_type')
            if inc:
                types = inc.split(',')
                is_unknown_sub = (Resource.sub_type == '未知') | (Resource.sub_type == '') | (Resource.sub_type.is_(None))
                is_unknown_date = (Resource.publish_date == '未知') | (Resource.publish_date == '') | (Resource.publish_date.is_(None))
                is_unknown_size = (Resource.size == 0) | (Resource.size.is_(None))
                
                if 'sub_type_missing' in types: query = query.filter(is_unknown_sub)
                if 'date_missing' in types: query = query.filter(is_unknown_date)
                if 'size_missing' in types: query = query.filter(is_unknown_size)
                # ... (此处逻辑与 get_resources_with_filters 保持同步)

            if filters.get('search'):
                term = f"%{filters['search']}%"
                query = query.filter(or_(Resource.title.ilike(term), Resource.sub_type.ilike(term)))
                
            target_resources = query.all()
            logger.info(f"全量回炉扫描完成，共找到 {len(target_resources)} 条匹配项")
        else:
            # 手动选择模式
            if not tids:
                return invalid_parameter_response(message='请指定要回炉的 TID 列表')
            target_resources = Resource.query.filter(Resource.tid.in_(tids)).all()

        if not target_resources:
            return success_response(data={'recycled_count': 0}, message='没有找到符合条件的资源')

        # 执行批量处理
        for res in target_resources:
            # 1. 加入重试列表
            FailedTID.add(
                tid=res.tid,
                section=res.section,
                url=res.detail_url or f"https://sehuatang.org/forum.php?mod=viewthread&tid={res.tid}",
                reason="用户手动申请批量重修",
                force_activate=True  # 核心点：强制激活，无视之前的“成功”状态
            )
            # 2. 从主表移除
            db.session.delete(res)
            recycled_count += 1
        
        db.session.commit()
        
        duration_ms = (time.time() - start_time) * 1000
        logger.info(f'[API] 批量资源回炉 - 成功: {recycled_count} 条')
        
        return success_response(
            data={'recycled_count': recycled_count},
            message=f'已成功将 {recycled_count} 条资源送回重造队列'
        )
        
    except Exception as e:
        db.session.rollback()
        log_error_with_traceback(logger, e, message='批量回炉操作失败')
        return error_response(code=ErrorCode.INTERNAL_ERROR, message='批量回炉操作失败')


@api_core_bp.route('/categories')
def api_categories():
    """获取所有分类，包括数据库中的实际分类和统计信息 - 添加缓存优化"""
    start_time = time.time()

    # 尝试从缓存获取（延长缓存时间到15分钟）
    cache_key = CacheKeys.CATEGORIES
    cached_categories = cache_manager.get(cache_key)
    if cached_categories:
        return jsonify(cached_categories)

    try:
        # 获取数据库中实际存在的分类（从Resource表）
        existing_categories = db.session.query(
            Resource.section,
            func.count(Resource.id).label('count')
        ).filter(
            Resource.section.isnot(None),
            Resource.section != ''
        ).group_by(Resource.section).all()

        # 获取Category表中定义的所有分类
        defined_categories = Category.query.all()
        defined_cat_dict = {cat.name: cat for cat in defined_categories}

        # 构建分类列表，包含统计信息
        categories_list = []

        # 添加数据库中实际存在的分类
        for section_name, count in existing_categories:
            cat_info = {
                'name': section_name,
                'count': count,
                'defined': section_name in defined_cat_dict
            }
            # 如果在Category表中有定义，添加远程主题和页数信息
            if section_name in defined_cat_dict:
                cat_obj = defined_cat_dict[section_name]
                cat_info['total_topics'] = cat_obj.total_topics or 0
                cat_info['total_pages'] = cat_obj.total_pages or 0
                cat_info['fid'] = cat_obj.fid
            categories_list.append(cat_info)

        # 添加定义但可能没有数据的分类
        for cat in defined_categories:
            if cat.name not in [c['name'] for c in categories_list]:
                categories_list.append({
                    'name': cat.name,
                    'count': 0,
                    'defined': True,
                    'total_topics': cat.total_topics or 0,
                    'total_pages': cat.total_pages or 0,
                    'fid': cat.fid
                })

        # 按名称排序
        categories_list.sort(key=lambda x: x['name'])

        # 缓存结果（72小时 = 259200秒）
        cache_manager.set(cache_key, categories_list, ttl=259200)

        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/categories',
            params={},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return jsonify(categories_list)

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/categories'},
            message='获取分类列表失败'
        )

        # 返回基础分类列表作为备选
        try:
            categories = Category.query.all()
            fallback_list = [{'name': c.name, 'count': 0, 'defined': True} for c in categories]

            log_api_call(
                logger,
                method='GET',
                endpoint='/api/categories',
                params={'note': 'fallback used'},
                status='success',
                response_code=200,
                duration_ms=duration_ms
            )

            return jsonify(fallback_list)
        except Exception as fallback_err:
            log_api_call(
                logger,
                method='GET',
                endpoint='/api/categories',
                params={'note': 'fallback failed'},
                status='error',
                response_code=500,
                duration_ms=duration_ms,
                error=str(fallback_err)
            )

            return jsonify([])


@api_core_bp.route('/stats/categories')
def api_stats_categories():
    """按分类统计条目数"""
    start_time = time.time()

    try:
        logger.info(f"[STATS] 开始获取分类统计数据")

        # 检查是否强制刷新
        force_refresh = request.args.get('force', 'false').lower() == 'true'

        if not force_refresh:
            # 尝试从缓存获取
            cached_result = cache_manager.get(CacheKeys.CATEGORIES)
            if cached_result:
                logger.info(f"✓ [CACHE] 从缓存返回分类统计数据: {len(cached_result)}个分类")

                duration_ms = (time.time() - start_time) * 1000

                log_api_call(
                    logger,
                    method='GET',
                    endpoint='/api/stats/categories',
                    params={'force': force_refresh, 'note': 'from cache'},
                    status='success',
                    response_code=200,
                    duration_ms=duration_ms
                )

                return success_response(
                    data=cached_result,
                    message=f'从缓存获取分类统计，共 {len(cached_result)} 个分类'
                )

        # 使用统一服务层获取分类数据
        try:
            categories_list = UnifiedService.category_service.get_all_categories(
                include_stats=True,
                include_defined=False
            )
        except Exception as service_error:
            logger.error(f"[STATS] 服务层获取分类失败: {service_error}")
            # 降级方案：直接从数据库查询
            existing_categories = db.session.query(
                Resource.section,
                func.count(Resource.id).label('count')
            ).filter(
                Resource.section.isnot(None),
                Resource.section != ''
            ).group_by(Resource.section).all()

            categories_list = []
            for section_name, count in existing_categories:
                categories_list.append({
                    'name': section_name,
                    'count': count
                })

        # 提取统计数据
        result = []
        if isinstance(categories_list, dict):
            # 如果返回的是字典，转换为列表
            result = [
                {'section': cat.get('name', '未知'), 'count': cat.get('count', 0)}
                for cat in categories_list.values()
            ]
        elif isinstance(categories_list, list):
            result = [
                {'section': cat.get('name', '未知'), 'count': cat.get('count', 0)}
                for cat in categories_list
            ]
        else:
            result = categories_list

        logger.info(f"✓ [STATS] 统计API返回: {len(result)}个分类")
        for item in result[:5]:
            logger.info(f"  - {item['section']}: {item['count']} 条")

        # 缓存结果（5分钟）
        cache_manager.set(CacheKeys.CATEGORIES, categories_list, ttl=300)

        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/stats/categories',
            params={'force': force_refresh},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            data=result,
            message=f'获取分类统计成功，共 {len(result)} 个分类'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/stats/categories', 'params': request.args.to_dict()},
            message='获取分类统计失败'
        )

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/stats/categories',
            params=request.args.to_dict(),
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='获取分类统计失败',
            details=str(e)
        )


# ==================== 日志API ====================

@api_core_bp.route('/logs/test')
def api_logs_test():
    """测试日志API - 用于诊断"""
    LOG_FILE = current_app.config.get('LOG_FILE', '')
    
    return jsonify({
        'log_file_path': LOG_FILE,
        'file_exists': os.path.exists(LOG_FILE) if LOG_FILE else False,
        'config_log_file': LOG_FILE
    })


@api_core_bp.route('/logs/recent')
def api_logs_recent():
    """获取最近的日志"""
    start_time = time.time()
    global _log_version

    LOG_FILE = current_app.config.get('LOG_FILE', '')
    limit = request.args.get('limit', 300, type=int)

    # 应用参数验证
    if limit < 1:
        limit = 1
    elif limit > 10000:
        limit = 10000

    lines = []

    # 优先从文件读取（多worker共享）
    try:
        if LOG_FILE and os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                lines = content.splitlines()[-limit:]
    except Exception as e:
        logger.error(f"[LOGS] 读取日志文件失败: {e}")
        pass

    # 退回内存缓冲
    if not lines:
        lines = list(logs_buffer)[-limit:]
        logger.info(f"[LOGS] 从内存缓冲读取: {len(lines)} 行")

    duration_ms = (time.time() - start_time) * 1000

    log_api_call(
        logger,
        method='GET',
        endpoint='/api/logs/recent',
        params={'limit': limit},
        status='success',
        response_code=200,
        duration_ms=duration_ms
    )

    # 直接返回前端期望的格式
    return jsonify({
        'lines': lines,
        'version': _log_version,
        'timestamp': datetime.now().isoformat()
    })


@api_core_bp.route('/logs/search')
def api_logs_search():
    """搜索日志"""
    start_time = time.time()

    try:
        LOG_FILE = current_app.config.get('LOG_FILE', '')
        q = (request.args.get('q') or '').strip()
        limit = request.args.get('limit', 300, type=int)

        lines = []
        try:
            if LOG_FILE and os.path.exists(LOG_FILE):
                with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    lines = content.splitlines()
        except Exception:
            pass

        if not lines:
            lines = list(logs_buffer)

        if q:
            ql = q.lower()
            lines = [ln for ln in lines if ql in ln.lower()]

        lines = lines[-limit:]

        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/logs/search',
            params={'q': q, 'limit': limit},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            data={'lines': lines, 'count': len(lines)},
            message=f'搜索日志成功，找到 {len(lines)} 条记录'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/logs/search', 'params': request.args.to_dict()},
            message='搜索日志失败'
        )

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/logs/search',
            params=request.args.to_dict(),
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='搜索日志失败',
            details=str(e)
        )


@api_core_bp.route('/logs/session')
def api_logs_session():
    """获取本次启动期间的日志"""
    start_time = time.time()

    LOG_FILE = current_app.config.get('LOG_FILE', '')
    limit = request.args.get('limit', 1000, type=int)

    # 应用参数验证
    if limit < 1:
        limit = 1
    elif limit > 10000:
        limit = 10000

    lines = []
    session_lines = []

    try:
        if LOG_FILE and os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                lines = content.splitlines()
    except Exception:
        pass

    if not lines:
        lines = list(logs_buffer)

    # 尝试根据时间戳筛选本次启动的日志
    for line in lines:
        # 简单的启动标识检查
        if any(keyword in line for keyword in [
            'SHT资源聚合系统启动成功',
            '开始初始化数据库',
            '配置验证通过',
            'Flask应用启动'
        ]):
            # 找到启动标识后，从这里开始收集日志
            session_lines = []
        session_lines.append(line)

    # 如果没有找到启动标识，返回最近的日志
    if not session_lines:
        session_lines = lines[-limit:]
    else:
        session_lines = session_lines[-limit:]

    duration_ms = (time.time() - start_time) * 1000

    log_api_call(
        logger,
        method='GET',
        endpoint='/api/logs/session',
        params={'limit': limit},
        status='success',
        response_code=200,
        duration_ms=duration_ms
    )

    return success_response(
        data={
            'lines': session_lines,
            'total': len(session_lines),
            'message': f'本次启动期间的日志 (共 {len(session_lines)} 条)'
        },
        message=f'获取会话日志成功，共 {len(session_lines)} 条'
    )


@api_core_bp.route('/logs/export')
def api_logs_export():
    """导出日志文件"""
    start_time = time.time()
    LOG_FILE = current_app.config.get('LOG_FILE', '')
    try:
        # 创建时间戳
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # 收集所有日志内容
        all_logs = []

        # 从文件读取
        if LOG_FILE and os.path.exists(LOG_FILE):
            try:
                with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                    all_logs.extend(f.readlines())
            except Exception as e:
                all_logs.append(f"读取日志文件失败: {e}\n")

        # 从内存缓冲读取
        if logs_buffer:
            all_logs.extend([line + '\n' for line in logs_buffer])

        if not all_logs:
            all_logs = ['暂无日志数据\n']

        # 创建响应内容
        log_content = ''.join(all_logs)

        # 返回文件下载
        response = Response(
            log_content,
            mimetype='text/plain',
            headers={
                'Content-Disposition': f'attachment; filename=sht_logs_{timestamp}.txt',
                'Content-Type': 'text/plain; charset=utf-8'
            }
        )

        return response

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/logs/export'},
            message='导出日志失败'
        )

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/logs/export',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='导出日志失败',
            details=str(e)
        )


@api_core_bp.route('/logs/clear', methods=['POST'])
def api_logs_clear():
    """清除日志缓存API"""
    global _log_version

    LOG_FILE = current_app.config.get('LOG_FILE', '')

    try:
        # 清除内存缓冲区
        if logs_buffer is not None:
            logs_buffer.clear()
        else:
            logger.warning("🧪 [CLEAN] logs_buffer 为 None，跳过内存清理")

        # 同时清除日志文件内容
        if LOG_FILE and os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'w', encoding='utf-8') as f:
                f.truncate(0)

        # 递增版本号，强制前端刷新
        _log_version += 1

        logger.info(f"✓ [CLEAN] 日志缓存已清除（包括文件），版本号: {_log_version}")

        return jsonify({
            'status': 'success',
            'message': '日志缓存已清除',
            'version': _log_version,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })

    except Exception as e:
        logger.error(f"✗ [CLEAN] 清除日志缓存失败: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500


# ==================== 维护API ====================

@api_core_bp.route('/maintenance/cleanup', methods=['POST'])
def api_maintenance_cleanup():
    """数据维护清理API"""
    start_time = time.time()

    try:
        from maintenance_tools import DatabaseMaintenance

        maintenance = DatabaseMaintenance()

        # 获取清理类型
        data = request.get_json() or {}
        cleanup_type = data.get('type', 'duplicates')

        if not cleanup_type:
            duration_ms = (time.time() - start_time) * 1000
            return missing_parameter_response('type')

        result = {}

        if cleanup_type == 'duplicates':
            # 清理重复数据
            removed_count = Resource.cleanup_duplicates()
            result = {
                'type': 'duplicates',
                'removed_count': removed_count,
                'message': f'清理了 {removed_count} 条重复记录'
            }
        elif cleanup_type == 'normalize_dates':
            # 标准化日期
            with current_app.app_context():
                maintenance.normalize_dates()
            result = {
                'type': 'normalize_dates',
                'normalized_count': maintenance.stats.get('normalized_dates', 0),
                'message': f'标准化了 {maintenance.stats.get("normalized_dates", 0)} 条日期记录'
            }
        elif cleanup_type == 'optimize':
            # 优化数据库
            with current_app.app_context():
                maintenance.optimize_database()
            result = {
                'type': 'optimize',
                'message': '数据库优化完成'
            }
        elif cleanup_type == 'full':
            # 完整维护
            maintenance.run_full_maintenance()
            result = {
                'type': 'full',
                'stats': maintenance.stats,
                'message': '完整维护完成'
            }
        else:
            duration_ms = (time.time() - start_time) * 1000
            return invalid_parameter_response('type', details=f'不支持的清理类型: {cleanup_type}')

        # 清理缓存
        cache_manager.clear()

        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/maintenance/cleanup',
            params={'type': cleanup_type},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            data=result,
            message='数据维护完成'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/maintenance/cleanup'},
            message='数据维护失败'
        )

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/maintenance/cleanup',
            params={'type': request.get_json().get('type') if request.is_json else None},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='数据维护失败',
            details=str(e)
        )


@api_core_bp.route('/maintenance/info')
def api_maintenance_info():
    """获取数据库维护信息"""
    start_time = time.time()

    try:
        from maintenance_tools import DatabaseMaintenance

        maintenance = DatabaseMaintenance()
        info = maintenance.get_database_info()

        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/maintenance/info',
            params={},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            data=info,
            message='获取维护信息成功'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/maintenance/info'},
            message='获取维护信息失败'
        )

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/maintenance/info',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='获取维护信息失败',
            details=str(e)
        )
