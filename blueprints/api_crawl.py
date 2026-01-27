#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫API Blueprint - 处理爬虫控制、论坛信息、健康检查和SHT2BM相关的API
"""

import os
import json
import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from flask import Blueprint, jsonify, request, current_app
import concurrent.futures

from models import db, Category, Resource
from configuration import Config
from cache_manager import cache_manager, CacheKeys
from utils.api_response import (
    success_response,
    error_response,
    ErrorCode,
    missing_parameter_response,
    invalid_request_response,
    invalid_parameter_response,
    not_found_response,
    operation_failed_response,
    operation_in_progress_response,
    log_api_call,
    log_error_with_traceback
)

api_crawl_bp = Blueprint('api_crawl', __name__, url_prefix='/api')

# 爬虫启动互斥锁，防止并发启动
_crawl_start_lock = threading.Lock()

logger = logging.getLogger(__name__)


# ==================== 爬虫控制API ====================

@api_crawl_bp.route('/crawl/status')
def api_crawl_status():
    """获取爬虫状态 - 多进程安全版本，直接读取状态文件"""
    # 修复：移除缓存，直接从状态协调器获取最新状态
    # 这样可以确保多个 worker 进程看到一致的状态
    try:
        from crawler_control.cc_control_bridge import get_crawler_control_bridge
        bridge = get_crawler_control_bridge()

        # 关键修复：强制从文件重新加载状态，确保多进程一致性
        coordinator_state = bridge.get_current_state(force_reload=True)

        # 检查信号队列中是否有待处理的信号
        # 如果有，返回对应的"过渡状态"，避免前端状态跳动
        pending_signals = bridge.queue_manager.get_pending_signals()

        # 判断是否有待处理的暂停/停止/恢复信号
        has_pending_pause = any(s.type == 'pause' and not s.processed for s in pending_signals)
        has_pending_stop = any(s.type == 'stop' and not s.processed for s in pending_signals)
        has_pending_resume = any(s.type == 'resume' and not s.processed for s in pending_signals)

        # 验证信号与当前状态的一致性，避免显示过时的pending信号
        # 如果当前状态已经是目标状态，忽略对应的pending信号
        if has_pending_pause and coordinator_state['is_paused']:
            # 当前已经暂停了，不再显示"正在暂停"
            has_pending_pause = False
        if has_pending_resume and coordinator_state['is_crawling'] and not coordinator_state['is_paused']:
            # 当前已经恢复运行了，不再显示"正在恢复"
            has_pending_resume = False
        if has_pending_stop and not coordinator_state['is_crawling'] and not coordinator_state['is_paused']:
            # 当前已经停止了，不再显示"正在停止"
            has_pending_stop = False

        # 确定最终状态（优先级：stop > pause > resume > 当前状态）
        if has_pending_stop:
            # 有待处理的停止信号 - 无论当前状态如何，都应显示正在停止
            # 停止时不应显示为暂停状态，避免混淆
            is_crawling = coordinator_state['is_crawling'] if coordinator_state['current_state'] == 'running' else False
            is_paused = False  # 停止过程中不应标记为暂停
            message = '正在停止...'
        elif has_pending_pause:
            # 有待处理的暂停信号
            # 暂停意味着不再爬取，所以 is_crawling 应为 False
            is_crawling = False  # 暂停时不应显示为爬取中
            is_paused = True     # 提前标记为暂停，避免跳动
            message = '正在暂停...'
        elif has_pending_resume:
            # 有待处理的恢复信号
            is_crawling = True  # 即将恢复运行
            is_paused = False   # 提前标记为非暂停
            message = '正在恢复...'
        else:
            # 没有待处理信号，使用当前状态
            is_crawling = coordinator_state['is_crawling']
            is_paused = coordinator_state['is_paused']
            if is_crawling:
                message = '正在爬取'
            elif is_paused:
                message = '已暂停'
            else:
                message = '空闲'

        # 使用状态协调器的状态作为权威来源
        crawl_status = {
            'is_crawling': is_crawling,
            'is_paused': is_paused,
            'message': message
        }
        crawl_progress = coordinator_state.get('progress', {})

        # v1.5.4: 只在状态变化时输出日志，且仅在爬虫活跃时记录
        last_state_key = "last_crawl_status"
        last_state = cache_manager.shared_get(last_state_key)
        current_state_tuple = (crawl_status['is_crawling'], crawl_status['is_paused'])
        
        # 只在以下情况记录日志：
        # 1. 状态确实发生了变化
        # 2. 爬虫处于活跃状态（正在运行或暂停中）
        if not last_state or list(last_state) != list(current_state_tuple):
            if crawl_status['is_crawling'] or crawl_status['is_paused']:
                logger.debug(f"[STATUS] 状态变化: is_crawling={crawl_status['is_crawling']}, is_paused={crawl_status['is_paused']}")
            cache_manager.shared_set(last_state_key, current_state_tuple, ttl=60)

    except Exception as e:
        logger.warning(f"[STATUS] 无法从状态协调器获取状态，降级到 app.config: {e}")
        # 降级方案：使用 app.config
        crawl_status = current_app.config.get('CRAWL_STATUS', {})
        crawl_progress = current_app.config.get('CRAWL_PROGRESS', {})
    

    SUMMARY_FILE = current_app.config.get('SUMMARY_FILE', '')

    # 只使用状态协调器作为唯一权威源
    # 状态文件读取很快（几毫秒），不会影响性能

    # 构建状态数据
    status_data = {
        **crawl_status,
        'progress': crawl_progress if crawl_status.get('is_crawling') else None,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    # 如果有最近的爬取结果，包含在状态中
    if not crawl_status.get('is_crawling'):
        try:
            if os.path.exists(SUMMARY_FILE):
                with open(SUMMARY_FILE, 'r', encoding='utf-8') as f:
                    summary_data = json.load(f)
                    # 只返回最近的结果摘要
                    if summary_data:
                        results = summary_data.get('results', {})
                        duration = summary_data.get('duration', {})
                        status_data['last_result'] = {
                            'total_saved': results.get('total_saved', 0),
                            'total_skipped': results.get('total_skipped', 0),
                            'total_time': duration.get('formatted', '0秒'),
                            'duration_seconds': duration.get('total_seconds', 0),
                            'sections': summary_data.get('per_section_results', {}),
                            'timestamp': summary_data.get('timestamp', ''),
                            'success_rate': results.get('success_rate', 0),
                            'page_statistics': summary_data.get('page_statistics', {}),
                            'section_page_breakdown': summary_data.get('section_page_breakdown', {}),
                            'metadata': summary_data.get('engine_set', {})  # 添加引擎配置信息
                        }
        except Exception as e:
            logger.error(f"✗ [LOG] 读取摘要文件失败: {e}")
            pass

    # 修复：移除缓存，确保多进程环境下状态实时一致
    # 每次都返回最新状态，避免 worker 进程间状态不同步

    return jsonify(status_data)


@api_crawl_bp.route('/crawl/start', methods=['POST'])
def api_crawl_start():
    """手动触发爬虫任务（带并发启动互斥锁）"""
    start_time = time.time()

    crawl_status = current_app.config.get('CRAWL_STATUS', {})
    LOG_DIR = current_app.config.get('LOG_DIR', '')
    OPTIONS_FILE = current_app.config.get('OPTIONS_FILE', '')

    logger.info(f"[CRAWLER] 收到爬虫启动请求")

    # 使用互斥锁防止并发启动
    if not _crawl_start_lock.acquire(blocking=False):
        logger.warning(f"[CRAWLER] 检测到并发启动请求，拒绝启动")

        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/crawl/start',
            params={},
            status='error',
            response_code=429,
            duration_ms=duration_ms,
            error='爬虫正在启动中，请稍后再试'
        )

        return operation_in_progress_response('爬虫正在启动中，请稍后再试')

    try:
        # 使用原子操作检查并设置状态，防止竞态条件
        if crawl_status.get('is_crawling'):
            logger.warning(f"[CRAWLER] 爬虫任务已在进行中，拒绝启动")
            _crawl_start_lock.release()

            duration_ms = (time.time() - start_time) * 1000

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/start',
                params={},
                status='error',
                response_code=400,
                duration_ms=duration_ms,
                error='爬虫任务正在进行中'
            )

            return error_response(
                code=ErrorCode.BUSINESS_ERROR,
                message='爬虫任务正在进行中'
            )

        # 立即设置状态为 True，防止重复点击
        crawl_status['is_crawling'] = True
        crawl_status['message'] = '正在初始化...'

        # 同步到共享状态
        cache_manager.shared_set(CacheKeys.CRAWL_STATUS, crawl_status)
        cache_manager.shared_set(CacheKeys.CRAWL_PROGRESS, current_app.config.get('CRAWL_PROGRESS', {}))

    except Exception as e:
        logger.error(f"[CRAWLER] 启动前设置状态失败: {e}")
        _crawl_start_lock.release()

        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/crawl/start'},
            message='启动前设置状态失败'
        )

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/crawl/start',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message=f'启动失败: {str(e)}'
        )
    finally:
        # 立即释放锁，允许后续请求
        _crawl_start_lock.release()

    payload = request.get_json(silent=True) or {}
    section_fids = payload.get('fids') or None
    date_mode = payload.get('date_mode') or 'all'
    date_value = payload.get('date_value') or None
    dateline = payload.get('dateline') or None
    max_pages = int(payload.get('max_pages') or 3)
    page_mode = payload.get('page_mode') or 'smart'
    page_range = payload.get('page_range') or None  # 新增：页码范围 [start, end]

    logger.info(f"[CRAWLER] 爬虫参数: fids={section_fids}, date_mode={date_mode}, max_pages={max_pages}")

    # 在启动线程前获取 app 引用和配置路径
    app = current_app._get_current_object()

    def run_task():
        """爬虫任务执行函数 - 在独立线程中运行"""
        try:
            with app.app_context():
                # 获取全局变量
                crawl_status = app.config.get('CRAWL_STATUS', {})
                crawl_control = app.config.get('CRAWL_CONTROL', {})
                crawl_progress = app.config.get('CRAWL_PROGRESS', {})
                LOG_DIR = app.config.get('LOG_DIR', '')
                OPTIONS_FILE = app.config.get('OPTIONS_FILE', '')

                logger.info(f"[CRAWLER] 线程内部开始执行")

                crawl_control['paused'] = False
                crawl_control['stop'] = False
                crawl_progress.update({
                    'sections_total': 0,
                    'sections_done': 0,
                    'current_section': '',
                    'current_section_pages': 0,
                    'current_section_processed': 0,
                    'current_page': 0,
                    'max_pages': max_pages,
                    'processed_pages': 0,
                    'total_saved': 0,
                    'total_skipped': 0,
                    'current_section_saved': 0,
                    'current_section_skipped': 0,
                    'estimated_total_pages': 0
                })
                crawl_status['is_crawling'] = True

                # 构建更友好的状态消息
                def format_crawl_message(fids, date_mode, _date_value, dateline, max_pages):
                    # FID到分类名称的映射
                    from constants import SECTION_MAP

                    # 格式化分类名称
                    if fids and len(fids) > 0:
                        category_names = [SECTION_MAP.get(fid, f"板块{fid}") for fid in fids]
                        if len(category_names) == 1:
                            category_str = category_names[0]
                        elif len(category_names) <= 3:
                            category_str = "、".join(category_names)
                        else:
                            category_str = f"{category_names[0]}等{len(category_names)}个板块"
                    else:
                        category_str = "全部板块"

                    # 格式化时间范围
                    time_ranges = {
                        'all': '全部时间',
                        'day': '一天内', '1day': '一天内',
                        '2day': '两天内',
                        '3day': '三天内',
                        'week': '一周内', '1week': '一周内',
                        'month': '一个月内', '1month': '一个月内',
                        '3month': '三个月内',
                        '6month': '半年内',
                        'year': '一年内', '1year': '一年内'
                    }

                    if date_mode and date_mode != 'all':
                        time_str = time_ranges.get(date_mode, f'{date_mode}内')
                    elif dateline:
                        time_str = f'指定时间范围'
                    else:
                        time_str = '全部时间'

                    return f'正在爬取 {category_str} - {time_str} - {max_pages}页'

                crawl_status['message'] = format_crawl_message(section_fids, date_mode, date_value, dateline, max_pages)
                logger.info(f"[CRAWLER] 准备启动爬虫: {crawl_status['message']}")

                try:
                    # 延迟导入以避免循环依赖
                    logger.info(f"[CRAWLER] 导入 crawler_scheduler 模块")
                    from scheduler.core import run_crawling_with_options

                    logger.info(f"[CRAWLER] 保存爬虫配置到: {OPTIONS_FILE}")
                    try:
                        os.makedirs(LOG_DIR, exist_ok=True)
                        with open(OPTIONS_FILE, 'w', encoding='utf-8') as f:
                            json.dump({
                                'fids': section_fids,
                                'date_mode': date_mode,
                                'date_value': date_value,
                                'dateline': dateline,
                                'max_pages': max_pages,
                                'page_mode': page_mode,
                                'page_range': page_range # 保存页码范围
                            }, f, ensure_ascii=False)
                        logger.info(f"[CRAWLER] 配置保存成功")
                    except Exception as e:
                        logger.warning(f"[CRAWLER] 配置保存失败: {e}")

                    logger.info(f"[CRAWLER] 开始执行爬虫任务")
                    run_crawling_with_options(section_fids=section_fids, date_mode=date_mode, date_value=date_value, dateline=dateline, max_pages=max_pages, page_mode=page_mode, page_range=page_range, task_type='manual')
                    crawl_status['message'] = '爬取完成'
                    crawl_status['last_crawl_time'] = datetime.now().isoformat()
                    logger.info(f"[CRAWLER] 爬虫任务完成")
                except Exception as e:
                    import traceback
                    logger.error(f"✗ [CRAWLER] 爬取异常详情: {traceback.format_exc()}")
                    crawl_status['message'] = f'爬取出错: {str(e)}'
                finally:
                    crawl_status['is_crawling'] = False
                    crawl_status['is_paused'] = False  # 确保清除暂停标志
                    logger.info(f"[CRAWLER] 爬虫任务完成")

                    # 重置状态协调器到空闲状态
                    try:
                        from crawler_control.cc_control_bridge import get_crawler_control_bridge
                        bridge = get_crawler_control_bridge()
                        bridge.reset_to_idle()
                        logger.info(f"[CRAWLER] 状态协调器已重置到空闲状态")
                    except Exception as reset_err:
                        logger.warning(f"[CRAWLER] 重置状态协调器失败: {reset_err}")

                    # 清除共享状态中的暂停标志
                    try:
                        crawl_control = app.config.get('CRAWL_CONTROL', {})
                        crawl_control['paused'] = False
                        crawl_control['stop'] = False
                        cache_manager.shared_set(CacheKeys.CRAWL_CONTROL, crawl_control)
                        cache_manager.shared_set(CacheKeys.CRAWL_STATUS, crawl_status)
                    except Exception as cache_err:
                        logger.warning(f"[CRAWLER] 清除共享状态失败: {cache_err}")

        except Exception as e:
            import traceback
            logger.error(f"✗ [CRAWLER] 线程执行失败: {traceback.format_exc()}")

            # 发送异常终止通知
            try:
                from scheduler.notifier import _send_telegram_message, render_message_template
                error_msg, parse_mode = render_message_template('crawler_thread_error', {
                    'error_type': '线程执行失败',
                    'error_message': str(e)[:200],
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'suggestion': '检查日志获取详细错误信息'
                })
                if not error_msg:
                    error_msg = f"""❌ *爬虫任务异常终止*
━━━━━━━━━━━━━━
⚠️ 错误类型：线程执行失败
📝 错误信息：{str(e)[:200]}
⏰ 终止时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
💡 建议：检查日志获取详细错误信息"""
                    parse_mode = 'Markdown'
                _send_telegram_message(error_msg, parse_mode=parse_mode)
            except Exception as notify_err:
                logger.error(f"发送异常通知失败: {notify_err}")

            # 清理状态
            try:
                crawl_status = app.config.get('CRAWL_STATUS', {})
                crawl_status['is_crawling'] = False
                crawl_status['message'] = f'任务异常终止: {str(e)[:100]}'

                from crawler_control.cc_control_bridge import get_crawler_control_bridge
                bridge = get_crawler_control_bridge()
                bridge.coordinator.reset_state()
            except Exception as cleanup_err:
                logger.error(f"清理状态失败: {cleanup_err}")

    thread = threading.Thread(target=run_task)
    thread.daemon = False  # 非daemon线程，防止静默终止
    thread.start()

    logger.info(f"[CRAWLER] 爬虫线程已启动，线程ID: {thread.name}")

    duration_ms = (time.time() - start_time) * 1000

    log_api_call(
        logger,
        method='POST',
        endpoint='/api/crawl/start',
        params={'fids': section_fids, 'date_mode': date_mode, 'max_pages': max_pages},
        status='success',
        response_code=200,
        duration_ms=duration_ms
    )

    return success_response(
        message='爬虫任务已启动'
    )


@api_crawl_bp.route('/crawl/stop', methods=['POST'])
def api_crawl_stop():
    """停止爬虫任务"""
    start_time = time.time()
    from crawler_control.cc_control_bridge import get_crawler_control_bridge

    crawl_status = current_app.config.get('CRAWL_STATUS', {})

    if not crawl_status.get('is_crawling'):
        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/crawl/stop',
            params={},
            status='error',
            response_code=400,
            duration_ms=duration_ms,
            error='当前没有正在运行的爬虫任务'
        )

        return error_response(
            code=ErrorCode.BUSINESS_ERROR,
            message='当前没有正在运行的爬虫任务'
        )

    try:
        # 使用新的信号队列系统
        bridge = get_crawler_control_bridge()
        signal_id = bridge.send_stop_signal()

        # 更新状态显示
        crawl_status['message'] = '正在停止...'

        # 同步到共享状态（保持兼容性）
        crawl_control = current_app.config.get('CRAWL_CONTROL', {})
        crawl_control['stop'] = True
        crawl_control['paused'] = False
        cache_manager.shared_set(CacheKeys.CRAWL_CONTROL, crawl_control)
        cache_manager.shared_set(CacheKeys.CRAWL_STATUS, crawl_status)

        logger.info(f"[CRAWLER] 收到停止请求，信号ID: {signal_id}")

        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/crawl/stop',
            params={},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            data={'signal_id': signal_id},
            message='停止指令已发送'
        )
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/crawl/stop'},
            message='发送停止信号失败'
        )

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/crawl/stop',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='发送停止信号失败',
            details=str(e)
        )


@api_crawl_bp.route('/crawl/pause', methods=['POST'])
def api_crawl_pause():
    """暂停爬虫任务"""
    start_time = time.time()

    try:
        from crawler_control.cc_control_bridge import get_crawler_control_bridge

        crawl_status = current_app.config.get('CRAWL_STATUS', {})

        if not crawl_status.get('is_crawling'):
            duration_ms = (time.time() - start_time) * 1000

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/pause',
                params={},
                status='error',
                response_code=400,
                duration_ms=duration_ms,
                error='当前没有正在运行的爬虫任务'
            )

            return error_response(
                code=ErrorCode.BUSINESS_ERROR,
                message='当前没有正在运行的爬虫任务'
            )

        # 使用新的信号队列系统
        bridge = get_crawler_control_bridge()

        # 检查当前状态，避免重复暂停
        try:
            current_state = bridge.get_current_state()
            if current_state.get('is_paused'):
                logger.info(f"[CRAWLER] 爬虫已经处于暂停状态，忽略重复请求")

                duration_ms = (time.time() - start_time) * 1000

                log_api_call(
                    logger,
                    method='POST',
                    endpoint='/api/crawl/pause',
                    params={'note': '已暂停'},
                    status='success',
                    response_code=200,
                    duration_ms=duration_ms
                )

                return success_response(
                    data={'already_paused': True},
                    message='爬虫已处于暂停状态'
                )
        except Exception as state_err:
            logger.warning(f"[CRAWLER] 获取当前状态失败: {state_err}，继续发送暂停信号")

        signal_id = bridge.send_pause_signal()

        # 更新状态显示
        crawl_status['message'] = '正在暂停...'

        # 同步到共享状态（保持兼容性）
        crawl_control = current_app.config.get('CRAWL_CONTROL', {})
        crawl_control['paused'] = True
        cache_manager.shared_set(CacheKeys.CRAWL_CONTROL, crawl_control)
        cache_manager.shared_set(CacheKeys.CRAWL_STATUS, crawl_status)

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
            data={'signal_id': signal_id},
            message='暂停指令已发送'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/crawl/pause'},
            message='发送暂停信号失败'
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
            message='发送暂停信号失败',
            details=str(e)
        )


@api_crawl_bp.route('/crawl/resume', methods=['POST'])
def api_crawl_resume():
    """恢复爬虫任务"""
    start_time = time.time()
    from crawler_control.cc_control_bridge import get_crawler_control_bridge

    crawl_status = current_app.config.get('CRAWL_STATUS', {})

    if not crawl_status.get('is_crawling'):
        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/crawl/resume',
            params={},
            status='error',
            response_code=400,
            duration_ms=duration_ms,
            error='当前没有正在运行的爬虫任务'
        )

        return error_response(
            code=ErrorCode.BUSINESS_ERROR,
            message='当前没有正在运行的爬虫任务'
        )

    try:
        # 使用新的信号队列系统
        bridge = get_crawler_control_bridge()
        signal_id = bridge.send_resume_signal()

        # 更新状态显示
        crawl_status['message'] = '正在恢复...'

        # 同步到共享状态（保持兼容性）
        crawl_control = current_app.config.get('CRAWL_CONTROL', {})
        crawl_control['paused'] = False
        cache_manager.shared_set(CacheKeys.CRAWL_CONTROL, crawl_control)
        cache_manager.shared_set(CacheKeys.CRAWL_STATUS, crawl_status)

        logger.info(f"[CRAWLER] 收到恢复请求，信号ID: {signal_id}")

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
            data={'signal_id': signal_id},
            message='恢复指令已发送'
        )
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/crawl/resume'},
            message='发送恢复信号失败'
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
            message='发送恢复信号失败',
            details=str(e)
        )


@api_crawl_bp.route('/crawl/force-clear', methods=['POST'])
def api_force_clear_crawl():
    """
    强制清除当前任务

    直接重置所有状态，不经过信号队列
    用于紧急情况下的状态恢复
    """
    start_time = time.time()

    try:
        from crawler_control.cc_control_bridge import get_crawler_control_bridge

        bridge = get_crawler_control_bridge()

        # 1. 清除所有待处理信号
        bridge.queue_manager.clear_signals()

        # 2. 重置状态协调器到初始状态
        bridge.coordinator.reset_state()

        # 3. 清除 app.config 中的状态（关键！）
        crawl_status = current_app.config.get('CRAWL_STATUS', {})
        crawl_status['is_crawling'] = False
        crawl_status['is_paused'] = False
        crawl_status['message'] = '空闲'

        crawl_control = current_app.config.get('CRAWL_CONTROL', {})
        crawl_control['paused'] = False
        crawl_control['stop'] = False

        crawl_progress = current_app.config.get('CRAWL_PROGRESS', {})
        crawl_progress.clear()

        # 4. 清除缓存中的共享状态
        cache_manager.shared_set(CacheKeys.IS_CRAWLING, False)
        cache_manager.shared_set(CacheKeys.IS_PAUSED, False)
        cache_manager.shared_set(CacheKeys.CRAWL_PROGRESS, {})

        # 5. 清除爬虫控制状态（旧系统兼容）
        cache_manager.shared_set(CacheKeys.CRAWL_CONTROL, {
            'running': False,
            'paused': False,
            'stopping': False
        })

        # 6. 清除爬虫状态缓存（旧系统兼容）
        cache_manager.shared_set(CacheKeys.CRAWL_STATUS, {
            'is_crawling': False,
            'message': '空闲'
        })

        # 7. 清除统一爬虫状态（新状态管理器）
        cache_manager.shared_set(CacheKeys.CRAWL_UNIFIED_STATE, {
            'is_crawling': False,
            'is_paused': False,
            'should_stop': False,
            'message': '空闲',
            'progress_percent': 0,
            'total_saved': 0,
            'total_skipped': 0,
            'total_failed': 0
        })

        logger.info("🔄 [CRAWLER] 强制清除任务完成")

        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/crawl/force-clear',
            params={},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            message='已强制清除当前任务'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/crawl/force-clear'},
            message='强制清除任务失败'
        )

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/crawl/force-clear',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='强制清除任务失败',
            details=str(e)
        )


@api_crawl_bp.route('/crawl/summary')
def api_crawl_summary():
    """获取爬虫摘要"""
    SUMMARY_FILE = current_app.config.get('SUMMARY_FILE', '')
    try:
        if os.path.exists(SUMMARY_FILE):
            with open(SUMMARY_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return jsonify(data)
    except Exception:
        pass
    return jsonify({})


@api_crawl_bp.route('/crawl/options', methods=['GET', 'POST'])
def api_crawl_options():
    """获取/保存爬虫选项"""
    start_time = time.time()
    LOG_DIR = current_app.config.get('LOG_DIR', '')
    OPTIONS_FILE = current_app.config.get('OPTIONS_FILE', '')

    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
            with open(OPTIONS_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    'fids': data.get('fids'),
                    'date_mode': data.get('date_mode'),
                    'date_value': data.get('date_value'),
                    'dateline': data.get('dateline'),
                    'max_pages': int(data.get('max_pages') or 3)
                }, f, ensure_ascii=False)

            duration_ms = (time.time() - start_time) * 1000

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/options',
                params={'data_keys': list(data.keys()) if data else []},
                status='success',
                response_code=200,
                duration_ms=duration_ms
            )

            return success_response(message='保存成功')
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000

            log_error_with_traceback(
                logger,
                e,
                context={'endpoint': '/api/crawl/options', 'method': 'POST'},
                message='保存选项失败'
            )

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/options',
                params={},
                status='error',
                response_code=500,
                duration_ms=duration_ms,
                error=str(e)
            )

            return error_response(
                code=ErrorCode.INTERNAL_ERROR,
                message='保存选项失败',
                details=str(e)
            )
    else:
        try:
            if os.path.exists(OPTIONS_FILE):
                with open(OPTIONS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                    duration_ms = (time.time() - start_time) * 1000

                    log_api_call(
                        logger,
                        method='GET',
                        endpoint='/api/crawl/options',
                        params={},
                        status='success',
                        response_code=200,
                        duration_ms=duration_ms
                    )

                    return success_response(
                        data=data,
                        message='获取选项成功'
                    )

            duration_ms = (time.time() - start_time) * 1000

            log_api_call(
                logger,
                method='GET',
                endpoint='/api/crawl/options',
                params={'note': 'no options file'},
                status='success',
                response_code=200,
                duration_ms=duration_ms
            )

            return success_response(
                data={},
                message='暂无选项配置'
            )
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000

            log_error_with_traceback(
                logger,
                e,
                context={'endpoint': '/api/crawl/options', 'method': 'GET'},
                message='获取选项失败'
            )

            log_api_call(
                logger,
                method='GET',
                endpoint='/api/crawl/options',
                params={},
                status='error',
                response_code=500,
                duration_ms=duration_ms,
                error=str(e)
            )

            return error_response(
                code=ErrorCode.INTERNAL_ERROR,
                message='获取选项失败',
                details=str(e)
            )


@api_crawl_bp.route('/crawl/config', methods=['GET'])
def api_crawl_config_get():
    """获取爬虫配置"""
    try:
        from configuration import config_manager
        config = config_manager.get_crawl_summary()

        return jsonify({
            'status': 'success',
            'data': config
        })
    except Exception as e:
        logger.error(f"获取爬虫配置失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_crawl_bp.route('/crawl/config', methods=['POST'])
def api_crawl_config_set():
    """保存爬虫配置"""
    try:
        from configuration import config_manager

        data = request.get_json()
        if not data:
            return jsonify({
                'status': 'error',
                'message': '无效的请求数据'
            }), 400

        # 使用新的一次性设置方法
        success = config_manager.update(data, section='crawler')

        if success:
            return jsonify({
                'status': 'success',
                'message': '配置保存成功'
            })
        else:
            return jsonify({
                'status': 'error',
                'message': '保存配置失败'
            }), 500

    except Exception as e:
        logger.error(f"✗ [CONFIG] 保存爬虫配置失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_crawl_bp.route('/crawl/selected-forums', methods=['GET'])
def api_get_selected_forums():
    """获取选中的论坛列表"""
    start_time = time.time()

    try:
        from configuration import config_manager
        selected_forums = config_manager.get_crawl_config('selected_forums') or []

        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/crawl/selected-forums',
            params={},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            data={'selected_forums': selected_forums},
            message='获取选中论坛成功'
        )
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/crawl/selected-forums', 'method': 'GET'},
            message='获取选中论坛失败'
        )

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/crawl/selected-forums',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='获取选中论坛失败',
            details=str(e)
        )


@api_crawl_bp.route('/crawl/selected-forums', methods=['POST'])
def api_set_selected_forums():
    """保存选中的论坛列表"""
    start_time = time.time()

    try:
        from configuration import config_manager

        data = request.get_json()
        if not data or 'selected_forums' not in data:
            duration_ms = (time.time() - start_time) * 1000

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/selected-forums',
                params={},
                status='error',
                response_code=400,
                duration_ms=duration_ms,
                error='无效的请求数据'
            )

            return invalid_request_response('无效的请求数据')

        selected_forums = data['selected_forums']
        if not isinstance(selected_forums, list):
            duration_ms = (time.time() - start_time) * 1000

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/selected-forums',
                params={'selected_forums': type(selected_forums).__name__},
                status='error',
                response_code=400,
                duration_ms=duration_ms,
                error='选中论坛必须是数组'
            )

            return invalid_parameter_response('selected_forums', details='选中论坛必须是数组')

        success = config_manager.set_crawl_config('selected_forums', selected_forums)

        if success:
            duration_ms = (time.time() - start_time) * 1000

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/selected-forums',
                params={'count': len(selected_forums)},
                status='success',
                response_code=200,
                duration_ms=duration_ms
            )

            return success_response(
                message='选中论坛已保存'
            )
        else:
            duration_ms = (time.time() - start_time) * 1000

            log_api_call(
                logger,
                method='POST',
                endpoint='/api/crawl/selected-forums',
                params={},
                status='error',
                response_code=500,
                duration_ms=duration_ms,
                error='保存失败'
            )

            return operation_failed_response('保存失败')

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/crawl/selected-forums', 'method': 'POST'},
            message='保存选中论坛失败'
        )

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/crawl/selected-forums',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='保存选中论坛失败',
            details=str(e)
        )


# ==================== 论坛信息API ====================

@api_crawl_bp.route('/forum/info/<fid>')
def api_forum_info(fid):
    """获取板块信息API - 使用持久化存储"""
    try:
        # 尝试从数据库获取
        cat = Category.query.filter_by(fid=str(fid)).first()
        if cat:
            return jsonify({
                'status': 'success',
                'data': cat.to_dict(),
                'cached': True
            })

        # 如果数据库没有，尝试获取新数据并保存
        logger.debug(f"[DB] 数据库未命中，尝试获取板块信息: fid={fid}")
        from crawler import SHT  # 从新的 crawler 模块导入
        sht = SHT()

        # 传递已有分类信息作为缓存参考
        all_categories = {c.fid: c.to_dict() for c in Category.query.all()}
        forum_info = sht.get_forum_info(fid, all_categories if all_categories else None)

        if forum_info:
            cat = Category(
                fid=str(fid),
                name=forum_info.get('name', 'Unknown'),
                parent_id=str(forum_info.get('parent_id', '')) if forum_info.get('parent_id') else None,
                description=forum_info.get('description')
            )
            db.session.add(cat)
            db.session.commit()

            return jsonify({
                'status': 'success',
                'data': cat.to_dict(),
                'cached': False
            })
        else:
            return jsonify({
                'status': 'error',
                'message': '无法获取板块信息'
            }), 404

    except Exception as e:
        logger.error(f"✗ [CRAWLER] 获取板块信息失败: fid={fid}, 错误: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_crawl_bp.route('/forum/info/batch')
def api_forum_info_batch():
    """批量获取所有板块信息API - 使用持久化存储"""
    # 添加明确的入口日志
    logger.info(f"[API] ========================================")
    logger.info(f"[API] 收到 /api/forum/info/batch 请求")
    logger.info(f"[API] ========================================")

    # 获取参数
    fast_mode = request.args.get('fast', 'false').lower() == 'true'
    force_refresh = request.args.get('force', 'false').lower() == 'true'

    logger.info(f"[API] 请求参数: fast_mode={fast_mode}, force_refresh={force_refresh}")

    try:
        # 检查是否强制刷新
        if not force_refresh:
            # 检查数据库内容
            categories = Category.get_all_categories()

            if categories:
                logger.debug(f"[DB] 从数据库获取论坛信息: {len(categories)}个板块")
                return jsonify({
                    'status': 'success',
                    'data': {c.fid: c.to_dict() for c in categories},
                    'cached': True,
                    'mode': 'fast' if fast_mode else 'detailed',
                    'cache_info': Category.get_cache_info()
                })

        # 缓存未命中、过期或强制刷新，获取新数据
        if force_refresh:
            logger.info(f"[CRAWLER] 强制刷新论坛信息")
        else:
            logger.info(f"[CACHE] 持久化缓存无效，重新获取论坛信息")

        from crawler import SHT  # 从新的 crawler 模块导入

        if fast_mode:
            # 快速模式：只获取基本信息
            sht = SHT()
            all_forums_info = sht.get_all_forums_info()

            if all_forums_info:
                # 保存到数据库
                Category.update_forum_info(all_forums_info)

                return jsonify({
                    'status': 'success',
                    'data': all_forums_info,
                    'cached': False,
                    'mode': 'fast',
                    'cache_info': Category.get_cache_info()
                })
            else:
                return jsonify({
                    'status': 'error',
                    'message': '无法获取板块基本信息'
                }), 404

        else:
            # 详细模式：获取包括页数在内的详细信息
            sht = SHT()
            all_forums_info = sht.get_all_forums_info()

            if not all_forums_info:
                return jsonify({
                    'status': 'error',
                    'message': '无法获取板块基本信息'
                }), 404

            # 并发获取详细信息
            detailed_forums_info = {}

            def get_forum_detail(fid):
                """获取单个板块详细信息的线程函数"""
                try:
                    # 为每个线程创建独立的SHT实例
                    thread_sht = SHT()
                    # 传入全局缓存，避免重复网络请求
                    forum_detail = thread_sht.get_forum_info(fid, all_forums_info)
                    return fid, forum_detail
                except Exception as e:
                    logger.warning(f"! [CRAWLER] 线程获取板块 {fid} 详细信息异常: {e}")
                    return fid, None

            # 使用线程池并发获取（增加并发数）
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                # 提交所有任务
                future_to_fid = {
                    executor.submit(get_forum_detail, fid): fid
                    for fid in all_forums_info.keys()
                }

                # 收集结果
                for future in concurrent.futures.as_completed(future_to_fid, timeout=45):
                    try:
                        fid, forum_detail = future.result()
                        if forum_detail:
                            detailed_forums_info[fid] = forum_detail
                        else:
                            # 如果获取详细信息失败，使用基本信息
                            detailed_forums_info[fid] = all_forums_info[fid]
                            logger.warning(f"! [CRAWLER] 获取板块 {fid} 详细信息失败，使用基本信息")
                    except Exception as e:
                        fid = future_to_fid[future]
                        logger.warning(f"获取板块 {fid} 详细信息异常: {e}")
                        # 使用基本信息作为备用
                        detailed_forums_info[fid] = all_forums_info[fid]

            if detailed_forums_info:
                # 保存到数据库
                Category.update_forum_info(detailed_forums_info)

                logger.info(f"并发获取完成，共 {len(detailed_forums_info)} 个板块")

                return jsonify({
                    'status': 'success',
                    'data': detailed_forums_info,
                    'cached': False,
                    'mode': 'detailed',
                    'cache_info': Category.get_cache_info()
                })
            else:
                return jsonify({
                    'status': 'error',
                    'message': '无法获取板块详细信息'
                }), 404

    except Exception as e:
        logger.error(f"批量获取板块信息失败: {e}")
        return jsonify({
            'status': 'error',
            'message': f'获取板块信息失败: {str(e)}'
        }), 500


@api_crawl_bp.route('/forum/cache/info')
def api_forum_cache_info():
    """获取论坛板块数据库存储信息"""
    try:
        cache_info = Category.get_cache_info()
        return jsonify({
            'status': 'success',
            'cache_info': cache_info
        })
    except Exception as e:
        logger.error(f"获取缓存信息失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_crawl_bp.route('/forum/cache/clear', methods=['POST'])
def api_forum_cache_clear():
    """清除论坛板块数据 (重置 last_updated 来强制刷新)"""
    try:
        # 通过将 last_updated 设置为过去的时间来使缓存失效
        db.session.query(Category).update({Category.last_updated: datetime.now(timezone.utc) - timedelta(days=365)})
        db.session.commit()
        return jsonify({
            'status': 'success',
            'message': '板块缓存已重置'
        })
    except Exception as e:
        logger.error(f"清除缓存失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


# ==================== 健康检查API ====================

@api_crawl_bp.route('/health')
def api_health():
    """系统健康检查API - 统一转发至 health 枢纽"""
    from health import monitor

    # 检查是否强制刷新
    force_refresh = request.args.get('force', 'false').lower() == 'true'

    try:
        # 尝试从缓存获取（除非强制刷新）
        cache_key = f"{CacheKeys.HEALTH}:check"
        if not force_refresh:
            cached_health = cache_manager.get(cache_key)
            if cached_health:
                return jsonify(cached_health)

        # 获取健康摘要
        health_summary = monitor.get_summary()

        # 补充配置与代理状态
        config_status = Config.get_config_summary()
        proxy_status = {'enabled': False, 'working': False, 'details': {}}

        if config_status.get('proxy_enabled', False):
            proxy_status['enabled'] = True
            proxy_url = Config.PROXY
            try:
                import requests
                from utils.retry_utils import retry_request, RETRY_CONFIG

                config = RETRY_CONFIG['proxy_check']
                resp = retry_request(
                    requests.get,
                    url='http://httpbin.org/ip',
                    proxies={'http': proxy_url, 'https': proxy_url},
                    raise_on_fail=False,
                    **config
                )
                proxy_status['working'] = resp is not None and resp.status_code == 200
                if proxy_status['working']:
                    proxy_status['details']['proxy_ip'] = resp.json().get('origin', 'Unknown')
            except:
                proxy_status['working'] = False

        # 获取统计概览
        db_metrics = health_summary.get('metrics', {}).get('db', {})

        # 安全退保：如果指标为空，尝试即时采集
        if not db_metrics or db_metrics.get('status') != 'ok':
            try:
                db_metrics = monitor._get_db_info()
            except:
                pass

        # 兼容性适配
        db_ok = db_metrics.get('resources', 0) > 0 or db_metrics.get('categories', 0) > 0

        # 系统各个组件的状态汇总
        database_status = {
            'status': 'ok' if db_ok else 'warning',
            'categories': db_metrics.get('categories', 0),
            'resources': db_metrics.get('resources', 0),
            'details': db_metrics
        }

        # SHT2BM 状态检测
        sht2bm_enabled = os.environ.get('ENABLE_SHT2BM', 'true').lower() == 'true'
        sht2bm_running = False

        # 检测 SHT2BM API 是否已注册（直接检查 Blueprint，避免 HTTP 请求导致死锁）
        if sht2bm_enabled:
            try:
                # 检查 Blueprint 是否已注册
                from flask import current_app as _app
                sht2bm_running = 'sht2bm' in _app.blueprints
                if not sht2bm_running:
                    # 降级：尝试导入模块检测
                    try:
                        from sht2bm_adapter import sht2bm_bp
                        sht2bm_running = True
                    except:
                        pass
            except:
                sht2bm_running = False

        sht2bm_status = {
            'enabled': sht2bm_enabled,
            'running': sht2bm_running,
            'status': 'ok' if sht2bm_running else ('disabled' if not sht2bm_enabled else 'error'),
            'integrated': True,  # 标记为内置集成
            'endpoint': '/api/bt' if sht2bm_running else None
        }

        # 合并结果
        result = {
            'status': health_summary['status'],
            'score': health_summary['score'],
            'issues': health_summary['issues'],
            'metrics': health_summary['metrics'],
            'database': database_status,
            'sht2bm': sht2bm_status,
            'cache': health_summary['metrics'].get('cache', {}),
            'config': config_status,
            'config_status': config_status,
            'proxy_status': proxy_status,
            'version': Config.VERSION,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

        # 存入缓存 (5分钟)
        cache_manager.set(cache_key, result, ttl=300)
        return jsonify(result)

    except Exception as e:
        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/health'},
            message='健康检查失败'
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='健康检查失败',
            details=str(e)
        )


# ==================== SHT2BM API ====================

@api_crawl_bp.route('/sht2bm/start', methods=['POST'])
def api_sht2bm_start():
    """启动 SHT2BM 服务"""
    try:
        # 检查服务是否已经运行
        sht2bm_port = os.environ.get('BM_PORT', '5144')
        try:
            import requests
            from utils.retry_utils import retry_request, RETRY_CONFIG

            config = RETRY_CONFIG['health_check']
            response = retry_request(
                requests.get,
                url=f'http://localhost:{sht2bm_port}/api/health',
                raise_on_fail=False,
                **config
            )
            if response and response.status_code == 200:
                return jsonify({
                    'status': 'success',
                    'message': 'SHT2BM 服务已在运行',
                    'port': sht2bm_port
                })
        except:
            pass  # 服务未运行，继续启动

        # 启动服务
        def start_service():
            try:
                # 设置环境变量 - 使用配置管理器获取数据库路径
                from configuration import Config
                db_path = os.environ.get('DB_PATH', Config.get_path('db_path'))
                os.environ['SHT_DB_PATH'] = db_path
                os.environ['PORT'] = sht2bm_port

                # SHT2BM 已改为 Blueprint 方式集成，不需要单独启动
                logger.info("SHT2BM 服务已通过 Blueprint 集成，无需单独启动")

            except Exception as e:
                logger.error(f"SHT2BM 服务启动失败: {e}")

        # 在后台线程中启动
        thread = threading.Thread(target=start_service, daemon=True, name='SHT2BM-Manual-Start')
        thread.start()

        # 等待一下让服务启动
        time.sleep(2)

        # 验证服务是否启动成功
        try:
            import requests
            from utils.retry_utils import retry_request, RETRY_CONFIG

            config = RETRY_CONFIG['health_check']
            response = retry_request(
                requests.get,
                url=f'http://localhost:{sht2bm_port}/api/health',
                raise_on_fail=False,
                **config
            )
            if response and response.status_code == 200:
                return jsonify({
                    'status': 'success',
                    'message': 'SHT2BM 服务启动成功',
                    'port': sht2bm_port
                })
            else:
                return jsonify({
                    'status': 'error',
                    'message': f'SHT2BM 服务启动失败，响应码: {response.status_code if response else "N/A"}'
                }), 500
        except Exception as e:
            return jsonify({
                'status': 'error',
                'message': f'SHT2BM 服务启动失败: {str(e)}'
            }), 500

    except Exception as e:
        logger.error(f"启动 SHT2BM 服务失败: {e}")
        return jsonify({
            'status': 'error',
            'message': f'启动失败: {str(e)}'
        }), 500


@api_crawl_bp.route('/sht2bm/stop', methods=['POST'])
def api_sht2bm_stop():
    """停止 SHT2BM 服务"""
    try:
        import psutil

        sht2bm_port = os.environ.get('BM_PORT', '5144')

        # 查找 SHT2BM 进程
        killed_processes = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if 'sht2bm.py' in cmdline or f'PORT={sht2bm_port}' in cmdline:
                    proc.terminate()  # 优雅终止
                    killed_processes.append(proc.info['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        if killed_processes:
            return jsonify({
                'status': 'success',
                'message': f'SHT2BM 服务已停止，终止进程: {killed_processes}',
                'port': sht2bm_port
            })
        else:
            return jsonify({
                'status': 'warning',
                'message': 'SHT2BM 服务未运行或无法找到相关进程',
                'port': sht2bm_port
            })

    except Exception as e:
        logger.error(f"停止 SHT2BM 服务失败: {e}")
        return jsonify({
            'status': 'error',
            'message': f'停止失败: {str(e)}'
        }), 500


@api_crawl_bp.route('/sht2bm/sync', methods=['POST'])
def api_sht2bm_sync():
    """手动同步数据到SHT2BM"""
    try:
        sht2bm_port = os.environ.get('BM_PORT', '5144')

        # 检查SHT2BM服务是否运行
        try:
            import requests
            from utils.retry_utils import retry_request, RETRY_CONFIG

            config = RETRY_CONFIG['health_check']
            response = retry_request(
                requests.get,
                url=f'http://localhost:{sht2bm_port}/api/health',
                raise_on_fail=False,
                **config
            )
            if not response or response.status_code != 200:
                return jsonify({
                    'status': 'error',
                    'message': 'SHT2BM服务未运行，请先启动服务'
                }), 400
        except:
            return jsonify({
                'status': 'error',
                'message': 'SHT2BM服务连接失败'
            }), 400

        # 触发数据同步
        try:
            import requests
            from utils.retry_utils import retry_request, RETRY_CONFIG

            config = RETRY_CONFIG['sht2bm_api']
            sync_response = retry_request(
                requests.post,
                url=f'http://localhost:{sht2bm_port}/api/sync',
                raise_on_fail=False,
                **config
            )
            if sync_response and sync_response.status_code == 200:
                return jsonify({
                    'status': 'success',
                    'message': 'SHT2BM数据同步完成',
                    'data': sync_response.json()
                })
            else:
                return jsonify({
                    'status': 'error',
                    'message': f'同步失败: HTTP {sync_response.status_code if sync_response else "N/A"}'
                }), 500
        except Exception as e:
            return jsonify({
                'status': 'error',
                'message': f'同步失败: {str(e)}'
            }), 500

    except Exception as e:
        logger.error(f"SHT2BM同步失败: {e}")
        return jsonify({
            'status': 'error',
            'message': f'同步失败: {str(e)}'
        }), 500
