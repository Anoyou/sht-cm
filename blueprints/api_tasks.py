#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务管理API Blueprint - 处理任务管理和失败TID相关的API
"""

import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from flask import Blueprint, jsonify, request

from models import db, FailedTID
from utils.api_response import (
    success_response,
    error_response,
    ErrorCode,
    missing_parameter_response,
    invalid_parameter_response,
    not_found_response,
    log_api_call,
    log_error_with_traceback
)

api_tasks_bp = Blueprint('api_tasks', __name__, url_prefix='/api')

logger = logging.getLogger(__name__)


# ==================== 任务管理API ====================

def _is_task_manager_available() -> bool:
    """检查任务管理器是否可用"""
    try:
        from task_manager import task_manager
        # 检查任务管理器是否存在且已启动
        return task_manager is not None and task_manager.running
    except Exception as e:
        logger.debug(f"检查任务管理器可用性失败: {e}")
        return False


def _task_manager_unavailable():
    """返回任务管理器不可用的错误响应"""
    return error_response(
        code=ErrorCode.OPERATION_IN_PROGRESS,
        message='任务管理器已停用。此功能不再支持。',
        details='任务管理已整合到爬虫控制系统中'
    )


@api_tasks_bp.route('/tasks/status')
def api_tasks_status():
    """获取所有任务状态"""
    # 检查任务管理器可用性
    if not _is_task_manager_available():
        return _task_manager_unavailable()

    try:
        from task_manager import task_manager

        tasks = task_manager.get_all_tasks_status()

        return jsonify({
            'status': 'success',
            'tasks': tasks,
            'total': len(tasks),
            'timestamp': datetime.now(timezone.utc).isoformat()
        })

    except Exception as e:
        logger.error(f"获取任务状态失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'tasks': [],
            'total': 0
        }), 500


@api_tasks_bp.route('/tasks/<task_id>')
def api_task_detail(task_id):
    """获取单个任务详情"""
    # 检查任务管理器可用性
    if not _is_task_manager_available():
        return _task_manager_unavailable()

    try:
        from task_manager import task_manager

        task = task_manager.get_task_status(task_id)

        if task:
            return jsonify({
                'status': 'success',
                'task': task
            })
        else:
            return jsonify({
                'status': 'error',
                'message': '任务不存在'
            }), 404

    except Exception as e:
        logger.error(f"获取任务详情失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_tasks_bp.route('/tasks/<task_id>/execute', methods=['POST'])
def api_task_execute(task_id):
    """手动执行任务"""
    # 检查任务管理器可用性
    if not _is_task_manager_available():
        return _task_manager_unavailable()

    try:
        from task_manager import task_manager

        # 获取任务信息
        task = task_manager.get_task_status(task_id)
        if not task:
            return jsonify({
                'status': 'error',
                'message': '任务不存在'
            }), 404

        # 检查是否可以手动执行
        if not task.get('manual_executable', False):
            return jsonify({
                'status': 'error',
                'message': '该任务不支持手动执行'
            }), 400

        # 检查任务是否正在运行
        if task.get('status') == 'running':
            return jsonify({
                'status': 'error',
                'message': '任务正在运行中，请稍后再试'
            }), 400

        # 执行任务
        success = task_manager.manual_execute(task_id)

        if success:
            logger.info(f"手动执行任务: {task['name']} ({task_id})")
            return jsonify({
                'status': 'success',
                'message': '任务已开始执行',
                'task_id': task_id,
                'task_name': task['name']
            })
        else:
            return jsonify({
                'status': 'error',
                'message': '任务执行失败'
            }), 500

    except Exception as e:
        logger.error(f"手动执行任务失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_tasks_bp.route('/tasks/<task_id>/pause', methods=['POST'])
def api_task_pause(task_id):
    """暂停任务"""
    # 检查任务管理器可用性
    if not _is_task_manager_available():
        return _task_manager_unavailable()

    try:
        from task_manager import task_manager

        # 获取任务信息
        task = task_manager.get_task_status(task_id)
        if not task:
            return jsonify({
                'status': 'error',
                'message': '任务不存在'
            }), 404

        # 暂停任务
        success = task_manager.pause_task(task_id)

        if success:
            logger.info(f"暂停任务: {task['name']} ({task_id})")
            return jsonify({
                'status': 'success',
                'message': '任务已暂停',
                'task_id': task_id,
                'task_name': task['name']
            })
        else:
            return jsonify({
                'status': 'error',
                'message': '暂停任务失败'
            }), 500

    except Exception as e:
        logger.error(f"暂停任务失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_tasks_bp.route('/tasks/<task_id>/resume', methods=['POST'])
def api_task_resume(task_id):
    """恢复任务"""
    # 检查任务管理器可用性
    if not _is_task_manager_available():
        return _task_manager_unavailable()

    try:
        from task_manager import task_manager

        # 获取任务信息
        task = task_manager.get_task_status(task_id)
        if not task:
            return jsonify({
                'status': 'error',
                'message': '任务不存在'
            }), 404

        # 恢复任务
        success = task_manager.resume_task(task_id)

        if success:
            logger.info(f"恢复任务: {task['name']} ({task_id})")
            return jsonify({
                'status': 'success',
                'message': '任务已恢复',
                'task_id': task_id,
                'task_name': task['name']
            })
        else:
            return jsonify({
                'status': 'error',
                'message': '恢复任务失败'
            }), 500

    except Exception as e:
        logger.error(f"恢复任务失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_tasks_bp.route('/tasks/manager/start', methods=['POST'])
def api_task_manager_start():
    """启动任务管理器"""
    try:
        from task_manager import task_manager

        if task_manager.running:
            return jsonify({
                'status': 'success',
                'message': '任务管理器已在运行'
            })

        task_manager.start_manager()

        return jsonify({
            'status': 'success',
            'message': '任务管理器已启动'
        })

    except Exception as e:
        logger.error(f"启动任务管理器失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_tasks_bp.route('/tasks/manager/stop', methods=['POST'])
def api_task_manager_stop():
    """停止任务管理器"""
    try:
        from task_manager import task_manager

        if not task_manager.running:
            return jsonify({
                'status': 'success',
                'message': '任务管理器已停止'
            })

        task_manager.stop_manager()

        return jsonify({
            'status': 'success',
            'message': '任务管理器已停止'
        })

    except Exception as e:
        logger.error(f"停止任务管理器失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_tasks_bp.route('/tasks/manager/status')
def api_task_manager_status():
    """获取任务管理器状态"""
    try:
        from task_manager import task_manager

        return jsonify({
            'status': 'success',
            'manager_running': task_manager.running,
            'total_tasks': len(task_manager.tasks),
            'running_tasks': len([t for t in task_manager.tasks.values() if t.status.value == 'running']),
            'timestamp': datetime.now(timezone.utc).isoformat()
        })

    except Exception as e:
        logger.error(f"获取任务管理器状态失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500



@api_tasks_bp.route('/tasks/sht2bm_service/start', methods=['POST'])
def api_task_sht2bm_start():
    """启动SHT2BM服务 - 现在是内置蓝图，直接返回成功"""
    from flask import current_app

    logger.info("请求启动 SHT2BM 服务 (当前已作为内置蓝图运行)")

    # 检查 Blueprint 是否已注册
    is_registered = 'sht2bm' in current_app.blueprints

    return jsonify({
        "status": "success" if is_registered else "warning",
        "message": "SHT2BM 服务已作为内置 Blueprint 运行，无需独立启动" if is_registered else "SHT2BM Blueprint 未注册",
        "details": {
            "integrated": True,
            "registered": is_registered,
            "endpoint": "/api/bt",
            "health_check": "/api/bt/health",
            "note": "此服务已整合到主应用中，随应用启动而自动运行"
        }
    })


@api_tasks_bp.route('/tasks/sht2bm_service/stop', methods=['POST'])
def api_task_sht2bm_stop():
    """停止SHT2BM服务 - 内置蓝图无法单独停止"""
    return jsonify({
        "status": "warning",
        "message": "SHT2BM 服务已整合为内置组件，无法单独停止。它将随主应用一同运行。",
        "can_stop": False
    })


# ==================== 失败TID管理API ====================

@api_tasks_bp.route('/failed-tids/summary')
def api_failed_tids_summary():
    """获取失败TID统计摘要"""
    try:
        summary = {
            'total': FailedTID.query.count(),
            'pending': FailedTID.query.filter_by(status='pending').count(),
            'retrying': FailedTID.query.filter_by(status='retrying').count(),
            'success': FailedTID.query.filter_by(status='success').count(),
            'abandoned': FailedTID.query.filter_by(status='abandoned').count()
        }

        return jsonify({
            'status': 'success',
            'data': summary
        })
    except Exception as e:
        logger.error(f"获取失败TID摘要失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_tasks_bp.route('/failed-tids/list')
def api_failed_tids_list():
    """获取失败TID列表"""
    try:
        section = request.args.get('section')
        status = request.args.get('status')
        limit = request.args.get('limit', 100, type=int)

        # 根据status参数过滤
        if status == 'abandoned':
            # 只返回已放弃的
            query = FailedTID.query.filter_by(status='abandoned')
        else:
            # 默认返回待重试的（pending 和 retrying）
            query = FailedTID.query.filter(FailedTID.status.in_(['pending', 'retrying']))

        if section:
            query = query.filter_by(section=section)

        failed_tids = query.order_by(FailedTID.updated_at.desc()).limit(limit).all()
        data = [f.to_dict() for f in failed_tids]

        return jsonify({
            'status': 'success',
            'data': data,
            'total': len(data)
        })
    except Exception as e:
        logger.error(f"获取失败TID列表失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_tasks_bp.route('/failed-tids/retry', methods=['POST'])
def api_failed_tids_retry():
    """重试失败TID"""
    try:
        from flask import current_app

        # 检查是否有正在运行的爬虫任务
        crawl_status = current_app.config.get('CRAWL_STATUS', {})
        if crawl_status.get('is_crawling'):
            return jsonify({
                'status': 'error',
                'message': '当前有爬虫任务正在运行，请等待完成后再重试TID'
            }), 400

        data = request.get_json() or {}
        section = data.get('section')
        limit = data.get('limit', 50)
        batch_size = data.get('batch_size', 10)
        continuous = data.get('continuous', False)  # 新增：是否循环模式
        max_rounds = data.get('max_rounds', 20)     # 新增：最大轮数

        # 导入重试服务
        from maintenance_tools import FailedTidRetryService

        retry_service = FailedTidRetryService()

        # 捕获 Flask app 实例
        app = current_app._get_current_object()

        # 使用状态协调器设置状态，防止冲突
        from crawler_control.cc_control_bridge import get_crawler_control_bridge
        bridge = get_crawler_control_bridge()
        
        # 标记为运行中
        bridge.start_crawling()
        mode_desc = f"循环模式（最多{max_rounds}轮）" if continuous else "单轮模式"
        bridge.update_progress({
            'message': f'正在重试失败的TID ({mode_desc})...',
            'current_section': '重试失败TID',
            'processed_pages': 0,
            'max_pages': max_rounds if continuous else 1
        })

        # 在后台线程中执行重试
        def run_retry():
            try:
                # 在线程中使用 Flask app context
                with app.app_context():
                    # 修复：使用正确的方法名 retry_failed_tids
                    result = retry_service.retry_failed_tids(
                        section=section,
                        limit=limit,
                        batch_size=batch_size,
                        continuous=continuous,
                        max_rounds=max_rounds
                    )
                    logger.info(f"重试完成: {result}")

            except Exception as e:
                logger.error(f"后台重试失败: {e}")
                import traceback
                logger.error(f"详细错误: {traceback.format_exc()}")
            finally:
                # 重试完成后重置到空闲状态
                try:
                    from crawler_control.cc_control_bridge import get_crawler_control_bridge
                    bridge = get_crawler_control_bridge()
                    bridge.stop_crawling()
                    logger.info("重试TID任务状态已重置")
                except Exception as reset_err:
                    logger.error(f"重试完成后重置状态失败: {reset_err}")

        retry_thread = threading.Thread(target=run_retry, daemon=True)
        retry_thread.start()

        mode_info = f"循环模式（最多{max_rounds}轮，每轮{limit}个）" if continuous else f"单轮模式（处理{limit}个）"
        return jsonify({
            'status': 'success',
            'message': f'重试任务已启动 ({mode_info})，请查看日志了解进度',
            'mode': 'continuous' if continuous else 'single',
            'limit': limit,
            'max_rounds': max_rounds if continuous else 1
        })

    except Exception as e:
        logger.error(f"启动重试任务失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_tasks_bp.route('/failed-tids/cleanup', methods=['POST'])
def api_failed_tids_cleanup():
    """清理失败TID记录 - 增强版"""
    try:
        data = request.get_json() or {}
        days = data.get('days')
        clear_all = data.get('all', False)
        keyword = data.get('keyword')
        status = data.get('status')

        query = FailedTID.query

        # 逻辑：all 的优先级最高
        if clear_all:
            deleted_count = query.delete()
            message = '已清空所有失败TID记录'
        else:
            # 按条件组合过滤
            if days is not None:
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=int(days))
                query = query.filter(FailedTID.updated_at < cutoff_date)
            
            if keyword:
                query = query.filter(FailedTID.failure_reason.ilike(f"%{keyword}%"))
            
            if status:
                query = query.filter(FailedTID.status == status)
            
            # 如果什么参数都没传，默认清理30天前的 (保持兼容)
            if days is None and not keyword and not status:
                days = 30
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
                query = query.filter(FailedTID.updated_at < cutoff_date)

            deleted_count = query.delete(synchronize_session=False)
            message = f'已清理符合条件的失败TID记录，共删除 {deleted_count} 条'

        db.session.commit()

        return jsonify({
            'status': 'success',
            'message': message,
            'deleted_count': deleted_count
        })
    except Exception as e:
        db.session.rollback()
        logger.error(f"清理失败TID记录失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@api_tasks_bp.route('/failed-tids/abandon', methods=['POST'])
def api_failed_tids_abandon():
    """手动放弃指定的失败TID"""
    start_time = time.time()

    try:
        data = request.get_json() or {}
        tid = data.get('tid')

        if not tid:
            return missing_parameter_response('tid')

        # 查找该TID的失败记录
        failed_entry = FailedTID.query.filter_by(tid=tid).first()

        if not failed_entry:
            return not_found_response(f'失败TID记录', details=f'未找到TID {tid} 的失败记录')

        # 标记为已放弃
        failed_entry.status = 'abandoned'
        failed_entry.failure_reason = (failed_entry.failure_reason or '') + ' [手动放弃]'
        failed_entry.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        logger.info(f"已手动放弃失败TID: {tid}")

        duration_ms = (time.time() - start_time) * 1000

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/failed-tids/abandon',
            params={'tid': tid},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            message=f'已放弃 TID {tid}',
            tid=tid
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/failed-tids/abandon'},
            message='放弃失败TID失败'
        )

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/failed-tids/abandon',
            params={'tid': request.get_json().get('tid') if request.is_json else None},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='放弃失败TID失败',
            details=str(e)
        )


@api_tasks_bp.route('/failed-tids/restore', methods=['POST'])
def api_failed_tids_restore():
    """将已放弃的TID重新加入重试列表"""
    try:
        data = request.get_json() or {}
        tid = data.get('tid')

        if not tid:
            return jsonify({
                'status': 'error',
                'message': 'TID参数缺失'
            }), 400

        # 查找该TID的失败记录
        failed_entry = FailedTID.query.filter_by(tid=tid, status='abandoned').first()

        if not failed_entry:
            return jsonify({
                'status': 'error',
                'message': f'未找到已放弃的TID {tid}'
            }), 404

        # 重新标记为待重试，重置重试次数
        failed_entry.status = 'pending'
        failed_entry.retry_count = 0
        failed_entry.failure_reason = (failed_entry.failure_reason or '').replace(' [手动放弃]', '') + ' [重新加入重试]'
        failed_entry.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        logger.info(f"已将TID {tid} 重新加入重试列表")

        return jsonify({
            'status': 'success',
            'message': f'TID {tid} 已重新加入重试列表'
        })

    except Exception as e:
        logger.error(f"恢复失败TID失败: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
