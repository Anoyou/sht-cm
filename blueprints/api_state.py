#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
状态同步 API - 多客户端实时状态同步
"""

import time
import logging
from flask import Blueprint, jsonify, request
from utils.state_manager import get_state_manager, get_unified_state, sync_from_legacy
from utils.api_response import (
    success_response,
    error_response,
    ErrorCode,
    log_api_call,
    log_error_with_traceback
)

logger = logging.getLogger(__name__)

# 创建蓝图
api_state = Blueprint('api_state', __name__, url_prefix='/api/state')


@api_state.route('/sync', methods=['GET'])
def sync_state():
    """状态同步端点 - 支持增量更新"""
    start_time = time.time()

    try:
        # 获取客户端版本号
        client_version = request.args.get('version', 0, type=int)

        # 是否强制全量同步
        force_full = request.args.get('force', 'false').lower() == 'true'

        if force_full:
            client_version = 0

        # 从传统状态源同步最新数据
        sync_from_legacy()

        # 获取状态信息
        state_data = get_unified_state(client_version)

        # 添加服务器时间戳
        state_data['server_time'] = time.time()

        # 记录 API 调用日志
        duration_ms = (time.time() - start_time) * 1000
        log_api_call(
            logger,
            method='GET',
            endpoint='/api/state/sync',
            params={'version': client_version, 'force': force_full},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            data=state_data,
            message='状态同步成功'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        # 记录错误日志
        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/state/sync'},
            message='状态同步失败'
        )

        # 记录 API 调用日志（失败）
        log_api_call(
            logger,
            method='GET',
            endpoint='/api/state/sync',
            params={'version': request.args.get('version', 0), 'force': request.args.get('force')},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='状态同步失败',
            details=str(e)
        )


@api_state.route('/version', methods=['GET'])
def get_version():
    """获取当前状态版本号"""
    start_time = time.time()

    try:
        state_manager = get_state_manager()

        duration_ms = (time.time() - start_time) * 1000
        log_api_call(
            logger,
            method='GET',
            endpoint='/api/state/version',
            params={},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            data={
                'version': state_manager._state.version,
                'timestamp': state_manager._state.last_update_time,
                'server_time': time.time()
            },
            message='获取版本成功'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/state/version'},
            message='获取版本失败'
        )

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/state/version',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='获取版本失败',
            details=str(e)
        )


@api_state.route('/health', methods=['GET'])
def state_health():
    """状态管理器健康检查"""
    start_time = time.time()

    try:
        state_manager = get_state_manager()

        # 执行一次同步测试
        sync_from_legacy()

        duration_ms = (time.time() - start_time) * 1000
        log_api_call(
            logger,
            method='GET',
            endpoint='/api/state/health',
            params={},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            data={
                'status': 'healthy',
                'version': state_manager._state.version,
                'last_update': state_manager._state.last_update_time,
                'subscribers': len(state_manager._subscribers),
                'server_time': time.time()
            },
            message='健康检查通过'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/state/health'},
            message='状态健康检查失败'
        )

        log_api_call(
            logger,
            method='GET',
            endpoint='/api/state/health',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='状态健康检查失败',
            details=str(e),
            status='unhealthy'
        )


@api_state.route('/reset', methods=['POST'])
def reset_state():
    """重置状态（调试用）"""
    start_time = time.time()

    try:
        state_manager = get_state_manager()
        state_manager.reset_state()

        duration_ms = (time.time() - start_time) * 1000
        log_api_call(
            logger,
            method='POST',
            endpoint='/api/state/reset',
            params={},
            status='success',
            response_code=200,
            duration_ms=duration_ms
        )

        return success_response(
            message='状态已重置'
        )

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000

        log_error_with_traceback(
            logger,
            e,
            context={'endpoint': '/api/state/reset'},
            message='重置状态失败'
        )

        log_api_call(
            logger,
            method='POST',
            endpoint='/api/state/reset',
            params={},
            status='error',
            response_code=500,
            duration_ms=duration_ms,
            error=str(e)
        )

        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message='重置状态失败',
            details=str(e)
        )


# 注册蓝图的函数
def register_state_api(app):
    """注册状态 API 蓝图"""
    app.register_blueprint(api_state)
    logger.info("✓ 状态同步 API 已注册")
