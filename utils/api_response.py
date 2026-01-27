#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一 API 响应和错误处理模块
- 统一 API 响应格式
- 定义错误码和错误信息
- 提供结构化日志记录
"""

import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from flask import jsonify, Response


# ==================== 错误码定义 ====================

class ErrorCode:
    """API 错误码枚举"""

    # 通用错误码
    SUCCESS = 0
    UNKNOWN_ERROR = 1000
    INVALID_REQUEST = 1001
    MISSING_PARAMETER = 1002
    INVALID_PARAMETER = 1003

    # 认证和授权
    UNAUTHORIZED = 2000
    FORBIDDEN = 2001

    # 资源相关
    NOT_FOUND = 3000
    RESOURCE_CONFLICT = 3001

    # 业务逻辑错误
    BUSINESS_ERROR = 4000
    OPERATION_FAILED = 4001
    OPERATION_IN_PROGRESS = 4002

    # 外部服务
    EXTERNAL_SERVICE_ERROR = 5000
    EXTERNAL_SERVICE_TIMEOUT = 5001

    # 系统错误
    INTERNAL_ERROR = 9000
    DATABASE_ERROR = 9001
    CACHE_ERROR = 9002


# 错误码对应的 HTTP 状态码映射
ERROR_CODE_TO_HTTP = {
    ErrorCode.SUCCESS: 200,
    ErrorCode.INVALID_REQUEST: 400,
    ErrorCode.MISSING_PARAMETER: 400,
    ErrorCode.INVALID_PARAMETER: 400,
    ErrorCode.UNAUTHORIZED: 401,
    ErrorCode.FORBIDDEN: 403,
    ErrorCode.NOT_FOUND: 404,
    ErrorCode.RESOURCE_CONFLICT: 409,
    ErrorCode.BUSINESS_ERROR: 400,
    ErrorCode.OPERATION_FAILED: 500,
    ErrorCode.OPERATION_IN_PROGRESS: 429,
    ErrorCode.EXTERNAL_SERVICE_ERROR: 502,
    ErrorCode.EXTERNAL_SERVICE_TIMEOUT: 504,
    ErrorCode.INTERNAL_ERROR: 500,
    ErrorCode.DATABASE_ERROR: 500,
    ErrorCode.CACHE_ERROR: 500,
}


# ==================== 统一响应函数 ====================

def success_response(data: Any = None, message: str = '操作成功', **extra) -> Response:
    """
    成功响应

    Args:
        data: 返回的数据
        message: 成功消息
        **extra: 额外的响应字段

    Returns:
        Flask Response 对象
    """
    response = {
        'code': ErrorCode.SUCCESS,
        'message': message,
        'success': True,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    if data is not None:
        response['data'] = data

    response.update(extra)
    return jsonify(response), 200


def error_response(
    code: int,
    message: str,
    details: Optional[str] = None,
    **extra
) -> Response:
    """
    错误响应

    Args:
        code: 错误码
        message: 错误消息
        details: 错误详情（用于调试）
        **extra: 额外的响应字段

    Returns:
        Flask Response 对象
    """
    http_status = ERROR_CODE_TO_HTTP.get(code, 500)

    response = {
        'code': code,
        'message': message,
        'success': False,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    if details:
        response['details'] = details

    response.update(extra)
    return jsonify(response), http_status


def unknown_error_response(message: str = '未知错误', details: Optional[str] = None, **extra) -> Response:
    """未知错误响应"""
    return error_response(ErrorCode.UNKNOWN_ERROR, message, details, **extra)


def invalid_request_response(message: str = '无效的请求', details: Optional[str] = None, **extra) -> Response:
    """无效请求响应"""
    return error_response(ErrorCode.INVALID_REQUEST, message, details, **extra)


def missing_parameter_response(param_name: str, details: Optional[str] = None, **extra) -> Response:
    """缺少参数响应"""
    return error_response(ErrorCode.MISSING_PARAMETER, f'缺少参数: {param_name}', details, **extra)


def invalid_parameter_response(param_name: str, details: Optional[str] = None, **extra) -> Response:
    """无效参数响应"""
    return error_response(ErrorCode.INVALID_PARAMETER, f'无效参数: {param_name}', details, **extra)


def not_found_response(resource: str = '资源', details: Optional[str] = None, **extra) -> Response:
    """资源未找到响应"""
    return error_response(ErrorCode.NOT_FOUND, f'{resource}不存在', details, **extra)


def operation_failed_response(message: str = '操作失败', details: Optional[str] = None, **extra) -> Response:
    """操作失败响应"""
    return error_response(ErrorCode.OPERATION_FAILED, message, details, **extra)


def operation_in_progress_response(message: str = '操作进行中', **extra) -> Response:
    """操作进行中响应"""
    return error_response(ErrorCode.OPERATION_IN_PROGRESS, message, **extra)


def internal_error_response(message: str = '内部错误', details: Optional[str] = None, **extra) -> Response:
    """内部错误响应"""
    return error_response(ErrorCode.INTERNAL_ERROR, message, details, **extra)


# ==================== 结构化日志工具 ====================

def log_api_call(
    logger: logging.Logger,
    method: str,
    endpoint: str,
    params: Dict,
    status: str,
    response_code: int,
    duration_ms: float,
    error: Optional[str] = None
):
    """
    记录 API 调用日志（结构化）
    """
    # 屏蔽高频轮询接口的日志记录
    if endpoint in ['/api/logs/recent', '/api/crawl/status', '/api/state/sync', '/api/stats/categories']:
        return
    log_data = {
        'type': 'api_call',
        'method': method,
        'endpoint': endpoint,
        'status': status,
        'code': response_code,
        'duration_ms': round(duration_ms, 2),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    if params:
        log_data['params'] = params

    if error:
        log_data['error'] = error
        logger.error(f'[API] {log_data}')
    else:
        logger.info(f'[API] {log_data}')


def log_business_event(
    logger: logging.Logger,
    event_type: str,
    event_data: Dict,
    level: str = 'info'
):
    """
    记录业务事件日志（结构化）

    Args:
        logger: 日志记录器
        event_type: 事件类型
        event_data: 事件数据
        level: 日志级别
    """
    log_data = {
        'type': 'business_event',
        'event_type': event_type,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    log_data.update(event_data)

    if level == 'error':
        logger.error(f'[BUSINESS] {log_data}')
    elif level == 'warning':
        logger.warning(f'[BUSINESS] {log_data}')
    else:
        logger.info(f'[BUSINESS] {log_data}')


def log_error_with_traceback(
    logger: logging.Logger,
    error: Exception,
    context: Optional[Dict] = None,
    message: str = '发生异常'
):
    """
    记录错误及其堆栈信息（结构化）

    Args:
        logger: 日志记录器
        error: 异常对象
        context: 上下文信息
        message: 错误消息
    """
    log_data = {
        'type': 'error',
        'message': message,
        'error_type': type(error).__name__,
        'error_message': str(error),
        'traceback': traceback.format_exc(),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    if context:
        log_data['context'] = context

    logger.error(f'[ERROR] {log_data}')


# ==================== 兼容性转换函数 ====================

def convert_legacy_response(
    status: str,
    message: str,
    data: Any = None,
    **extra
) -> Response:
    """
    将旧的响应格式转换为新的统一格式（用于渐进式迁移）

    Args:
        status: 旧格式的状态（'success' 或 'error'）
        message: 消息
        data: 数据
        **extra: 额外字段

    Returns:
        Flask Response 对象
    """
    if status == 'success':
        return success_response(data=data, message=message, **extra)
    else:
        return error_response(ErrorCode.BUSINESS_ERROR, message, details=data, **extra)
