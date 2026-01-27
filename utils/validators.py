#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一的 API 参数验证模块
- 分页参数验证
- 日期范围验证
- 字符串参数验证
- 数字参数验证
"""

import logging
from datetime import datetime
from typing import Optional, Tuple, Any
from flask import request

from utils.api_response import (
    invalid_parameter_response,
    missing_parameter_response,
    ErrorCode
)

logger = logging.getLogger(__name__)


# ==================== 分页验证 ====================

class PaginationValidator:
    """分页参数验证器"""

    DEFAULT_PAGE = 1
    DEFAULT_PER_PAGE = 20
    MIN_PAGE = 1
    MAX_PAGE = 10000  # 防止过大的页码
    MIN_PER_PAGE = 1
    MAX_PER_PAGE = 100

    @staticmethod
    def validate_page(
        page: Optional[int] = None,
        page_key: str = 'page'
    ) -> Tuple[int, Optional[dict]]:
        """
        验证页码参数

        Args:
            page: 页码值
            page_key: 参数键名（用于错误消息）

        Returns:
            (验证后的页码, 错误响应)
        """
        if page is None:
            page = PaginationValidator.DEFAULT_PAGE

        if not isinstance(page, int) or page < PaginationValidator.MIN_PAGE:
            return PaginationValidator.DEFAULT_PAGE, invalid_parameter_response(
                page_key,
                details=f'页码必须大于等于 {PaginationValidator.MIN_PAGE}'
            )

        if page > PaginationValidator.MAX_PAGE:
            return PaginationValidator.MAX_PAGE, None

        return page, None

    @staticmethod
    def validate_per_page(
        per_page: Optional[int] = None,
        per_page_key: str = 'per_page'
    ) -> Tuple[int, Optional[dict]]:
        """
        验证每页数量参数

        Args:
            per_page: 每页数量
            per_page_key: 参数键名（用于错误消息）

        Returns:
            (验证后的每页数量, 错误响应)
        """
        if per_page is None:
            per_page = PaginationValidator.DEFAULT_PER_PAGE

        if not isinstance(per_page, int) or per_page < PaginationValidator.MIN_PER_PAGE:
            return PaginationValidator.DEFAULT_PER_PAGE, invalid_parameter_response(
                per_page_key,
                details=f'每页数量必须大于等于 {PaginationValidator.MIN_PER_PAGE}'
            )

        if per_page > PaginationValidator.MAX_PER_PAGE:
            return PaginationValidator.MAX_PER_PAGE, None

        return per_page, None


# ==================== 日期验证 ====================

class DateValidator:
    """日期参数验证器"""

    SUPPORTED_FORMATS = ['%Y-%m-%d', '%Y/%m/%d', '%Y%m%d']
    MIN_DATE = datetime(2020, 1, 1)  # 合理的最小日期
    MAX_DATE = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

    @staticmethod
    def validate_date_string(
        date_str: str,
        date_key: str = 'date'
    ) -> Tuple[Optional[str], Optional[dict]]:
        """
        验证日期字符串

        Args:
            date_str: 日期字符串
            date_key: 参数键名（用于错误消息）

        Returns:
            (验证后的日期字符串, 错误响应)
        """
        if not date_str or not date_str.strip():
            return None, None

        date_str = date_str.strip()

        # 尝试解析日期
        parsed_date = None
        for fmt in DateValidator.SUPPORTED_FORMATS:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue

        if parsed_date is None:
            return None, invalid_parameter_response(
                date_key,
                details=f'无效的日期格式，支持的格式: YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD'
            )

        # 检查日期范围
        if parsed_date < DateValidator.MIN_DATE:
            return None, invalid_parameter_response(
                date_key,
                details=f'日期不能早于 {DateValidator.MIN_DATE.strftime("%Y-%m-%d")}'
            )

        if parsed_date > DateValidator.MAX_DATE:
            return None, invalid_parameter_response(
                date_key,
                details=f'日期不能晚于当前时间'
            )

        # 返回标准格式 YYYY-MM-DD
        return parsed_date.strftime('%Y-%m-%d'), None

    @staticmethod
    def validate_date_range(
        date_start_str: Optional[str] = None,
        date_end_str: Optional[str] = None,
        start_key: str = 'date_start',
        end_key: str = 'date_end'
    ) -> Tuple[Optional[str], Optional[str], Optional[dict]]:
        """
        验证日期范围

        Args:
            date_start_str: 开始日期字符串
            date_end_str: 结束日期字符串
            start_key: 开始日期参数键名
            end_key: 结束日期参数键名

        Returns:
            (验证后的开始日期, 验证后的结束日期, 错误响应)
        """
        # 验证开始日期
        date_start, error = DateValidator.validate_date_string(date_start_str, start_key)
        if error:
            return None, None, error

        # 验证结束日期
        date_end, error = DateValidator.validate_date_string(date_end_str, end_key)
        if error:
            return None, None, error

        # 检查日期范围逻辑
        if date_start and date_end:
            start_dt = datetime.strptime(date_start, '%Y-%m-%d')
            end_dt = datetime.strptime(date_end, '%Y-%m-%d')

            if start_dt > end_dt:
                return None, None, invalid_parameter_response(
                    start_key,
                    details='开始日期不能晚于结束日期'
                )

        return date_start, date_end, None


# ==================== 字符串验证 ====================

class StringValidator:
    """字符串参数验证器"""

    @staticmethod
    def validate_search_term(
        search_term: str,
        min_length: int = 2,
        max_length: int = 200,
        key: str = 'search'
    ) -> Tuple[str, Optional[dict]]:
        """
        验证搜索词

        Args:
            search_term: 搜索词
            min_length: 最小长度
            max_length: 最大长度
            key: 参数键名

        Returns:
            (验证后的搜索词, 错误响应)
        """
        if not search_term:
            return '', None

        search_term = search_term.strip()

        # 检查长度
        if len(search_term) > 0 and len(search_term) < min_length:
            return '', invalid_parameter_response(
                key,
                details=f'搜索词至少需要 {min_length} 个字符'
            )

        if len(search_term) > max_length:
            return search_term[:max_length], None  # 截断而非报错

        return search_term, None

    @staticmethod
    def validate_category(
        category: str,
        allowed_values: Optional[list] = None,
        key: str = 'category'
    ) -> Tuple[str, Optional[dict]]:
        """
        验证分类参数

        Args:
            category: 分类名称
            allowed_values: 允许的分类列表
            key: 参数键名

        Returns:
            (验证后的分类, 错误响应)
        """
        if not category:
            return '', None

        category = category.strip()

        # 检查允许值
        if allowed_values and category not in allowed_values and category != 'all':
            return '', invalid_parameter_response(
                key,
                details=f'无效的分类: {category}'
            )

        return category, None


# ==================== 数字验证 ====================

class NumberValidator:
    """数字参数验证器"""

    @staticmethod
    def validate_int(
        value: Any,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        default_value: int = 0,
        key: str = 'value'
    ) -> Tuple[int, Optional[dict]]:
        """
        验证整数参数

        Args:
            value: 待验证的值
            min_value: 最小值
            max_value: 最大值
            default_value: 默认值
            key: 参数键名

        Returns:
            (验证后的整数, 错误响应)
        """
        # 尝试转换为整数
        try:
            int_value = int(value) if value is not None else default_value
        except (ValueError, TypeError):
            return default_value, invalid_parameter_response(
                key,
                details=f'必须是有效的整数'
            )

        # 检查范围
        if min_value is not None and int_value < min_value:
            return min_value, None

        if max_value is not None and int_value > max_value:
            return max_value, None

        return int_value, None

    @staticmethod
    def validate_float(
        value: Any,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        default_value: float = 0.0,
        key: str = 'value'
    ) -> Tuple[float, Optional[dict]]:
        """
        验证浮点数参数

        Args:
            value: 待验证的值
            min_value: 最小值
            max_value: 最大值
            default_value: 默认值
            key: 参数键名

        Returns:
            (验证后的浮点数, 错误响应)
        """
        # 尝试转换为浮点数
        try:
            float_value = float(value) if value is not None else default_value
        except (ValueError, TypeError):
            return default_value, invalid_parameter_response(
                key,
                details=f'必须是有效的数字'
            )

        # 检查范围
        if min_value is not None and float_value < min_value:
            return min_value, None

        if max_value is not None and float_value > max_value:
            return max_value, None

        return float_value, None


# ==================== 请求参数提取辅助 ====================

class RequestParams:
    """请求参数提取和验证辅助类"""

    @staticmethod
    def get_pagination_params():
        """
        从请求中提取并验证分页参数

        Returns:
            (page, per_page, error_response)
        """
        page = request.args.get('page', PaginationValidator.DEFAULT_PAGE, type=int)
        per_page = request.args.get('per_page', PaginationValidator.DEFAULT_PER_PAGE, type=int)

        page, error = PaginationValidator.validate_page(page, 'page')
        if error:
            return page, per_page, error

        per_page, error = PaginationValidator.validate_per_page(per_page, 'per_page')
        if error:
            return page, per_page, error

        return page, per_page, None

    @staticmethod
    def get_date_range_params(
        start_key: str = 'date_start',
        end_key: str = 'date_end'
    ):
        """
        从请求中提取并验证日期范围参数

        Returns:
            (date_start, date_end, error_response)
        """
        date_start = request.args.get(start_key, '').strip()
        date_end = request.args.get(end_key, '').strip()

        return DateValidator.validate_date_range(
            date_start,
            date_end,
            start_key,
            end_key
        )

    @staticmethod
    def get_search_params(
        min_length: int = 2,
        max_length: int = 200
    ):
        """
        从请求中提取并验证搜索参数

        Returns:
            (search_term, error_response)
        """
        search = request.args.get('search', '').strip()
        return StringValidator.validate_search_term(
            search,
            min_length,
            max_length
        )

    @staticmethod
    def get_category_params(
        allowed_values: Optional[list] = None
    ):
        """
        从请求中提取并验证分类参数

        Returns:
            (category, error_response)
        """
        category = request.args.get('category', '').strip()
        return StringValidator.validate_category(
            category,
            allowed_values
        )
