#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一服务层 - 封装 Web 和 Bot 共同的业务逻辑
- 资源查询服务
- 分类查询服务
- 统计查询服务
"""

import logging
from datetime import timezone
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy import func, or_, and_

from models import db, Resource, Category
from utils.validators import PaginationValidator, DateValidator, StringValidator, RequestParams

logger = logging.getLogger(__name__)


# ==================== 资源服务 ====================

class ResourceService:
    """资源查询服务"""

    @staticmethod
    def search_resources(
        keyword: Optional[str] = None,
        page: int = 1,
        per_page: int = 20
    ) -> Dict[str, Any]:
        """
        搜索资源 (从模型层迁移)

        Args:
            keyword: 搜索关键词
            page: 页码
            per_page: 每页数量

        Returns:
            包含资源和分页信息的字典
        """
        from sqlalchemy import or_
        query = Resource.query

        if keyword:
            pattern = f"%{keyword}%"
            query = query.filter(or_(
                Resource.title.ilike(pattern),
                Resource.sub_type.ilike(pattern),
                Resource.section.ilike(pattern)
            ))

        # 获取总数
        total = query.count()

        # 排序和分页
        resources = query.order_by(Resource.created_at.desc()).paginate(
            page=page, per_page=per_page, error_out=False
        )

        return {
            'resources': [r.to_dict() for r in resources.items],
            'total': total,
            'pages': (total + per_page - 1) // per_page if total > 0 else 1,
            'current_page': page,
            'per_page': per_page,
            'has_next': resources.has_next,
            'has_prev': resources.has_prev
        }

    @staticmethod
    def cleanup_duplicates() -> int:
        """
        清理重复的种子记录 (从模型层迁移)

        Returns:
            删除的重复记录数
        """
        try:
            from sqlalchemy import text
            results = db.session.execute(text(
                "SELECT tid FROM resource GROUP BY tid HAVING COUNT(tid) > 1"
            )).fetchall()

            total_removed = 0
            for row in results:
                tid = row[0]
                duplicates = Resource.query.filter_by(tid=tid).order_by(Resource.id.desc()).all()
                for dup in duplicates[1:]:
                    db.session.delete(dup)
                    total_removed += 1

            db.session.commit()
            logger.info(f"清理了 {total_removed} 条重复记录")
            return total_removed
        except Exception as e:
            db.session.rollback()
            logger.error(f"清理重复记录失败: {e}")
            return 0

    @staticmethod
    def get_statistics() -> Dict[str, Any]:
        """
        获取资源统计信息 (从模型层迁移)

        Returns:
            统计数据字典
        """
        from cache_manager import cache_manager, CacheKeys
        from datetime import datetime, timedelta

        # 尝试从缓存获取
        cached_stats = cache_manager.get(CacheKeys.STATS)
        if cached_stats:
            return cached_stats

        try:
            total_count = Resource.query.count()

            # 统计今日新增 (UTC时间)
            today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            today_count = Resource.query.filter(Resource.created_at >= today_start).count()

            # 统计本周新增
            week_start = today_start - timedelta(days=today_start.weekday())
            week_count = Resource.query.filter(Resource.created_at >= week_start).count()

            stats = {
                'total': total_count,
                'today': today_count,
                'week': week_count,
                'last_updated': datetime.now(timezone.utc).isoformat()
            }

            # 缓存结果
            cache_manager.set(CacheKeys.STATS, stats, ttl=300)
            return stats

        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {
                'total': 0,
                'today': 0,
                'week': 0,
                'last_updated': datetime.now(timezone.utc).isoformat()
            }

    @staticmethod
    def get_resources_with_filters(
        page: int = 1,
        per_page: int = 20,
        category: Optional[str] = None,
        search: Optional[str] = None,
        date_start: Optional[str] = None,
        date_end: Optional[str] = None,
        order_by: str = 'created_at',
        incomplete_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取资源列表（支持多种筛选条件，含残缺数据筛选）
        """
        query = Resource.query

        # 结构化故障诊断筛选 - 支持多选叠加 (AND 逻辑)
        if incomplete_type:
            types = incomplete_type.split(',') if isinstance(incomplete_type, str) else incomplete_type
            
            # 基础故障原子定义
            is_unknown_sub = (Resource.sub_type == '未知') | (Resource.sub_type == '') | (Resource.sub_type.is_(None))
            is_unknown_date = (Resource.publish_date == '未知') | (Resource.publish_date == '') | (Resource.publish_date.is_(None))
            is_unknown_size = (Resource.size == 0) | (Resource.size.is_(None))

            # 只有当用户勾选了具体的瑕疵项时才进行 AND 叠加
            conditions = []
            if 'sub_type_missing' in types: conditions.append(is_unknown_sub)
            if 'date_missing' in types: conditions.append(is_unknown_date)
            if 'size_missing' in types: conditions.append(is_unknown_size)
            
            # 特殊全量快捷项 (OR 逻辑)
            if 'any_missing' in types:
                query = query.filter(is_unknown_sub | is_unknown_date | is_unknown_size)
            elif conditions:
                # 叠加所有选中的瑕疵条件 (AND 逻辑)
                for cond in conditions:
                    query = query.filter(cond)
            
            # 兼容旧版的快捷综合项
            if 'total_loss' in types:
                query = query.filter(is_unknown_sub & is_unknown_date & is_unknown_size)
            elif 'critical_error' in types:
                query = query.filter(is_unknown_sub & is_unknown_size)

        # 分类筛选
        if category and category != 'all':
            if category == 'unknown':
                query = query.filter(Resource.section.is_(None) | (Resource.section == ''))
            else:
                query = query.filter(Resource.section == category)

        # 关键词搜索
        if search:
            search_terms = search.strip().split()

            if len(search_terms) == 1:
                term = search_terms[0]
                term_pattern = f'%{term}%'
                query = query.filter(
                    or_(
                        Resource.title.ilike(term_pattern),
                        Resource.sub_type.ilike(term_pattern)
                    )
                )
            elif len(search_terms) > 1:
                conditions = []
                for term in search_terms:
                    if len(term) >= 2:
                        term_pattern = f'%{term}%'
                        term_condition = or_(
                            Resource.title.ilike(term_pattern),
                            Resource.sub_type.ilike(term_pattern)
                        )
                        conditions.append(term_condition)

                if conditions:
                    query = query.filter(and_(*conditions))

        # 日期筛选
        if date_start:
            query = query.filter(Resource.publish_date >= date_start)

        if date_end:
            query = query.filter(Resource.publish_date <= date_end)

        # 获取总数
        total = query.count()

        # 排序和分页
        order_field = getattr(Resource, order_by, Resource.created_at)
        resources = query.order_by(order_field.desc()).paginate(
            page=page, per_page=per_page, error_out=False
        )

        return {
            'resources': [r.to_dict() for r in resources.items],
            'total': total,
            'pages': (total + per_page - 1) // per_page if total > 0 else 1,
            'current_page': page,
            'per_page': per_page,
            'has_next': resources.has_next,
            'has_prev': resources.has_prev
        }

    @staticmethod
    def get_latest_resources_by_category(
        category_name: str,
        limit: int = 10
    ) -> List[Resource]:
        """
        获取指定分类的最新资源（Bot 使用）

        Args:
            category_name: 分类名称
            limit: 返回数量限制

        Returns:
            资源列表
        """
        resources = Resource.query.filter_by(section=category_name)\
            .order_by(Resource.created_at.desc())\
            .limit(limit)\
            .all()

        return resources

    @staticmethod
    def get_resource_by_id(resource_id: int) -> Optional[Resource]:
        """
        根据 ID 获取资源

        Args:
            resource_id: 资源 ID

        Returns:
            资源对象或 None
        """
        return Resource.query.get(resource_id)


# ==================== 分类服务 ====================

class CategoryService:
    """分类查询服务"""

    @staticmethod
    def get_all_active() -> List['Category']:
        """
        获取所有激活的分类 (从模型层迁移)

        Returns:
            激活的分类列表
        """
        return Category.query.filter_by(is_active=True).order_by(Category.display_order).all()

    @staticmethod
    def update_forum_info(forums_info: Dict[str, Dict]) -> bool:
        """
        根据爬虫获取的信息深度同步本地板块数据 (从模型层迁移)

        Args:
            forums_info: 论坛信息字典 {fid: {name, description, total_topics, total_pages}}

        Returns:
            是否更新成功
        """
        try:
            for fid, info in forums_info.items():
                cat = Category.query.filter_by(fid=int(fid)).first()
                if not cat:
                    cat = Category(fid=int(fid), name=info.get('name', f"板块{fid}"))
                    db.session.add(cat)

                cat.description = info.get('description', cat.description or '')
                topics_raw = info.get('total_topics') if info.get('total_topics') is not None else info.get('topics')
                pages_raw = info.get('total_pages') if info.get('total_pages') is not None else info.get('pages')

                cat.total_topics = topics_raw if topics_raw is not None else cat.total_topics
                cat.total_pages = pages_raw if pages_raw is not None else cat.total_pages
                cat.last_updated = datetime.now(timezone.utc)

            db.session.commit()
            logger.info(f"成功更新 {len(forums_info)} 个板块信息")
            return True
        except Exception as e:
            db.session.rollback()
            logger.error(f"更新板块信息失败: {e}")
            return False

    @staticmethod
    def update_counts() -> bool:
        """
        从 Resource 表同步更新各分类的资源数量统计 (从模型层迁移)

        Returns:
            是否更新成功
        """
        try:
            counts = db.session.query(
                Resource.section, db.func.count(Resource.id)
            ).group_by(Resource.section).all()

            count_map = {name: count for name, count in counts}

            all_categories = Category.query.all()
            for cat in all_categories:
                cat.resource_count = count_map.get(cat.name, 0)

            db.session.commit()
            logger.info(f"成功更新 {len(all_categories)} 个分类的资源计数")
            return True
        except Exception as e:
            db.session.rollback()
            logger.error(f"更新分类计数失败: {e}")
            return False

    @staticmethod
    def get_all_categories(
        include_stats: bool = True,
        include_defined: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取所有分类 (优化版本 - 使用联表查询减少数据库查询次数)

        Args:
            include_stats: 是否包含统计信息
            include_defined: 是否包含定义但无数据的分类

        Returns:
            分类列表
        """
        result = {}

        # 使用联表查询一次性获取所有分类信息
        if include_stats and include_defined:
            # 联表查询：Category表左连接Resource统计
            category_stats = db.session.query(
                Category.name,
                Category.fid,
                Category.total_topics,
                Category.total_pages,
                func.count(Resource.id).label('resource_count')
            ).outerjoin(
                Resource, Category.name == Resource.section
            ).filter(
                Resource.section.isnot(None),
                Resource.section != ''
            ).group_by(
                Category.id, Category.name, Category.fid,
                Category.total_topics, Category.total_pages
            ).all()

            for name, fid, total_topics, total_pages, count in category_stats:
                result[name] = {
                    'name': name,
                    'count': count,
                    'defined': True,
                    'total_topics': total_topics or 0,
                    'total_pages': total_pages or 0,
                    'fid': fid
                }

            # 添加定义但无数据的分类
            defined_without_data = db.session.query(
                Category.name, Category.fid,
                Category.total_topics, Category.total_pages
            ).outerjoin(
                Resource, Category.name == Resource.section
            ).filter(
                Resource.id.is_(None)
            ).all()

            for name, fid, total_topics, total_pages in defined_without_data:
                if name not in result:
                    result[name] = {
                        'name': name,
                        'count': 0,
                        'defined': True,
                        'total_topics': total_topics or 0,
                        'total_pages': total_pages or 0,
                        'fid': fid
                    }
        elif include_stats:
            # 仅获取数据库中实际存在的分类
            existing_categories = db.session.query(
                Resource.section,
                func.count(Resource.id).label('count')
            ).filter(
                Resource.section.isnot(None),
                Resource.section != ''
            ).group_by(Resource.section).all()

            for section_name, count in existing_categories:
                result[section_name] = {
                    'name': section_name,
                    'count': count,
                    'defined': False
                }
        elif include_defined:
            # 仅获取 Category 表中定义的分类
            defined_categories = Category.query.all()
            for cat in defined_categories:
                result[cat.name] = {
                    'name': cat.name,
                    'count': 0,
                    'defined': True,
                    'total_topics': cat.total_topics or 0,
                    'total_pages': cat.total_pages or 0,
                    'fid': cat.fid
                }

        # 转换为列表并排序
        return list(sorted(result.values(), key=lambda x: x['name']))

    @staticmethod
    def get_category_by_fid(fid: str) -> Optional[Category]:
        """
        根据 FID 获取分类

        Args:
            fid: 论坛 ID

        Returns:
            分类对象或 None
        """
        return Category.query.filter_by(fid=fid).first()

    @staticmethod
    def get_category_by_name(name: str) -> Optional[Category]:
        """
        根据名称获取分类

        Args:
            name: 分类名称

        Returns:
            分类对象或 None
        """
        return Category.query.filter_by(name=name).first()


# ==================== 统计服务 ====================

class StatisticsService:
    """统计查询服务"""

    @staticmethod
    def get_resource_statistics() -> Dict[str, Any]:
        """
        获取资源统计信息

        Returns:
            统计数据字典
        """
        total_count = Resource.query.count()
        today_count = Resource.query.filter(
            Resource.publish_date >= func.date(func.now())
        ).count()

        # 按分类统计
        category_stats = db.session.query(
            Resource.section,
            func.count(Resource.id).label('count')
        ).filter(
            Resource.section.isnot(None),
            Resource.section != ''
        ).group_by(Resource.section).order_by(
            func.count(Resource.id).desc()
        ).limit(10).all()

        categories = [
            {'section': s or '未知', 'count': c}
            for s, c in category_stats
        ]

        return {
            'total': total_count,
            'today': today_count,
            'categories': categories
        }

    @staticmethod
    def get_database_info() -> Dict[str, Any]:
        """
        获取数据库信息

        Returns:
            数据库统计信息
        """
        resource_count = Resource.query.count()
        category_count = Category.query.count()

        return {
            'resources': resource_count,
            'categories': category_count
        }


# ==================== 统一服务入口 ====================

class UnifiedService:
    """统一服务入口 - 整合所有服务"""

    resource_service = ResourceService
    category_service = CategoryService
    statistics_service = StatisticsService
