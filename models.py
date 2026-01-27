#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据模型模块 - 定义系统核心实体及数据库结构
整合了 Resource, Category, FailedTID 及 ValidationLog 模型
"""

import logging
from datetime import datetime, timedelta, timezone
from flask_sqlalchemy import SQLAlchemy
from flask_sqlalchemy.pagination import Pagination
from sqlalchemy import text, Index
from typing import Dict, List, Any, Optional

# 初始化 SQLAlchemy 实例
db = SQLAlchemy()

logger = logging.getLogger(__name__)

class Resource(db.Model):
    """资源数据模型 - 存储从SHT网站抓取的资源信息"""
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(500), nullable=False, index=True)  # 添加标题索引用于搜索
    sub_type = db.Column(db.String(200), index=True)  # 类型标签，如[国产原创]等，添加索引
    publish_date = db.Column(db.String(20), index=True)  # 发布日期，添加索引
    magnet = db.Column(db.Text)  # 磁力链接
    preview_images = db.Column(db.Text)  # 预览图链接，逗号分隔
    size = db.Column(db.Integer)  # 大小（MB）
    tid = db.Column(db.Integer, unique=True, nullable=False, index=True)  # 原始网站的tid，添加唯一约束
    section = db.Column(db.String(100), index=True)  # 所属版块，添加索引
    detail_url = db.Column(db.String(500))  # 详情页链接
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), index=True)  # 添加创建时间索引
    
    # 添加复合索引以提高复杂查询性能
    __table_args__ = (
        db.Index('idx_section_date', 'section', 'publish_date'),  # 分类+日期复合索引
    )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        # 处理预览图：如果是字符串则分割为列表
        images = []
        if self.preview_images:
            if isinstance(self.preview_images, str):
                images = [img.strip() for img in self.preview_images.split(',') if img.strip()]
            elif isinstance(self.preview_images, list):
                images = self.preview_images

        return {
            'id': self.id,
            'title': self.title,
            'sub_type': self.sub_type,
            'publish_date': self.publish_date,
            'magnet': self.magnet,
            'preview_images': images,
            'size': self.size,
            'tid': self.tid,
            'section': self.section,
            'detail_url': self.detail_url,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

    @classmethod
    def search_resources(cls, keyword: Optional[str], page: int = 1, per_page: int = 20) -> Pagination:
        """统一的搜索接口"""
        from sqlalchemy import or_
        query = cls.query
        if keyword:
            pattern = f"%{keyword}%"
            query = query.filter(or_(
                cls.title.ilike(pattern),
                cls.sub_type.ilike(pattern),
                cls.section.ilike(pattern)
            ))
        return query.order_by(cls.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)

    @classmethod
    def cleanup_duplicates(cls) -> int:
        """清理重复的种子记录（保留ID较大的最新记录）"""
        try:
            # N+1 查询优化 使用一条 SQL 语句删除所有重复记录（保留 ID 最大的那个）
            # 子查询找出每个 TID 对应的最大 ID
            result = db.session.execute(text("""
                DELETE FROM resource
                WHERE id NOT IN (
                    SELECT MAX(id) FROM resource GROUP BY tid
                )
            """))
            total_removed = result.rowcount

            if total_removed > 0:
                db.session.commit()
                logger.info(f"清理了 {total_removed} 条重复记录")
            return total_removed
        except Exception as e:
            db.session.rollback()
            logger.error(f"清理重复记录失败: {e}")
            return 0

    @classmethod
    def get_statistics(cls) -> Dict[str, Any]:
        """获取资源统计信息（高效查询）"""
        from cache_manager import cache_manager, CacheKeys
        from datetime import datetime, timedelta
        
        # 尝试从缓存获取
        cached_stats = cache_manager.get(CacheKeys.STATS)
        if cached_stats:
            return cached_stats
            
        try:
            total_count = cls.query.count()
            
            # 统计今日新增 (UTC时间)
            today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            today_count = cls.query.filter(cls.created_at >= today_start).count()
            
            # 统计近7日新增
            week_start = today_start - timedelta(days=7)
            recent_count = cls.query.filter(cls.created_at >= week_start).count()
            
            # 按版块统计
            section_stats = db.session.query(
                cls.section, db.func.count(cls.id)
            ).group_by(cls.section).all()
            
            stats = {
                'total_count': total_count,
                'today_count': today_count,
                'recent_count': recent_count,
                'sections': {s[0] or '未知': s[1] for s in section_stats},
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            # 写入缓存（有效期10分钟）
            cache_manager.set(CacheKeys.STATS, stats, ttl=600)
            return stats
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {'total_count': 0, 'today_count': 0, 'recent_count': 0, 'sections': {}, 'error': str(e)}

class Category(db.Model):
    """
    分类信息模型 - 管理论坛版块、抓取状态及统计信息
    取代了旧有的 forum_info.json
    """
    __tablename__ = 'category'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False, index=True)
    fid = db.Column(db.Integer, unique=True, nullable=False)  # 论坛版块ID
    description = db.Column(db.String(500))
    is_active = db.Column(db.Boolean, default=True)
    resource_count = db.Column(db.Integer, default=0)
    total_topics = db.Column(db.Integer, default=0) # 远程板块总主题数
    total_pages = db.Column(db.Integer, default=0)  # 远程板块总页数
    last_updated = db.Column(db.DateTime)
    display_order = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'name': self.name,
            'fid': self.fid,
            'resource_count': self.resource_count,
            'total_topics': self.total_topics,
            'total_pages': self.total_pages,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None,
            'is_active': self.is_active
        }

    @classmethod
    def get_all_active(cls) -> List['Category']:
        """获取所有激活的分类"""
        return cls.query.filter_by(is_active=True).order_by(cls.display_order).all()

    @classmethod
    def get_all_categories(cls) -> List['Category']:
        """获取所有已记录的板块"""
        return cls.query.order_by(cls.display_order.asc(), cls.id.asc()).all()

    @classmethod
    def get_cache_info(cls) -> Dict[str, Any]:
        """获取本地统计信息摘要 - 供 API 兼容使用"""
        try:
            total_forums = cls.query.count()
            active_forums = cls.query.filter_by(is_active=True).count()
            latest = cls.query.order_by(cls.last_updated.desc()).first()
            return {
                'total': total_forums,
                'active': active_forums,
                'last_updated': latest.last_updated.isoformat() if latest and latest.last_updated else None
            }
        except:
            return {'total': 0, 'active': 0, 'last_updated': None}

    @classmethod
    def update_forum_info(cls, forums_info: Dict[str, Dict]) -> bool:
        """根据爬虫获取的信息深度同步本地板块数据"""
        try:
            # N+1 查询优化 一次性查询所有板块，避免循环中重复查询
            all_fids = [int(fid) for fid in forums_info.keys()]
            existing_categories = cls.query.filter(cls.fid.in_(all_fids)).all()
            cat_map = {cat.fid: cat for cat in existing_categories}

            new_categories = []
            new_fids = set()

            for fid, info in forums_info.items():
                fid_int = int(fid)
                cat = cat_map.get(fid_int)

                if not cat:
                    # 防止重复与并发插入导致唯一键冲突
                    cat = cls.query.filter_by(fid=fid_int).first()
                    if cat:
                        cat_map[fid_int] = cat
                    else:
                        if fid_int in new_fids:
                            continue
                        # 新板块，暂存到列表，批量添加
                        cat = cls(fid=fid_int, name=info.get('name', f"板块{fid_int}"))
                        new_categories.append(cat)
                        new_fids.add(fid_int)
                        # 新板块没有 description，使用空字符串
                        cat.description = info.get('description', '')
                else:
                    # 智能对齐字段
                    cat.description = info.get('description', cat.description or '')

                # 兼容不同来源的统计字段名，保留None值（表示数据未获取）
                topics_raw = info.get('total_topics') if info.get('total_topics') is not None else info.get('topics')
                pages_raw = info.get('total_pages') if info.get('total_pages') is not None else info.get('pages')

                # 只有非None值才转换为int，否则保留已有值
                if topics_raw is not None:
                    cat.total_topics = int(topics_raw)
                if pages_raw is not None:
                    cat.total_pages = int(pages_raw)
                cat.last_updated = datetime.now(timezone.utc)

            # 批量添加新板块
            if new_categories:
                db.session.bulk_save_objects(new_categories)
                logger.info(f"批量添加了 {len(new_categories)} 个新板块")

            db.session.commit()
            logger.info(f"成功同步了 {len(forums_info)} 个板块的元数据")

            # 重建分类API的缓存（而不是清除），确保数据立即可用
            try:
                from cache_manager import cache_manager, CacheKeys
                from sqlalchemy import func

                # 重新生成缓存数据
                existing_categories = db.session.query(
                    Resource.section,
                    func.count(Resource.id).label('count')
                ).filter(
                    Resource.section.isnot(None),
                    Resource.section != ''
                ).group_by(Resource.section).all()

                defined_categories = cls.query.all()
                defined_cat_dict = {cat.name: cat for cat in defined_categories}

                categories_list = []
                for section_name, count in existing_categories:
                    cat_info = {
                        'name': section_name,
                        'count': count,
                        'defined': section_name in defined_cat_dict
                    }
                    if section_name in defined_cat_dict:
                        cat_obj = defined_cat_dict[section_name]
                        cat_info['total_topics'] = cat_obj.total_topics or 0
                        cat_info['total_pages'] = cat_obj.total_pages or 0
                        cat_info['fid'] = cat_obj.fid
                    categories_list.append(cat_info)

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

                categories_list.sort(key=lambda x: x['name'])

                # 重建缓存（72小时）
                cache_manager.set(CacheKeys.CATEGORIES, categories_list, ttl=259200)
                logger.info("已重建分类API缓存，有效期72小时")
            except Exception as cache_err:
                logger.warning(f"重建缓存失败: {cache_err}")

            return True
        except Exception as e:
            db.session.rollback()
            logger.error(f"批量更新板块信息异常: {e}")
            return False

    @classmethod
    def update_counts(cls) -> bool:
        """从 Resource 表同步更新各分类的资源数量统计"""
        try:
            # 获取 Resource 表中的按版块分组统计
            counts = db.session.query(
                Resource.section, db.func.count(Resource.id)
            ).group_by(Resource.section).all()
            
            count_map = {name: count for name, count in counts}
            
            # 更新 Category 表中对应的数量
            all_categories = cls.query.all()
            for cat in all_categories:
                cat.resource_count = count_map.get(cat.name, 0)
            
            db.session.commit()
            logger.info("同步分类资源统计完成")
            return True
        except Exception as e:
            db.session.rollback()
            logger.error(f"更新分类统计失败: {e}")
            return False

    def needs_crawl(self, hours: int = 24) -> bool:
        """判断该分类是否需要重新爬取（基于时间间隔）"""
        if not self.last_updated:
            return True
        return datetime.now(timezone.utc) - self.last_updated > timedelta(hours=hours)

class FailedTID(db.Model):
    """
    失败 TID 记录模型
    取代了旧有的 failed_tid_manager.py
    """
    __tablename__ = 'failed_tid'
    
    id = db.Column(db.Integer, primary_key=True)
    tid = db.Column(db.Integer, unique=True, nullable=False, index=True)
    section = db.Column(db.String(100))
    detail_url = db.Column(db.String(500))
    failure_reason = db.Column(db.String(500))
    retry_count = db.Column(db.Integer, default=0)
    status = db.Column(db.String(20), default='pending')  # 'pending', 'retrying', 'success', 'abandoned'
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'id': self.id,
            'tid': self.tid,
            'section': self.section,
            'detail_url': self.detail_url,
            'failure_reason': self.failure_reason,
            'retry_count': self.retry_count,
            'status': self.status,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

    @classmethod
    def add(cls, tid: int, section: Optional[str] = None, url: Optional[str] = None, reason: Optional[str] = None, force_activate: bool = False) -> bool:
        """记录一个失败的 TID（支持强制覆盖成功状态）"""
        existing = cls.query.filter_by(tid=tid).first()
        if existing:
            # 如果已经标记为成功，且未开启强制激活，则跳过
            if existing.status == 'success' and not force_activate:
                logger.debug(f"TID {tid} 已标记为成功，跳过更新")
                return True

            # 否则更新/强制激活信息
            existing.retry_count = 0 if force_activate else (existing.retry_count + 1)
            existing.failure_reason = reason
            existing.status = 'pending'
            if not force_activate and existing.retry_count >= 5:
                existing.status = 'abandoned'
        else:
            new_failed = cls(tid=tid, section=section, detail_url=url, failure_reason=reason)
            db.session.add(new_failed)

        try:
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            logger.error(f"保存失败TID {tid} 出错: {e}")
            return False

    @classmethod
    def get_pending_tids(cls, section: Optional[str] = None, limit: int = 100) -> List['FailedTID']:
        """获取待重试的 TID 列表"""
        query = cls.query.filter(cls.status.in_(['pending', 'retrying']))
        if section:
            query = query.filter_by(section=section)
        return query.order_by(cls.retry_count.asc(), cls.created_at.desc()).limit(limit).all()

    @classmethod
    def mark_success(cls, tid: int) -> bool:
        """记录重试成功"""
        failed = cls.query.filter_by(tid=tid).first()
        if failed:
            failed.status = 'success'
            db.session.commit()
            return True
        return False

class ValidationLog(db.Model):
    """
    验证日志模型
    记录爬虫抓取后的数据质量校验结果
    取代了 validation.py 中的原生 SQL 表
    """
    __tablename__ = 'validation_log'
    
    id = db.Column(db.Integer, primary_key=True)
    tid = db.Column(db.Integer, index=True, nullable=False)
    title = db.Column(db.String(512))
    detail_url = db.Column(db.String(1024))
    result = db.Column(db.String(20), nullable=False)  # 'passed', 'failed'
    failure_reasons = db.Column(db.Text)  # JSON 字符串存储失败原因
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), index=True)

    @classmethod
    def log(cls, tid: int, title: str, detail_url: str, result: str, reasons: Optional[list] = None) -> Optional['ValidationLog']:
        """记录一条验证日志"""
        import json
        try:
            log_entry = cls(
                tid=tid,
                title=title,
                detail_url=detail_url,
                result=result,
                failure_reasons=json.dumps(reasons) if reasons else None
            )
            db.session.add(log_entry)
            db.session.commit()
            return log_entry
        except Exception as e:
            db.session.rollback()
            logger.error(f"记录验证日志失败: {e}")
            return None

    @classmethod
    def get_recent_stats(cls, hours: int = 24):
        """获取最近 N 小时的验证统计"""
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        try:
            total = cls.query.filter(cls.created_at >= since).count()
            passed = cls.query.filter(cls.created_at >= since, cls.result == 'passed').count()
            failed = total - passed
            
            return {
                'total': total,
                'passed': passed,
                'failed': failed,
                'success_rate': round(passed / max(total, 1) * 100, 2)
            }
        except Exception as e:
            logger.error(f"获取验证统计失败: {e}")
            return {'total': 0, 'passed': 0, 'failed': 0, 'success_rate': 0}