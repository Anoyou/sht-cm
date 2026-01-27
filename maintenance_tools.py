#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一维护工具 - 整合数据库维护、失败TID清理和重试功能

合并自：
- database_maintenance.py (270行)
- cleanup_failed_tids.py (273行)
- retry_failed_tids.py (328行)

使用方法：
    python maintenance_tools.py db-info              # 显示数据库信息
    python maintenance_tools.py db-cleanup           # 清理重复数据
    python maintenance_tools.py db-optimize          # 优化数据库
    python maintenance_tools.py failed-analyze       # 分析失败TID
    python maintenance_tools.py failed-cleanup       # 清理失败TID
    python maintenance_tools.py failed-retry         # 重试失败TID
    python maintenance_tools.py full-maintenance     # 完整维护流程
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from sqlalchemy import text, func

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import get_flask_app, db_session_context, get_database_paths, setup_logging
from models import db, Resource, Category, FailedTID
# 延迟导入 crawler 以避免循环依赖
# from crawler import SHT
from cache_manager import cache_manager

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)


# ==================== 数据库维护类 ====================

class DatabaseMaintenance:
    """数据库维护工具类"""
    
    def __init__(self):
        self.app = get_flask_app()
        self.stats = {
            'cleaned_duplicates': 0,
            'normalized_dates': 0,
            'optimized_indexes': 0,
            'cleaned_orphans': 0
        }
    
    def run_full_maintenance(self):
        """运行完整的数据库维护"""
        logger.info("开始数据库维护...")

        with self.app.app_context():
            try:
                # 1. 清理重复数据
                self.clean_duplicates()

                # 2. 标准化日期格式
                self.normalize_dates()

                # 3. 清理孤立数据
                self.clean_orphaned_data()

                # 4. 清理WAL/SHM文件
                self.cleanup_wal_shm()

                # 5. 优化数据库
                self.optimize_database()

                # 6. 更新统计信息
                self.update_statistics()

                # 7. 清理缓存
                self.clear_cache()

                # 8. 清理旧数据（保留指定天数内的记录）
                days = 30  # 默认保留30天
                self.cleanup_old_records(days)

                logger.info("数据库维护完成")
                self.print_maintenance_report()

            except Exception as e:
                logger.error(f"数据库维护失败: {e}")
                db.session.rollback()
                raise
    
    def clean_duplicates(self):
        """清理重复数据"""
        logger.info("清理重复数据...")
        
        # 使用Resource模型的清理方法
        removed_count = Resource.cleanup_duplicates()
        self.stats['cleaned_duplicates'] = removed_count
        
        logger.info(f"清理了 {removed_count} 条重复记录")
    
    def normalize_dates(self):
        """标准化日期格式"""
        logger.info("标准化日期格式...")
        
        # 查找需要标准化的日期
        resources_with_invalid_dates = Resource.query.filter(
            Resource.publish_date.isnot(None),
            Resource.publish_date != '',
            ~Resource.publish_date.like('____-__-__')
        ).all()
        
        normalized_count = 0
        for resource in resources_with_invalid_dates:
            old_date = resource.publish_date
            new_date = self._normalize_date_string(old_date)
            
            if new_date != old_date:
                resource.publish_date = new_date
                normalized_count += 1
                logger.debug(f"日期标准化: '{old_date}' -> '{new_date}'")
        
        if normalized_count > 0:
            db.session.commit()
        
        self.stats['normalized_dates'] = normalized_count
        logger.info(f"标准化了 {normalized_count} 条日期记录")
    
    def clean_orphaned_data(self):
        """清理孤立数据"""
        logger.info("清理孤立数据...")
        
        # 清理空标题或空磁力链接的记录
        orphaned_resources = Resource.query.filter(
            (Resource.title.is_(None)) | 
            (Resource.title == '') |
            (Resource.magnet.is_(None)) |
            (Resource.magnet == '')
        ).all()
        
        cleaned_count = 0
        for resource in orphaned_resources:
            db.session.delete(resource)
            cleaned_count += 1
            logger.debug(f"删除孤立记录: ID={resource.id}")
        
        if cleaned_count > 0:
            db.session.commit()
        
        self.stats['cleaned_orphans'] = cleaned_count
        logger.info(f"清理了 {cleaned_count} 条孤立记录")
    
    def optimize_database(self):
        """优化数据库 - 独占模式修复版"""
        logger.info("优化数据库 (强制独占模式)...")

        try:
            # 1. 重要：强制切断所有当前活跃的 Session 数据库引用
            # 这能释放可能导致锁定的挂起连接
            db.session.remove()
            db.session.close_all()
            
            # 2. 直接在原始连接上执行 (规避所有 SQLAlchemy 事务干扰)
            # 使用 raw_connection 绕过所有 ORM 层
            import sqlite3
            raw_conn = db.engine.raw_connection()
            try:
                # 设置超时时间更长一些
                raw_conn.isolation_level = None  # 激活 Autocommit
                cursor = raw_conn.cursor()
                
                logger.debug("正在执行 VACUUM (此操作可能耗时几秒)...")
                cursor.execute('VACUUM')
                cursor.execute('ANALYZE')
                cursor.execute('PRAGMA optimize')
                cursor.close()
                logger.info("✅ VACUUM 和物理优化完成")
                self.stats['optimized_indexes'] = 1
            finally:
                raw_conn.close()

            logger.info("数据库优化成功")

        except Exception as e:
            # 特殊处理 busy 错误
            if "locked" in str(e).lower():
                logger.warning("🕒 数据库目前较忙，正在锁定中。跳过 VACUUM 以保持系统可用性。")
            else:
                logger.error(f"数据库优化失败: {e}")

        except Exception as e:
            logger.error(f"数据库优化失败: {e}")

    def cleanup_wal_shm(self):
        """清理SQLite的WAL和SHM文件"""
        logger.info("清理WAL/SHM文件...")

        try:
            # 获取数据库路径
            db_path = self.app.config['SQLALCHEMY_DATABASE_URI'].replace('sqlite:///', '')

            # WAL和SHM文件路径
            wal_path = f"{db_path}-wal"
            shm_path = f"{db_path}-shm"

            total_freed = 0

            # 清理WAL文件
            if os.path.exists(wal_path):
                wal_size = os.path.getsize(wal_path)
                try:
                    os.remove(wal_path)
                    total_freed += wal_size
                    logger.info(f"已删除WAL文件: {wal_path} ({self._format_size(wal_size)})")
                except Exception as e:
                    logger.error(f"删除WAL文件失败: {e}")

            # 清理SHM文件
            if os.path.exists(shm_path):
                shm_size = os.path.getsize(shm_path)
                try:
                    os.remove(shm_path)
                    total_freed += shm_size
                    logger.info(f"已删除SHM文件: {shm_path} ({self._format_size(shm_size)})")
                except Exception as e:
                    logger.error(f"删除SHM文件失败: {e}")

            if total_freed > 0:
                logger.info(f"总计释放空间: {self._format_size(total_freed)}")
            else:
                logger.info("没有需要清理的WAL/SHM文件")

            return total_freed

        except Exception as e:
            logger.error(f"清理WAL/SHM文件失败: {e}")
            return 0

    def _format_size(self, size_bytes: int) -> str:
        """
        格式化文件大小

        Args:
            size_bytes: 字节数

        Returns:
            str: 格式化后的大小字符串
        """
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"
    
    def cleanup_old_records(self, days: int = 30):
        """清理旧数据记录，保留指定天数内的数据"""
        logger.info(f"开始清理超过 {days} 天的旧数据...")
        
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
            
            # 清理旧的资源记录
            deleted_count = Resource.query.filter(
                Resource.created_at < cutoff_date
            ).delete(synchronize_session=False)
            logger.info(f"✅ 清理了 {deleted_count} 条超过 {days} 天的资源记录")
            
            # 清理旧的失败TID记录
            cutoff_date_for_failed = datetime.now(timezone.utc) - timedelta(days=days*2)
            
            deleted_failed_count = FailedTID.query.filter(
                FailedTID.created_at < cutoff_date_for_failed
            ).delete(synchronize_session=False)
            
            # 提交删除操作
            db.session.commit()
            
            logger.info(f"✅ 清理了 {deleted_failed_count} 条超过 {days*2} 天的失败TID记录")
            
            return {
                'deleted_resources': deleted_count,
                'deleted_failed_tids': deleted_failed_count
            }
        
        except Exception as e:
            logger.error(f"清理旧数据失败: {e}")
            return None
    
    def update_statistics(self):
        """更新统计信息"""
        logger.info("更新统计信息...")
        
        # 强制重新计算统计信息
        stats = Resource.get_statistics()
        logger.info(f"当前统计: 总计 {stats['total_count']} 条记录")
    
    def clear_cache(self):
        """清理缓存"""
        logger.info("清理缓存...")
        cache_manager.clear()
        logger.info("缓存已清理")
    
    def _normalize_date_string(self, date_str):
        """标准化日期字符串"""
        if not date_str:
            return None
        
        # 如果已经是标准格式，直接返回
        if len(date_str) == 10 and date_str.count('-') == 2:
            try:
                datetime.strptime(date_str, '%Y-%m-%d')
                return date_str
            except ValueError:
                pass
        
        # 尝试解析各种日期格式
        date_formats = [
            '%Y年%m月%d日',
            '%Y/%m/%d',
            '%Y.%m.%d',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
        ]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # 如果无法解析，返回原始值的前10个字符
        return date_str[:10] if len(date_str) >= 10 else date_str
    
    def print_maintenance_report(self):
        """打印维护报告"""
        print("\n" + "="*50)
        print("数据库维护报告")
        print("="*50)
        print(f"清理重复记录: {self.stats['cleaned_duplicates']} 条")
        print(f"标准化日期: {self.stats['normalized_dates']} 条")
        print(f"清理孤立数据: {self.stats['cleaned_orphans']} 条")
        print(f"数据库优化: {'完成' if self.stats['optimized_indexes'] else '跳过'}")
        print("="*50)
    
    def get_database_info(self):
        """获取数据库信息"""
        with self.app.app_context():
            try:
                # 获取数据库文件大小
                db_path = self.app.config['SQLALCHEMY_DATABASE_URI'].replace('sqlite:///', '')
                db_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
                
                # 获取表信息
                total_resources = Resource.query.count()
                total_categories = Category.query.count()
                
                # 获取最新和最旧的记录
                latest_resource = Resource.query.order_by(Resource.created_at.desc()).first()
                oldest_resource = Resource.query.order_by(Resource.created_at.asc()).first()
                
                return {
                    'database_size_mb': round(db_size / (1024 * 1024), 2),
                    'total_resources': total_resources,
                    'total_categories': total_categories,
                    'latest_record': latest_resource.created_at if latest_resource else None,
                    'oldest_record': oldest_resource.created_at if oldest_resource else None,
                    'cache_stats': cache_manager.get_stats()
                }
                
            except Exception as e:
                logger.error(f"获取数据库信息失败: {e}")
                return {}


# ==================== 失败TID清理类 ====================

class FailedTidCleaner:
    """失败TID清理工具类"""
    
    def __init__(self):
        self.main_db_path, self.failed_db_path = get_database_paths()
    
    def analyze_failed_tids(self):
        """分析失败TID的情况"""
        logger.info("🔍 分析失败TID情况")
        
        try:
            with db_session_context():
                # 总数统计
                total_count = FailedTID.query.filter(FailedTID.status.in_(['pending', 'retrying'])).count()
                
                # 按板块分布
                section_stats = db.session.query(
                    FailedTID.section, func.count(FailedTID.id)
                ).filter(FailedTID.status.in_(['pending', 'retrying'])).group_by(FailedTID.section).all()
                section_stats = dict(section_stats)
                
                # 按失败原因分布
                reason_stats = db.session.query(
                    FailedTID.failure_reason, func.count(FailedTID.id)
                ).filter(FailedTID.status.in_(['pending', 'retrying'])).group_by(FailedTID.failure_reason).all()
                reason_stats = dict(reason_stats)
            
            logger.info(f"📊 失败TID统计: 总数: {total_count}")
            return {'total_count': total_count, 'by_section': section_stats, 'by_reason': reason_stats}
        except Exception as e:
            logger.error(f"分析失败: {e}")
            return None
    
    def cleanup_existing_tids(self):
        """清理失败列表中已存在于本地数据库的TID"""
        logger.info("🧹 开始清理失败TID列表中的重复项")
        
        try:
            # 检查数据库文件是否存在
            if not os.path.exists(self.main_db_path):
                logger.error(f"❌ 主数据库不存在: {self.main_db_path}")
                return None
                
            if not os.path.exists(self.failed_db_path):
                logger.error(f"❌ 失败TID数据库不存在: {self.failed_db_path}")
                return None
            
            # 获取所有失败的TID
            failed_tids = self._get_failed_tids()
            
            if not failed_tids:
                logger.info("✅ 没有失败的TID需要清理")
                return {'total_checked': 0, 'already_exists': 0, 'cleaned_up': 0, 'errors': 0}
            
            logger.info(f"📋 检查 {len(failed_tids)} 个失败的TID")
            
            cleanup_stats = {
                'total_checked': len(failed_tids),
                'already_exists': 0,
                'cleaned_up': 0,
                'errors': 0
            }
            
            for item in failed_tids:
                tid = item['tid']
                
                try:
                    # 检查本地数据库是否已存在
                    if self._check_tid_exists(tid):
                        # 本地已存在，从失败列表中移除
                        success = self._mark_tid_success(tid)
                        
                        if success:
                            cleanup_stats['cleaned_up'] += 1
                            title = self._get_resource_title(tid)
                            logger.info(f"✅ 清理TID {tid}: 本地已存在 '{title[:50]}...'")
                        else:
                            cleanup_stats['errors'] += 1
                            logger.warning(f"⚠️ 清理TID {tid} 失败")
                        
                        cleanup_stats['already_exists'] += 1
                    
                except Exception as e:
                    cleanup_stats['errors'] += 1
                    logger.error(f"❌ 检查TID {tid} 时出错: {e}")
            
            # 输出清理结果
            logger.info("🎉 清理完成!")
            logger.info(f"   总检查数: {cleanup_stats['total_checked']}")
            logger.info(f"   本地已存在: {cleanup_stats['already_exists']}")
            logger.info(f"   成功清理: {cleanup_stats['cleaned_up']}")
            logger.info(f"   错误数: {cleanup_stats['errors']}")
            
            if cleanup_stats['cleaned_up'] > 0:
                logger.info(f"💡 已从失败列表中移除 {cleanup_stats['cleaned_up']} 个重复的TID")
            
            return cleanup_stats
            
        except Exception as e:
            logger.error(f"清理过程出错: {e}")
            return None
    
    def _get_failed_tids(self) -> List[Dict]:
        """获取所有失败的TID"""
        try:
            with db_session_context():
                records = FailedTID.get_pending_tids(limit=1000)
                return [{
                    'tid': r.tid,
                    'section': r.section,
                    'detail_url': r.detail_url,
                    'failure_reason': r.failure_reason,
                    'retry_count': r.retry_count
                } for r in records]
        except Exception as e:
            logger.error(f"获取失败TID列表失败: {e}")
            return []
    
    def _check_tid_exists(self, tid: int) -> bool:
        """检查TID是否存在于主数据库中"""
        try:
            with db_session_context():
                return Resource.query.filter_by(tid=tid).first() is not None
        except Exception as e:
            logger.error(f"检查TID {tid} 时出错: {e}")
            return False
    
    def _mark_tid_success(self, tid: int) -> bool:
        """在失败数据库中标记TID为成功"""
        return FailedTID.mark_success(tid)
    
    def _get_resource_title(self, tid: int) -> str:
        """获取资源标题"""
        try:
            with db_session_context():
                res = Resource.query.filter_by(tid=tid).first()
                return res.title if res else "未知标题"
        except Exception as e:
            logger.debug(f"获取TID {tid} 标题时出错: {e}")
            return "未知标题"


# ==================== 失败TID重试类 ====================

class FailedTidRetryService:
    """失败TID重试服务"""
    
    def __init__(self):
        self.sht = None
        self.app = None
    
    def init_crawler(self):
        """初始化爬虫"""
        try:
            # 延迟导入以避免循环依赖和 certifi 问题
            # 使用模块级导入以打破 crawler/__init__.py 触发的循环依赖
            from crawler.sync_crawler import SHT
            self.sht = SHT()
            self.app = get_flask_app()
            logger.info("爬虫初始化成功")
            return True
        except Exception as e:
            logger.error(f"爬虫初始化失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return False
    
    def retry_failed_tids(self, section: str = None, limit: int = 50,
                         batch_size: int = 10, max_retries: int = 3, 
                         continuous: bool = False, max_rounds: int = 20) -> Dict:
        """重试失败的TID
        
        Args:
            section: 板块名称（可选）
            limit: 每轮获取的最大TID数量
            batch_size: 每批处理的TID数量
            max_retries: 单个TID的最大重试次数
            continuous: 是否循环重试直到队列清空（默认False，只处理一轮）
            max_rounds: 循环模式下的最大轮数（防止无限循环）
        
        Returns:
            Dict: 重试结果统计
        """
        if not self.init_crawler():
            return {'success': False, 'error': '爬虫初始化失败'}

        # 总体统计（跨轮次）
        total_stats = {
            'success': True,
            'total_retry': 0,
            'success_count': 0,
            'failed_count': 0,
            'skipped_count': 0,
            'rounds': 0
        }

        round_num = 0
        while True:
            round_num += 1
            
            # 获取待重试的TID
            failed_entries = FailedTID.get_pending_tids(section=section, limit=limit)
            
            if not failed_entries:
                if round_num == 1:
                    logger.info("没有需要重试的TID")
                    return {
                        'success': True,
                        'total_retry': 0,
                        'success_count': 0,
                        'failed_count': 0,
                        'skipped_count': 0,
                        'rounds': 0,
                        'message': '没有需要重试的TID'
                    }
                else:
                    logger.info(f"✅ 第 {round_num} 轮：队列已清空")
                    break

            logger.info(f"{'='*50}")
            logger.info(f"🔄 第 {round_num} 轮重试开始，本轮处理 {len(failed_entries)} 个TID")
            logger.info(f"{'='*50}")

            # 发送重试开始通知（仅第一轮）
            if round_num == 1:
                try:
                    from scheduler.notifier import _send_telegram_message
                    section_desc = f"板块: {section}" if section else "所有板块"
                    mode_desc = f"循环模式（最多{max_rounds}轮）" if continuous else "单轮模式"
                    notify_msg = f"""🔄 *开始重试失败的TID*
━━━━━━━━━━━━━━
📋 {section_desc}
🔢 本轮数量: {len(failed_entries)} 个
📦 批次大小: {batch_size} 个/批
⚙️ 最大重试次数: {max_retries} 次
🔁 模式: {mode_desc}"""
                    _send_telegram_message(notify_msg, parse_mode='Markdown')
                except Exception as e:
                    logger.debug(f"发送重试开始通知失败: {e}")

            # 本轮统计
            round_stats = {
                'total_retry': len(failed_entries),
                'success_count': 0,
                'failed_count': 0,
                'skipped_count': 0
            }

            # 更新状态协调器进度
            try:
                from crawler_control.cc_control_bridge import get_crawler_control_bridge
                bridge = get_crawler_control_bridge()
                # 标记为运行中，并初始化进度
                bridge.start_crawling()
                bridge.update_progress({
                    'current_section': f'重试失败TID (第 {round_num} 轮)',
                    'total_saved': total_stats['success_count'],
                    'total_skipped': total_stats['skipped_count'],
                    'processed_pages': round_num,
                    'max_pages': max_rounds if continuous else 1,
                    'message': f'正在重试第 {round_num} 轮，本轮 {len(failed_entries)} 个'
                })
            except Exception as bridge_err:
                logger.debug(f"更新状态协调器失败: {bridge_err}")

            # 标记为重试中状态
            for f in failed_entries:
                f.status = 'retrying'
            db.session.commit()

            # 分批重试
            for i in range(0, len(failed_entries), batch_size):
                # 检查停止/暂停信号
                try:
                    from crawler_control.cc_control_bridge import get_crawler_control_bridge
                    bridge = get_crawler_control_bridge()
                    if bridge.check_stop_and_pause():
                        logger.info("🛑 收到停止信号，终止重试任务")
                        break
                except Exception as bridge_err:
                    logger.debug(f"检查控制信号失败: {bridge_err}")

                batch = failed_entries[i:i + batch_size]
                logger.info(f"处理批次 {i//batch_size + 1}/{(len(failed_entries) + batch_size - 1)//batch_size}")
                
                # 更新分批详细进度
                try:
                    bridge.update_progress({
                        'current_section_processed': i,
                        'current_section_pages': len(failed_entries),
                        'message': f'重试中: 轮次 {round_num}, 进度 {i}/{len(failed_entries)}'
                    })
                except: pass
                detail_urls = []
                for item in batch:
                    tid = item.tid
                    detail_url = item.detail_url
                    if not detail_url:
                        detail_url = f"https://sehuatang.org/forum.php?mod=viewthread&tid={tid}"
                    detail_urls.append((tid, detail_url))
                
                # 批量爬取
                try:
                    from configuration import config_manager
                    crawler_mode = config_manager.get('CRAWLER_MODE', 'async')
                    use_batch_mode = crawler_mode in ['async', 'thread']
                    batch_urls = [url for tid, url in detail_urls]

                    logger.info(f"使用 {crawler_mode} 模式重试TID (批量模式: {'是' if use_batch_mode else '否'})")
                    batch_results = self.sht.crawler_details_batch(batch_urls, use_batch_mode=use_batch_mode)
                    
                except Exception as e:
                    logger.error(f"批量爬取异常: {e}")
                    batch_results = [None] * len(detail_urls)
                
                # 处理批量结果
                for j, ((tid, detail_url), data) in enumerate(zip(detail_urls, batch_results)):
                    item = batch[j]
                    section_name = item.section or '未知板块'
                    
                    if not data or not data.get('magnet'):
                        # 重试仍然失败
                        failure_reason = "重试失败: 数据无效或缺少磁力链接"
                        
                        # 识别拦截页面
                        is_antibot = False
                        if isinstance(data, dict) and data.get('error_type') == 'antibot_detected':
                            is_antibot = True
                            failure_reason = f"触发反爬拦截: {data.get('error_msg', '未知拦截')}"

                        if item.retry_count >= max_retries:
                            logger.warning(f"❌ TID {tid} 达到最大重试次数，暂时放弃重试")
                            failure_reason += f" (已达上限)"
                            item.status = 'abandoned'
                        else:
                            item.status = 'pending'
                            if is_antibot:
                                logger.info(f"⏳ TID {tid} 被拦截，已将其标回等待队列")
                        
                        item.failure_reason = failure_reason
                        item.retry_count += 1
                        item.last_retry_time = datetime.now(timezone.utc)
                        db.session.commit()
                        
                        round_stats['failed_count'] += 1
                        logger.warning(f"❌ TID {tid} 重试失败: {failure_reason}")
                        
                    else:
                        # 重试成功，尝试保存
                        try:
                            with self.app.app_context():
                                saved = self.sht.save_to_db(data, section_name, tid, detail_url)

                                if saved:
                                    FailedTID.mark_success(tid)
                                    round_stats['success_count'] += 1
                                    logger.info(f"✅ TID {tid} 重试成功: {data.get('title', '')[:50]}...")
                                else:
                                    existing = Resource.query.filter_by(tid=tid).first()
                                    if existing:
                                        FailedTID.mark_success(tid)
                                        round_stats['skipped_count'] += 1
                                        logger.info(f"⏭️ TID {tid} 数据已存在")
                                    else:
                                        failure_reason = "重试失败: 保存失败但原因未知"
                                        FailedTID.add(tid=tid, section=section_name, url=detail_url, reason=failure_reason)
                                        round_stats['failed_count'] += 1
                                        logger.warning(f"❌ TID {tid} 保存失败")

                        except Exception as e:
                            failure_reason = f"重试失败: 保存异常 - {str(e)}"
                            if "database is locked" in str(e):
                                failure_reason = "重试失败: 数据库锁定，稍后重试"
                            FailedTID.add(tid=tid, section=section_name, url=detail_url, reason=failure_reason)
                            round_stats['failed_count'] += 1
                            logger.error(f"❌ TID {tid} 保存异常: {e}")
                
                # 批次间休息
                if i + batch_size < len(failed_entries):
                    time.sleep(2)
            
            # 累加到总体统计
            total_stats['total_retry'] += round_stats['total_retry']
            total_stats['success_count'] += round_stats['success_count']
            total_stats['failed_count'] += round_stats['failed_count']
            total_stats['skipped_count'] += round_stats['skipped_count']
            total_stats['rounds'] = round_num
            
            # 输出本轮结果
            logger.info(f"📊 第 {round_num} 轮完成:")
            logger.info(f"   本轮处理: {round_stats['total_retry']}")
            logger.info(f"   成功: {round_stats['success_count']}")
            logger.info(f"   失败: {round_stats['failed_count']}")
            logger.info(f"   已存在: {round_stats['skipped_count']}")
            
            # 检查是否继续
            if not continuous:
                logger.info("单轮模式，重试完成")
                break
            
            if round_num >= max_rounds:
                logger.warning(f"⚠️ 已达到最大轮数 {max_rounds}，停止重试")
                break
            
            # 轮次间休息
            logger.info("等待 5 秒后开始下一轮...")
            # 更新休息状态
            try:
                bridge.update_progress({
                    'message': '轮次间休息，5秒后继续...'
                })
            except: pass
            time.sleep(5)
        
        # 结束标记
        try:
            from crawler_control.cc_control_bridge import get_crawler_control_bridge
            bridge = get_crawler_control_bridge()
            bridge.stop_crawling()
        except: pass

        # 输出总体结果
        logger.info(f"{'='*50}")
        logger.info(f"🎉 重试全部完成!")
        logger.info(f"   总轮数: {total_stats['rounds']}")
        logger.info(f"   总处理数: {total_stats['total_retry']}")
        logger.info(f"   成功数: {total_stats['success_count']}")
        logger.info(f"   失败数: {total_stats['failed_count']}")
        logger.info(f"   已存在: {total_stats['skipped_count']}")
        logger.info(f"{'='*50}")

        success_rate = total_stats['success_count'] / total_stats['total_retry'] * 100 if total_stats['total_retry'] > 0 else 0
        logger.info(f"   成功率: {success_rate:.1f}%")

        # 发送重试完成通知
        try:
            from scheduler.notifier import _send_telegram_message
            section_desc = f"板块: {section}" if section else "所有板块"
            notify_msg = f"""✅ *重试失败TID完成！*
━━━━━━━━━━━━━━
📋 {section_desc}
📊 总重试数: {total_stats['total_retry']} 个
✅ 成功数: {total_stats['success_count']} 个
⏭️ 已存在: {total_stats['skipped_count']} 个
❌ 仍失败: {total_stats['failed_count']} 个
📈 成功率: {success_rate:.1f}%"""
            _send_telegram_message(notify_msg, parse_mode='Markdown')
        except Exception as e:
            logger.debug(f"发送重试完成通知失败: {e}")

        return {
            'success': True,
            **total_stats,
            'success_rate': success_rate
        }


# ==================== 元数据修复功能 ====================

def recycle_incomplete_resources(limit: int = 100, hours: int = 2, dry_run: bool = False):
    """
    外科手术式回收：只针对刚刚产生的残缺记录。
    dry_run 为 True 时，只列出清单不实际执行。
    """
    from models import Resource, FailedTID, db
    from datetime import datetime, timedelta, timezone
    app = get_flask_app()
    
    with app.app_context():
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        query = Resource.query.filter(
            Resource.created_at >= cutoff_time,
            (Resource.magnet != None),
            (
                (Resource.sub_type == None) | (Resource.sub_type == '') | 
                (Resource.sub_type == '未知') | (Resource.sub_type == '默认')
            )
        )
        total_matched = query.count()
        
        mode_prefix = "[试运行] " if dry_run else ""
        logger.info(f"📊 {mode_prefix}数据库扫描报告：发现 {total_matched} 条最近 {hours} 小时入库的残缺资源。")
        
        if total_matched == 0:
            return {"recycled_count": 0}
        
        targets = query.order_by(Resource.created_at.desc()).limit(limit).all()
        logger.info(f"⚙️ {mode_prefix}候选名单 (显示前 {len(targets)} 条):")
        print("\n" + "-"*80)
        print(f"{'TID':<10} | {'分类':<10} | {'大小':<10} | {'入库时间':<20} | {'标题'}")
        print("-"*80)
        
        recycled_count = 0
        for res in targets:
            local_time = res.created_at.astimezone() if res.created_at.tzinfo else res.created_at
            time_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
            size_str = f"{res.size}MB" if res.size else "0MB"
            print(f"{res.tid:<10} | {str(res.sub_type):<10} | {size_str:<10} | {time_str:<20} | {res.title[:40]}...")
            
            if not dry_run:
                try:
                    FailedTID.add(
                        tid=res.tid,
                        section=res.section,
                        url=res.detail_url or f"https://sehuatang.org/forum.php?mod=viewthread&tid={res.tid}",
                        reason=f"精准回炉(入库于 {local_time.strftime('%H:%M')})"
                    )
                    db.session.delete(res)
                    recycled_count += 1
                except Exception as e:
                    logger.error(f"❌ 撤回失败: {res.tid}, {e}")

        if not dry_run:
            db.session.commit()
            logger.info(f"🎉 任务已完成：本次成功撤回 {recycled_count} 条数据。")
        else:
            print("-"*80)
            logger.info(f"💡 以上为预览结果，数据库未做任何更改。如需正式执行，请去掉 --dry-run 参数。")
            
        return {"recycled_count": recycled_count}


# ==================== 命令行接口 ====================

def main():
    """主函数 - 统一的命令行接口"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='SHT资源聚合系统 - 统一维护工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
命令示例:
  %(prog)s db-info                  显示数据库信息
  %(prog)s db-cleanup               清理重复数据
  %(prog)s db-optimize              优化数据库
  %(prog)s db-normalize             标准化日期格式
  %(prog)s failed-analyze           分析失败TID
  %(prog)s failed-cleanup           清理失败TID
  %(prog)s failed-retry --limit 50  重试失败TID（限制50个）
  %(prog)s full-maintenance         运行完整维护流程
        """
    )
    
    parser.add_argument('command', 
                       choices=[
                           'db-info', 'db-cleanup', 'db-optimize', 'db-normalize',
                           'failed-analyze', 'failed-cleanup', 'failed-retry',
                           'full-maintenance', 'recycle-data'
                       ],
                       help='要执行的维护命令')
    
    # 通用参数
    parser.add_argument('--log-level', '-l', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='日志级别')
    parser.add_argument('--cleanup-days', '-d', type=int, default=30,
                       metavar='DAYS',
                       help='清理超过指定天数的旧数据（默认: 30天）')
    
    # 重试相关参数
    parser.add_argument('--limit', type=int, default=50, 
                       help='重试TID数量限制 (仅用于 failed-retry)')
    parser.add_argument('--section', '-s', 
                       help='指定板块名称 (仅用于 failed-retry)')
    parser.add_argument('--batch-size', '-b', type=int, default=10,
                       help='批量大小 (仅用于 failed-retry)')
    parser.add_argument('--dry-run', action='store_true',
                       help='试运行模式，只列出受影响的数据而不实际执行 (仅用于 recycle-data)')
    
    args = parser.parse_args()
    
    # 设置日志级别
    setup_logging(args.log_level)
    
    # 执行命令
    try:
        if args.command == 'db-info':
            # 显示数据库信息
            maintenance = DatabaseMaintenance()
            info = maintenance.get_database_info()
            print("\n" + "="*50)
            print("数据库信息")
            print("="*50)
            for key, value in info.items():
                print(f"  {key}: {value}")
            print("="*50)
            
        elif args.command == 'db-cleanup':
            # 清理重复数据
            maintenance = DatabaseMaintenance()
            with maintenance.app.app_context():
                maintenance.clean_duplicates()
                maintenance.print_maintenance_report()
                
        elif args.command == 'db-optimize':
            # 优化数据库
            maintenance = DatabaseMaintenance()
            with maintenance.app.app_context():
                maintenance.optimize_database()
                
        elif args.command == 'db-normalize':
            # 标准化日期
            maintenance = DatabaseMaintenance()
            with maintenance.app.app_context():
                maintenance.normalize_dates()
                
        elif args.command == 'failed-analyze':
            # 分析失败TID
            cleaner = FailedTidCleaner()
            cleaner.analyze_failed_tids()
            
        elif args.command == 'failed-cleanup':
            # 清理失败TID
            cleaner = FailedTidCleaner()
            cleaner.cleanup_existing_tids()
            
        elif args.command == 'failed-retry':
            # 重试失败TID
            retry_service = FailedTidRetryService()
            result = retry_service.retry_failed_tids(
                section=args.section,
                limit=args.limit,
                batch_size=args.batch_size
            )
            
            if result['success']:
                print(f"\n🎉 重试完成!")
                print(f"   总重试数: {result['total_retry']}")
                print(f"   成功数: {result['success_count']}")
                print(f"   失败数: {result['failed_count']}")
                print(f"   已存在: {result['skipped_count']}")
                print(f"   成功率: {result.get('success_rate', 0):.1f}%")
            else:
                print(f"❌ 重试失败: {result.get('error', '未知错误')}")
                
        elif args.command == 'full-maintenance':
            # 完整维护流程
            print("🚀 开始完整维护流程...")
            print("\n步骤 1/4: 数据库维护")
            maintenance = DatabaseMaintenance()
            maintenance.run_full_maintenance()
            
            print("\n步骤 2/4: 清理旧数据（可选）")
            days = args.cleanup_days if hasattr(args, 'cleanup_days') else None
            if days:
                cleanup_result = maintenance.cleanup_old_records(days)
                print(f"\n✅ 清理了 {cleanup_result['deleted_resources']} 个旧资源记录（保留 {days} 天内的数据）")
                print(f"\n✅ 清理了 {cleanup_result['deleted_failed_tids']} 个失败TID记录")
            
            print("\n步骤 3/4: 数据库优化")
            maintenance.optimize_database()
            print("\n✅ 数据库优化完成")
            
            print("\n步骤 4/4: 清理缓存")
            maintenance.clear_cache()
            print("\n✅ 缓存已清理")
            
            print("\n步骤 5/4: 更新统计")
            maintenance.update_statistics()
            print("\n✅ 统计信息已更新")
            
            print("\n" + "="*50)
            days = args.cleanup_days if hasattr(args, 'cleanup_days') else None
            if days:
                cleanup_result = maintenance.cleanup_old_records(days)
                print(f"\n✅ 清理了 {cleanup_result['deleted_resources']} 个旧资源记录")
                print(f"✅ 清理了 {cleanup_result['deleted_failed_tids']} 个旧失败TID记录")
            
            print("\n步骤 3/4: 数据库优化")
            maintenance.optimize_database()
            
            print("\n步骤 2/4: 分析失败TID")
            cleaner = FailedTidCleaner()
            cleaner.analyze_failed_tids()
            
            print("\n步骤 3/4: 清理失败TID")
            cleaner.cleanup_existing_tids()
            
            print("\n步骤 4/4: 重试部分失败TID")
            retry_service = FailedTidRetryService()
            retry_service.retry_failed_tids(limit=20)
            
            print("\n✅ 完整维护流程已完成!")

        elif args.command == 'recycle-data':
            # 残缺数据回炉重造
            action_text = "预览" if args.dry_run else "执行"
            print(f"️ 开始{action_text}：将残缺资源退回重试队列...")
            result = recycle_incomplete_resources(limit=args.limit, dry_run=args.dry_run)
            if not args.dry_run:
                print(f"\n✅ 成功处理 {result['recycled_count']} 条记录。它们已出现在'失败重试'列表中。")
            
    except KeyboardInterrupt:
        print("\n\n⚠️ 操作被用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"执行命令时出错: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
