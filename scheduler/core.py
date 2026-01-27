#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫核心调度逻辑模块 - 负责执行爬取任务的核心逻辑
"""

import time
import logging
import os
import json
import random
import asyncio
import concurrent.futures
import datetime as _dt
from datetime import datetime, timezone, timedelta
from crawler import SHT, AsyncSHTCrawler
from utils.async_bridge import run_async
from sqlalchemy import func
from models import db, Resource, FailedTID, Category
from configuration import config_manager
from utils import get_flask_app, get_flask_app_context
from constants import SECTION_MAP, SECTION_NAME_TO_FID
from .state import sync_crawl_state
from .notifier import _send_telegram_message, _send_crawl_report, render_message_template
from .utils import stop_event, pause_event, sleep_interruptible

logger = logging.getLogger(__name__)


def update_crawl_state(updates):
    """
    统一状态更新入口

    更新爬虫状态到统一状态管理器，同时更新全局变量以保持向后兼容
    """
    from utils.state_manager import update_unified_state

    try:
        # 1. 更新统一状态管理器
        if updates:
            update_unified_state(updates, source='scheduler_core')

        # 2. 同时更新传统状态（向后兼容）
        try:
            from flask import current_app
            crawl_status = current_app.config.get('CRAWL_STATUS', {})
            crawl_progress = current_app.config.get('CRAWL_PROGRESS', {})
            crawl_control = current_app.config.get('CRAWL_CONTROL', {})
        except Exception:
            from cache_manager import cache_manager, CacheKeys
            crawl_status = cache_manager.shared_get(CacheKeys.CRAWL_STATUS) or {}
            crawl_progress = cache_manager.shared_get(CacheKeys.CRAWL_PROGRESS) or {}
            crawl_control = cache_manager.shared_get(CacheKeys.CRAWL_CONTROL) or {}

        for key, value in updates.items():
            # 直接更新字段，不进行存在性检查
            # 这样新增的字段也能正确保存到状态中
            if key in ['is_crawling', 'is_paused', 'message', 'should_stop']:
                crawl_status[key] = value
            elif key in ['sections_total', 'sections_done', 'current_section',
                       'current_page', 'max_pages', 'total_saved', 'total_skipped',
                       'current_section_pages', 'current_section_processed',
                       'processed_pages', 'estimated_total_pages', 'progress_percent',
                       'total_failed', 'current_section_saved', 'current_section_skipped',
                       'start_time',
                       # 页码概念区分字段
                       'current_page_actual', 'max_pages_actual',
                       'current_page_task', 'max_pages_task']:
                crawl_progress[key] = value
            elif key in ['stop', 'paused']:
                crawl_control[key] = value

        # 3. 同步到共享缓存，便于跨进程读取
        try:
            from cache_manager import cache_manager, CacheKeys
            cache_manager.shared_set(CacheKeys.CRAWL_STATUS, crawl_status)
            cache_manager.shared_set(CacheKeys.CRAWL_PROGRESS, crawl_progress)
            cache_manager.shared_set(CacheKeys.CRAWL_CONTROL, crawl_control)
        except Exception:
            pass

    except Exception as e:
        logger.debug(f"更新状态失败: {e}")


def run_crawling_task():
    """执行爬取任务"""
    logger.info("开始执行爬取任务...")
    
    sht = SHT()
    
    for fid, section_name in SECTION_MAP.items():
        logger.info(f"正在爬取分类: {section_name}")
        
        # 爬取前几页的数据
        for page in range(1, 4):  # 爬取前3页
            url = f"https://sehuatang.org/forum.php?mod=forumdisplay&fid={fid}&mobile=2&page={page}"
            
            try:
                tid_list = sht.crawler_tid_list(url)
                
                if not tid_list:
                    logger.warning(f"第{page}页爬取失败，跳过")
                    continue
                
                for tid in tid_list:
                    detail_url = (
                        f"https://sehuatang.org/forum.php?mod=viewthread&tid={tid}"
                    )
                    
                    try:
                        data = sht.crawler_detail(detail_url)
                        if data:
                            # 保存到数据库
                            app = get_flask_app_context()
                            with app.app_context():
                                saved = sht.save_to_db(data, section_name, tid, detail_url)
                                if saved:
                                    logger.info(f"成功保存资源: {data.get('title', '未知标题')}")
                                else:
                                    logger.info(f"资源已存在，跳过: {data.get('title', '未知标题')}")
                    except Exception as e:
                        logger.error(f"TID {tid} 爬取失败: {e}")
                        
            except Exception as e:
                logger.error(f"爬取分类 {section_name} 第{page}页失败: {e}")
    
    logger.info("爬取任务完成")


def run_crawling_with_options(section_fids=None, date_mode=None, date_value=None,
                              dateline=None, max_pages=3, crawl_options=None,
                              page_mode='fixed', page_range=None, task_type='manual'):
    """
    运行爬取任务
    :param page_range: 页码范围 [start, end] list (可选)
    """
    # 用于跟踪通知发送状态
    notification_sent = False

    try:
        global stop_event, pause_event
        stop_event.clear()
        pause_event.set() # 确保开始时非暂停

        # 读取状态容器（优先 Flask app.config，其次共享缓存）
        try:
            from flask import current_app
            crawl_status = current_app.config.get('CRAWL_STATUS', {})
            crawl_control = current_app.config.get('CRAWL_CONTROL', {})
            crawl_progress = current_app.config.get('CRAWL_PROGRESS', {})
        except Exception:
            from cache_manager import cache_manager, CacheKeys
            crawl_status = cache_manager.shared_get(CacheKeys.CRAWL_STATUS) or {}
            crawl_control = cache_manager.shared_get(CacheKeys.CRAWL_CONTROL) or {}
            crawl_progress = cache_manager.shared_get(CacheKeys.CRAWL_PROGRESS) or {}

        # 强制重置并设置状态机
        try:
            from crawler_control.cc_control_bridge import get_crawler_control_bridge
            bridge = get_crawler_control_bridge()

            # 1. 清除所有旧信号
            bridge.queue_manager.clear_signals()
            logger.info("🧹 已清除所有旧信号")

            # 2. 强制重置状态到idle（清除持久化的旧状态）
            bridge.coordinator.reset_state()
            logger.info("🔄 已重置状态机到idle")

            # 3. 立即转换到running
            bridge.coordinator.transition_state('running', {'started_at': time.time()})

            # 4. 验证状态
            current_state = bridge.coordinator.get_current_state()
            logger.info(f"📊 当前状态机状态: {current_state.current_state}")

            if current_state.current_state != 'running':
                # 5. 如果还不是running，强制设置
                logger.error(f"❌ 状态异常，强制设置为running")
                bridge.coordinator._current_state.current_state = 'running'
                bridge.coordinator._current_state.is_crawling = True
                bridge.coordinator._current_state.is_paused = False
                bridge.coordinator.force_persist()  # 强制持久化
                logger.warning("⚠️ 已强制设置状态为running")
            else:
                logger.info("✅ 已通知状态机：爬虫进入running状态")

        except Exception as e:
            logger.error(f"❌ 状态机初始化失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # 不要继续，因为控制系统不工作
            raise RuntimeError(f"无法初始化爬虫控制系统: {e}")
    
        # 应用最新的日志等级配置
        try:
            config_manager.apply_log_level()
        except Exception as e:
            logger.warning(f"应用日志等级失败: {e}")
    
        logger.info(f"🚀 开始筛选爬取: fids={section_fids}, date_mode={date_mode}, date_value={date_value}, dateline={dateline}, pages={max_pages}")
        logger.info(f"📋 爬取配置 - 分类数: {len(section_fids) if section_fids else '全部'}, 最大页数: {max_pages}")
        
        # 记录开始时间
        start_time = time.time()
        update_crawl_state({'start_time': start_time})
        last_notification_time = start_time  # 用于5分钟定时通知
        
        # 显示日期过滤设置和智能建议
        if date_mode == 'all' or not date_mode:
            logger.info(f"📅 日期过滤: 爬取所有日期的资源")
        elif date_mode == 'day' and date_value:
            logger.info(f"📅 日期过滤: 仅爬取 {date_value} 发布的资源")
            # 检查日期是否合理
            # 移除冗余局部导入，改用全局导入
            try:
                target_date = datetime.strptime(date_value, '%Y-%m-%d').date()
                today = datetime.now().date()
                if target_date > today:
                    logger.warning(f"⚠️ 设置的日期 {date_value} 是未来日期，可能不会找到资源")
                elif (today - target_date).days > 30:
                    logger.info(f"💡 设置的日期 {date_value} 较早，建议使用月份模式或时间范围过滤")
            except ValueError:
                logger.error(f"❌ 日期格式错误: {date_value}，应为 YYYY-MM-DD 格式")
        elif date_mode == 'month' and date_value:
            logger.info(f"📅 日期过滤: 仅爬取 {date_value} 月份发布的资源")
            # 检查月份格式
            try:
                datetime.strptime(date_value, '%Y-%m')
            except ValueError:
                logger.error(f"❌ 月份格式错误: {date_value}，应为 YYYY-MM 格式")
        else:
            logger.info(f"📅 日期过滤: 模式={date_mode}, 值={date_value}")
        
        if dateline:
            # 将秒数转换为可读的时间描述
            seconds = int(dateline)
            if seconds == 86400:
                time_desc = "近1天"
            elif seconds == 604800:
                time_desc = "近1周"
            elif seconds == 2592000:
                time_desc = "近1月"
            elif seconds == 31536000:
                time_desc = "近1年"
            else:
                days = seconds // 86400
                time_desc = f"近{days}天"
            logger.info(f"⏰ 时间范围过滤: {time_desc} ({dateline} 秒内的资源)")
        
        logger.debug(f"🔍 传入的section_fids类型: {type(section_fids)}, 内容: {section_fids}")
        
        from constants import SECTION_MAP, SECTION_NAME_TO_FID
        
        sht = SHT()
        
        chosen_items = []
        
        if not section_fids:
            # 如果没有指定分类，爬取所有分类
            chosen_items = list(SECTION_MAP.items())
            logger.debug("🔄 未指定分类，将爬取所有分类")
        else:
            # 统一转换为字符串，增强鲁棒性
            section_fids = [str(fid) for fid in section_fids]
            logger.debug(f"🔍 传入的section_fids: {section_fids}")
            logger.debug(f"🔍 SECTION_MAP的键: {list(SECTION_MAP.keys())}")
            logger.debug(f"🔍 SECTION_MAP的值: {list(SECTION_MAP.values())}")
            
            # 检查传入的是fid还是分类名称
            if all(fid in SECTION_MAP for fid in section_fids):
                # 传入的是fid
                chosen_items = [(fid, SECTION_MAP[fid]) for fid in section_fids]
                logger.debug("🔄 检测到传入的是fid")
            else:
                # 传入的是分类名称，需要转换为fid
                logger.debug(f"🔍 SECTION_NAME_TO_FID映射: {SECTION_NAME_TO_FID}")
                
                chosen_items = []
                for name in section_fids:
                    if name in SECTION_NAME_TO_FID:
                        fid = SECTION_NAME_TO_FID[name]
                        chosen_items.append((fid, name))
                        logger.debug(f"✅ 成功映射: '{name}' -> fid '{fid}'")
                    else:
                        logger.error(f"❌ 无法找到分类名称 '{name}' 对应的fid")
                
                logger.debug("🔄 检测到传入的是分类名称，已转换为fid")
        
        if not chosen_items:
            logger.error(f"❌ 没有找到有效的分类，section_fids={section_fids}")
            logger.error(f"❌ 可用的分类名称: {list(SECTION_MAP.values())}")
            logger.error(f"❌ 可用的fid: {list(SECTION_MAP.keys())}")
            return
        
        logger.info(f"📋 实际处理的分类: {[(fid, name) for fid, name in chosen_items]}")
    
        # ======== 生成并发送爬取任务开始报告 ========
        try:
            task_type_text = "手动爬取任务" if task_type == 'manual' else "自动定时爬取任务"
    
            # 模式描述映射
            crawler_mode = config_manager.get('CRAWLER_MODE', 'async').lower()
            mode_desc_map = {
                'async': '异步并发',
                'thread': '多线程',
                'sync': '同步单线程'
            }
            mode_desc = mode_desc_map.get(crawler_mode, crawler_mode)
    
            # 构建板块列表文本（完整列表）
            if not section_fids:
                all_board_names = "全部板块"
            else:
                all_board_names = "、".join([name for _, name in chosen_items])
    
            # 构建时间范围描述 (优化后)
            if dateline:
                seconds = int(dateline)
                day_map = {86400: "一天内", 172800: "两天内", 259200: "三天内", 604800: "一周内", 2592000: "一个月内", 7776000: "三个月内", 15552000: "半年内", 31536000: "一年内"}
                time_range = day_map.get(seconds, f"近{seconds // 86400}天")
            elif date_mode:
                mode_map = {
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
                time_range = mode_map.get(str(date_mode).lower(), date_mode)
                if date_value and date_mode in ['day', 'month']:
                    time_range = f"{date_value} ({time_range})"
            else:
                time_range = "全部时间"
    
            # 页数模式描述
            if page_mode == 'fixed':
                page_mode_desc = "固定模式"
                page_desc = f"{max_pages}页"
            elif page_mode == 'full':
                page_mode_desc = "全部页面"
                page_desc = f"{max_pages}页"
            elif page_mode == 'range' and page_range and len(page_range) == 2:
                # 指定范围时显示具体页码
                start_page, end_page = page_range
                page_mode_desc = "指定范围"
                page_desc = f"第{start_page}-{end_page}页"
            else:
                page_mode_desc = "指定范围"
                page_desc = f"{max_pages}页"
    
            # 生成初始通知 - 延迟到获取第一个板块信息后再发送
            initial_report_template = {
                'task_type': task_type_text,
                'all_boards': all_board_names,
                'time_range': time_range,
                'page_mode': page_mode_desc,
                'page_desc': page_desc,
                'mode': mode_desc
            }
    
        except Exception as report_err:
            logger.warning(f"生成爬取报告时出错（不影响爬取任务）: {report_err}")
        # ======== 报告生成结束 ========
        total_saved = 0
        total_skipped = 0
        total_failed = 0
        per_section = {name: {'saved': 0, 'skipped': 0, 'failed': 0} for name in [n for _, n in chosen_items]}
    
        # 使用统一状态更新
        update_crawl_state({
            'sections_total': len(list(chosen_items)),
            'sections_done': 0,
            'max_pages': max_pages,
            'processed_pages': 0,
            'total_saved': 0,
            'total_skipped': 0,
            'start_time': start_time,
            'is_crawling': True,
            'message': '正在初始化...'
        })
        
        # 页面级别统计
        page_stats = {
            'successful_pages': [],  # 成功的页面
            'failed_pages': [],      # 失败的页面
            'total_pages_attempted': 0,
            'total_pages_successful': 0,
            'total_pages_failed': 0
        }
        
        # 计算预估总页数
        estimated_total = 0
        for fid, section_name in chosen_items:
            if section_name:
                estimated_total += max_pages
    
        # 使用统一状态更新
        update_crawl_state({'estimated_total_pages': estimated_total})
        
        # 批量获取板块信息，避免重复请求 - 优化版本
        logger.info("📊 批量获取板块信息...")
        # 获取数据库中的所有分类
        categories = Category.get_all_categories()
        all_forums_info = {c.fid: c.to_dict() for c in categories}
    
        # 检查是否需要更新板块信息
        needs_refresh = False
        
        # 逻辑：1. 数据库无信息 2. 板块统计全为0 3. 距离上次更新超过24小时
        if not all_forums_info:
            logger.info("📋 数据库无板块信息，需要初始化获取")
            needs_refresh = True
        else:
            # 更智能的陈旧检测逻辑
            # 只有当：1. 关键数据缺失(total_topics=0) 且 2. 时间确实很旧，才触发刷新
            stale_threshold = datetime.now(timezone.utc) - timedelta(hours=24)
            
            # 检查当前选中的板块是否真的需要同步
            target_fids = [str(fid) for fid, _ in chosen_items]
            need_sync_fids = []
            for fid in target_fids:
                info = all_forums_info.get(fid)
                if not info:
                    need_sync_fids.append(fid)
                    continue
                
                # 检查更新时间
                last_upd = info.get('last_updated')
                if isinstance(last_upd, str):
                    try:
                        last_upd = datetime.fromisoformat(last_upd)
                    except (ValueError, TypeError):
                        last_upd = None
                
                if last_upd and last_upd.tzinfo is None:
                    last_upd = last_upd.replace(tzinfo=timezone.utc)
                
                # 判据：数据缺失 或 时间超过24小时
                if (not info.get('total_topics')) or (not last_upd) or (last_upd < stale_threshold):
                    need_sync_fids.append(fid)

            if not need_sync_fids:
                logger.info(f"📋 数据库信息尚在有效期内，共 {len(all_forums_info)} 个板块")
                needs_refresh = False
            else:
                logger.info(f"🕒 检测到 {len(need_sync_fids)} 个板块信息需同步，正在更新...")
                needs_refresh = True
    
        if needs_refresh:
            logger.info("🔄 正在实时获取板块信息...")
            fresh_info = sht.get_all_forums_info()
            if fresh_info:
                # 保存到数据库
                Category.update_forum_info(fresh_info)
                all_forums_info = fresh_info
                logger.info(f"✅ 板块信息已更新并保存，共 {len(all_forums_info)} 个板块")
            else:
                logger.warning("⚠️ 获取板块信息失败，使用现有数据")
                if not all_forums_info:
                    all_forums_info = {}
    
        # 重新计算预估总页数（基于实际板块信息和页数模式）
        try:
            corrected_estimated_total = 0
            for fid, section_name in chosen_items:
                if not section_name:
                    continue
    
                forum_info = all_forums_info.get(fid)
                actual_total_pages = forum_info.get('total_pages', 0) if forum_info else 0
    
                # 根据页数模式计算该板块的实际爬取页数
                if page_range:
                    # 范围模式：使用范围的实际差值
                    start_page_range = int(page_range[0])
                    end_page_range = int(page_range[1])
                    actual_end_page = min(end_page_range, max(actual_total_pages, 1))
                    actual_start_page = max(start_page_range, 1)
                    section_adjusted_pages = max(1, actual_end_page - actual_start_page + 1)
                else:
                    # 固定页数或全部模式
                    if page_mode == 'full':
                        section_adjusted_pages = max(1, actual_total_pages)
                    else:
                        # 固定页数模式，但不超过板块最大页数
                        section_adjusted_pages = max(1, min(max_pages, max(actual_total_pages, 1)))
    
                corrected_estimated_total += section_adjusted_pages
    
            # 更新预估总页数
            if corrected_estimated_total > 0:
                update_crawl_state({'estimated_total_pages': corrected_estimated_total})
                logger.info(f"📊 校正预估总页数: {estimated_total} -> {corrected_estimated_total} 页")
        except Exception as e:
            logger.warning(f"⚠️ 校正预估总页数失败，使用初始值: {e}")
            # 保持使用初始计算的 estimated_total
    
        for fid, section_name in chosen_items:
            if not section_name:
                continue
            logger.info(f"📂 开始爬取分类: {section_name} (fid={fid})")
            # 使用统一状态更新
            update_crawl_state({
                'current_section': section_name,
                'current_section_saved': 0,
                'current_section_skipped': 0
            })
            
            # --- [优化] 增量同步水位线锚点 ---
            try:
                # 获取该板块目前数据库里的最大 TID 作为终止锚点
                stop_tid = db.session.query(func.max(Resource.tid)).filter(Resource.section == section_name).scalar() or 0
                logger.info(f"📍 [{section_name}] 增量同步水位线: {stop_tid}")
            except Exception as e:
                stop_tid = 0
                logger.warning(f"⚠️ 获取水位线失败: {e}")
    
            # 板块错误计数器，用于严重错误通知
            section_error_count = 0
            section_error_notified = False  # 避免重复通知
            
            # 从批量获取的信息中查找板块信息，避免重复请求
            forum_info = all_forums_info.get(fid)
            if forum_info:
                actual_total_pages = forum_info.get('total_pages') or 0
                total_topics = forum_info.get('total_topics') or 0
    
                logger.info(f"📊 [{section_name}] 板块统计: 总计{total_topics}主题, 共{actual_total_pages}页")
    
                # 特殊处理：0主题的板块
                if total_topics == 0 or actual_total_pages == 0:
                    logger.warning(f"⚠️ [{section_name}] 板块显示0主题/0页（可能是新板块或信息未更新）")
                    logger.info(f"💡 [{section_name}] 将强制爬取第1页以验证板块状态")
                    # 强制爬取至少1页来验证
                    adjusted_pages = 1
                    actual_total_pages = max(1, actual_total_pages)  # 确保actual_total_pages也至少为1
                else:
                    # 正常情况：智能调整爬取页数
                    if max_pages > actual_total_pages:
                        adjusted_pages = actual_total_pages
                        logger.info(f"📉 [{section_name}] 调整爬取页数: {max_pages} -> {adjusted_pages} (板块总页数限制)")
                    else:
                        adjusted_pages = max_pages
    
                    # 内容丰富度提示
                    if total_topics > 1000:
                        logger.info(f"🔥 [{section_name}] 内容丰富({total_topics}主题)，建议适当增加爬取页数")
    
                    # 智能页数优化：如果主题很少，减少爬取页数
                    if total_topics < 50 and adjusted_pages > 2:
                        adjusted_pages = 2
                        logger.info(f"📉 [{section_name}] 主题较少({total_topics})，优化页数为: {adjusted_pages}")
                    elif total_topics < 20 and adjusted_pages > 1:
                        adjusted_pages = 1
                        logger.info(f"📉 [{section_name}] 主题很少({total_topics})，优化页数为: {adjusted_pages}")
            else:
                adjusted_pages = max_pages
                actual_total_pages = max_pages  # 默认值
                logger.warning(f"⚠️ [{section_name}] 无法获取板块信息，使用默认页数: {max_pages}")
            
            # 确保 display_total_pages 有默认值
            display_total_pages = actual_total_pages
    
            # 根据页数模式决定爬取策略
            start_page = 1
            page_order = "升序"  # 从第1页到第N页
    
            if page_range: # 再次确认范围模式
                start_page_range = int(page_range[0])
                end_page_range = int(page_range[1])
                # 计算实际结束页（不能超过论坛总页数）
                actual_end_page = min(end_page_range, actual_total_pages)
                # 重新计算实际爬取页数
                adjusted_pages = actual_end_page - start_page_range + 1
                # 显示的总页数用论坛实际总页数
                display_total_pages = actual_total_pages
                # 重新生成页码列表（重要！）
                pages_to_crawl = range(start_page_range, actual_end_page + 1)
                logger.info(f"📊 范围模式确认:")
                logger.info(f"   - 实际爬取: 第{start_page_range}页 到 第{actual_end_page}页")
                logger.info(f"   - 总任务页数: {adjusted_pages}页")
                logger.info(f"   - 显示格式: 第X/{display_total_pages}页")
            
            if not page_range:
                if page_mode == 'fixed':
                # 固定页数模式：从第1页开始爬取指定页数
                # 注意：如果是0主题的特殊情况，adjusted_pages已经被设置为1，这里不再覆盖
                    start_page = 1
                    page_order = "升序"
                    display_total_pages = actual_total_pages
                    logger.info(f"📊 [{section_name}] 固定页数模式: 从第1页开始爬取{adjusted_pages}页")
                
                elif page_mode == 'full':
                    # 全部页面模式：爬取所有页面
                    start_page = 1
                    adjusted_pages = actual_total_pages
                    page_order = "升序"
                    display_total_pages = actual_total_pages
                    logger.info(f"📊 [{section_name}] 全部页面模式: 爬取全部{adjusted_pages}页")
    
            # 更新进度信息
            # 使用统一状态更新
            update_crawl_state({
                'current_section_pages': adjusted_pages,
                'current_section_processed': 0
            })
    
            # 发送板块通知
            current_board_index = chosen_items.index((fid, section_name))
    
            # 计算实际页码范围
            if page_range and len(page_range) == 2:
                actual_page_range = f"第{page_range[0]}-{actual_end_page}页"
            else:
                actual_page_range = f"第1-{adjusted_pages}页"
    
            if current_board_index == 0:
                # 发送初始任务通知
                try:
                    # 构建候选板块列表
                    pending_boards = [name for _, name in chosen_items[1:]]
                    pending_text = "、".join(pending_boards) if pending_boards else "无"
    
                    context = {
                        'task_type': initial_report_template.get('task_type'),
                        'all_boards': initial_report_template.get('all_boards'),
                        'time_range': initial_report_template.get('time_range'),
                        'page_mode': initial_report_template.get('page_mode'),
                        'page_desc': initial_report_template.get('page_desc'),
                        'mode': initial_report_template.get('mode'),
                        'section_name': section_name,
                        'actual_page_range': actual_page_range,
                        'pending_boards': pending_text,
                        'initial_report_template': initial_report_template
                    }
    
                    initial_msg, parse_mode = render_message_template('initial_report', context)
                    if not initial_msg:
                        initial_msg = f"""🚀 *开始{initial_report_template['task_type']}，本次爬取配置：*
    板块：{initial_report_template['all_boards']}
    时间：{initial_report_template['time_range']}
    页数：{initial_report_template['page_mode']} \\- {initial_report_template['page_desc']}
    模式：{initial_report_template['mode']}

    ━━━━━━━━━━━━━━
    📂 当前进行中的板块：{section_name}
    📄 板块 {section_name} 的实际任务页数：{actual_page_range}
    ⏳ 候选中的板块：{pending_text}"""
                        parse_mode = 'Markdown'
    
                    _send_telegram_message(initial_msg, parse_mode=parse_mode)
                    logger.info(f"✅ 已发送初始任务通知")
                except Exception as e:
                    logger.debug(f"发送初始任务通知失败: {e}")
    
            else:
                # 发送板块切换通知
                try:
                    # 构建已完成板块列表
                    completed_boards = [name for _, name in chosen_items[:current_board_index]]
                    completed_text = "、".join(completed_boards)
    
                    # 构建候选板块列表
                    pending_boards = [name for _, name in chosen_items[current_board_index + 1:]]
                    pending_text = "、".join(pending_boards) if pending_boards else "无"
    
                    # 上一个板块名称
                    prev_board_name = chosen_items[current_board_index - 1][1]
    
                    context = {
                        'task_type': initial_report_template.get('task_type'),
                        'all_boards': initial_report_template.get('all_boards'),
                        'time_range': initial_report_template.get('time_range'),
                        'page_mode': initial_report_template.get('page_mode'),
                        'page_desc': initial_report_template.get('page_desc'),
                        'mode': initial_report_template.get('mode'),
                        'section_name': section_name,
                        'actual_page_range': actual_page_range,
                        'completed_boards': completed_text,
                        'pending_boards': pending_text,
                        'prev_board_name': prev_board_name,
                        'initial_report_template': initial_report_template
                    }
    
                    board_switch_msg, parse_mode = render_message_template('board_switch', context)
                    if not board_switch_msg:
                        board_switch_msg = f"""✅ *{prev_board_name} 板块已完成，开始爬取候选板块*
    
    本次爬取配置：
    板块：{initial_report_template['all_boards']}
    时间：{initial_report_template['time_range']}
    页数：{initial_report_template['page_mode']} - {initial_report_template['page_desc']}
    模式：{initial_report_template['mode']}
    
    ━━━━━━━━━━━━━━
    📂 当前进行中的板块：{section_name}
    📄 板块 {section_name} 的实际任务页数：{actual_page_range}
    ✅ 已完成的板块：{completed_text}
    ⏳ 候选中的板块：{pending_text}"""
                        parse_mode = 'Markdown'
    
                    _send_telegram_message(board_switch_msg, parse_mode=parse_mode)
                    logger.info(f"✅ 已发送板块切换通知 ({prev_board_name} → {section_name})")
                except Exception as e:
                    logger.debug(f"发送板块切换通知失败: {e}")
    
            # 根据页数顺序生成页码列表（仅在非范围模式下）
            if not page_range:
                if page_order == "降序":
                    pages_to_crawl = range(actual_total_pages, start_page - 1, -1)
                    logger.info(f"📄 [{section_name}] 爬取顺序: 降序 (第{actual_total_pages}页 -> 第{start_page}页)")
                else:
                    pages_to_crawl = range(start_page, start_page + adjusted_pages)
                    logger.info(f"📄 [{section_name}] 爬取顺序: 升序 (第{start_page}页 -> 第{start_page + adjusted_pages - 1}页)")
            else:
                # 范围模式下，pages_to_crawl 已经在第399行正确设置
                logger.info(f"📄 [{section_name}] 爬取顺序: 升序 (第{start_page_range}页 -> 第{actual_end_page}页)")
            
    
            # 检查是否从暂停恢复，如果是则从保存的位置继续
            resume_offset = 0
            try:
                from crawler_control.cc_control_bridge import get_crawler_control_bridge
                bridge = get_crawler_control_bridge()
                saved_loop_state = bridge.coordinator.get_page_loop_state()
                
                if saved_loop_state and saved_loop_state.get('section_name') == section_name:
                    # 检查是否是同一个分类的恢复
                    resume_offset = saved_loop_state.get('current_offset', 0)
                    if resume_offset > 0:
                        logger.info(f"📍 从暂停点恢复: 分类={section_name}, 从偏移量={resume_offset} 继续")
                        # 清除保存的状态
                        bridge.coordinator._current_state.progress.pop('page_loop_state', None)
                        bridge.coordinator.force_persist()
            except Exception as e:
                logger.warning(f"⚠️ 恢复暂停状态失败: {e}")
            
            # 在循环中使用，支持从保存的偏移量继续
            pages_to_crawl_list = list(pages_to_crawl)
            pages_to_process = pages_to_crawl_list[resume_offset:]
            
            # v1.4.0: 加速查漏模式 (Burst Mode)
            # 如果是异步模式且页数较多，开启分页批处理，并发获取多页 TID
            crawler_mode = config_manager.get('CRAWLER_MODE', 'async').lower()
            page_batch_size = 5 if crawler_mode == 'async' else 1
            
            if page_batch_size > 1:
                logger.info(f"⚡ [{section_name}] 启用加速查漏模式: 每批次并发处理 {page_batch_size} 页列表")
            
            i = 0
            while i < len(pages_to_process):
                batch_indices = pages_to_process[i:i + page_batch_size]
                from .utils import check_stop_and_pause
                if check_stop_and_pause(): break
                burst_results = []
                if page_batch_size > 1:
                    # v1.4.6: [优化] 构造URL并同步UI状态
                    burst_urls = [f'https://sehuatang.org/forum.php?mod=forumdisplay&fid={fid}&mobile=2&page={p}' for p in batch_indices]
                    if dateline and str(dateline).strip() and str(dateline).strip() != '0':
                        dl_v = str(dateline).strip()
                        burst_urls = [f'{u}&orderby=dateline&filter=dateline&dateline={dl_v}' for u in burst_urls]
                    
                    target_pages_desc = f"第{batch_indices[0]}-{batch_indices[-1]}页"
                    update_crawl_state({
                        'message': f'正在并发扫描 [{section_name}] {target_pages_desc}...',
                        'current_page_actual': batch_indices[0]
                    })

                    try:
                        p_a = sht.proxies.get('http') if (hasattr(sht, 'proxies') and sht.proxies) else None
                        c_a = sht.cookie if hasattr(sht, 'cookie') else {'_safe': ''}
                        async def f_b():
                            async with AsyncSHTCrawler(max_connections=page_batch_size, proxy=p_a, cookies=c_a) as c:
                                return await c.crawl_tids_batch(burst_urls)
                        burst_results = run_async(f_b(), timeout=60.0)
                    except Exception as burst_err:
                        import traceback
                        logger.error(f"❌ [BURST] 列表批量获取致命异常: {burst_err}")
                        logger.debug(traceback.format_exc())
                        burst_results = [[] for _ in batch_indices]
                else:
                    p_idx = batch_indices[0]
                    u = f'https://sehuatang.org/forum.php?mod=forumdisplay&fid={fid}&mobile=2&page={p_idx}'
                    if dateline and str(dateline).strip() and str(dateline).strip() != '0':
                        u += f'&orderby=dateline&filter=dateline&dateline={str(dateline).strip()}'
                    try: 
                        burst_results = [sht.crawler_tid_list(u) or []]
                    except Exception as sync_err:
                        logger.error(f"❌ [SYNC] 同步获取TID列表失败: {sync_err}")
                        burst_results = [[]]
                
                batch_tasks = []
                reached_boundary = False
                
                # --- 步骤 2: 汇总缺失详情任务 ---
                for offset, page_tids in enumerate(burst_results):
                    curr_p = batch_indices[offset]
                    p_idx_curr = resume_offset + i + offset + 1
                    
                    # 进度报告
                    sect_prog_curr = (p_idx_curr / adjusted_pages) * 100
                    pg_disp_curr = f"第{curr_p}/{display_total_pages}页"
                    
                    update_crawl_state({
                        'current_page_actual': curr_p,
                        'max_pages_actual': display_total_pages,
                        'current_page_task': p_idx_curr,
                        'max_pages_task': adjusted_pages,
                        'current_page': curr_p,
                        'progress_percent': round(sect_prog_curr, 1),
                        'message': f'正在扫描 [{section_name}] {pg_disp_curr} ({p_idx_curr}/{adjusted_pages})...'
                    })

                    # 心跳监控
                    try:
                        heartbeat_interval = int(config_manager.get('HEARTBEAT_INTERVAL', 60))
                        cur_t = time.time()
                        if cur_t - last_notification_time >= heartbeat_interval:
                            elapsed_m = int((cur_t - start_time) / 60)
                            total_prog = int((crawl_progress.get('processed_pages', 0) / max(crawl_progress.get('estimated_total_pages', 1), 1)) * 100)
                            
                            heart_ctx = {
                                'elapsed_minutes': elapsed_m, 'section_name': section_name,
                                'page_display': pg_disp_curr, 'section_progress_percent': f"{sect_prog_curr:.1f}",
                                'task_progress_display': f"{p_idx_curr}/{adjusted_pages}",
                                'total_progress_percent': total_prog,
                                'processed_pages': crawl_progress.get('processed_pages', 0),
                                'estimated_total_pages': crawl_progress.get('estimated_total_pages', 0),
                                'total_saved': total_saved, 'total_skipped': total_skipped,
                                'total_failed': total_failed, 'timestamp': _dt.datetime.now().strftime('%H:%M:%S')
                            }
                            h_msg, p_mode = render_message_template('heartbeat', heart_ctx)
                            if not h_msg: # Fallback
                                h_msg = f"💓 *Burst Mode 运行中*\n⏱️ 已运行: {elapsed_m}m\n📂 板块: {section_name}\n📄 进度: {pg_disp_curr} ({sect_prog_curr:.1f}%)\n✅ 已存: {total_saved}\n❌ 失败: {total_failed}"
                                p_mode = 'Markdown'
                            _send_telegram_message(h_msg, parse_mode=p_mode)
                            last_notification_time = cur_t
                    except: pass

                    if not page_tids:
                        logger.warning(f"⚠️ [{section_name}] {pg_disp_curr} 扫描失败，已记录以待后续重试")
                        page_stats['total_pages_failed'] += 1
                        
                        # 记录到页面统计用于最后汇总
                        page_stats['failed_pages'].append({
                            'section': section_name,
                            'page': curr_p,
                            'reason': '列表扫描失败'
                        })
                        
                        # 记录到全局待重试列表
                        if 'failed_pages' not in crawl_progress:
                            crawl_progress['failed_pages'] = []
                        
                        # 构建该页面的完整URL用于重试时定位
                        retry_url = f"https://sehuatang.org/forum.php?mod=forumdisplay&fid={fid}&mobile=2&page={curr_p}"
                        if dateline and str(dateline).strip() and str(dateline).strip() != '0':
                            retry_url += f"&orderby=dateline&filter=dateline&dateline={str(dateline).strip()}"
                            
                        crawl_progress['failed_pages'].append({
                            'section_name': section_name,
                            'section_fid': fid,
                            'page': curr_p,
                            'url': retry_url
                        })
                        continue
                    
                    page_stats['total_pages_attempted'] += 1
                    
                    # 历史边界检查
                    if stop_tid > 0 and curr_p == 1:
                        if sum(1 for t in page_tids if t <= stop_tid) > 3 or (page_tids and max(page_tids) <= stop_tid):
                            logger.info(f"⏭️ [{section_name}] 触碰增量水位线 (TID <= {stop_tid})")
                            reached_boundary = True
                    
                    # 批量过滤
                    try:
                        e_tids = db.session.query(Resource.tid).filter(Resource.tid.in_(page_tids)).all()
                        e_set = {t[0] for t in e_tids}
                        f_cnt = 0
                        for tid in page_tids:
                            if tid not in e_set:
                                batch_tasks.append((tid, f'https://sehuatang.org/forum.php?mod=viewthread&tid={tid}'))
                            else:
                                total_skipped += 1
                                per_section[section_name]['skipped'] += 1
                                f_cnt += 1
                        if f_cnt > 0:
                            logger.info(f"🔍 [{section_name}] {pg_disp_curr} 过滤掉 {f_cnt} 个数据库已有资源")
                    except Exception as e:
                        logger.warning(f"⚠️ 库过滤失败: {e}")
                    
                    if reached_boundary: break

                # --- 步骤 3: 提取详情 (并发执行) ---
                if batch_tasks:
                    logger.info(f"🚀 [{section_name}] 发现 {len(batch_tasks)} 个新增资源，开始并发详情采集...")
                    m_urls = [t[1] for t in batch_tasks]
                    m_results = []
                    
                    try:
                        # v1.5.3: [根本修复] 详情采集强制使用线程池模式
                        # 原因：async + curl_cffi 在某些网络条件下会进入无法恢复的死锁
                        # 线程池虽然慢一点，但绝对不会卡死
                        force_thread_mode = config_manager.get('FORCE_THREAD_DETAIL_CRAWL', True)
                        
                        if crawler_mode == 'async' and not force_thread_mode:
                            # 仅在用户明确禁用强制线程模式时才使用异步
                            logger.warning(f"⚠️ [{section_name}] 使用异步模式采集详情（可能存在卡死风险）")
                            max_c = config_manager.get('CRAWLER_MAX_CONCURRENCY', 20)
                            p_a = sht.proxies.get('http') if sht.proxies else None
                            c_a = sht.cookie if hasattr(sht, 'cookie') else {'_safe': ''}
                            
                            batch_start_time = time.time()
                            logger.info(f"📡 [{section_name}] 开始异步批量采集 {len(m_urls)} 个详情页...")
                            
                            async def fetch_details():
                                async with AsyncSHTCrawler(max_connections=max_c, proxy=p_a, cookies=c_a) as c:
                                    return await c.crawl_details_batch(m_urls)
                            
                            detail_timeout = min(120, len(m_urls) * 10)
                            
                            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                                future = executor.submit(run_async, fetch_details(), detail_timeout)
                                poll_start = time.time()
                                task_abandoned = False
                                
                                while True:
                                    if stop_event.is_set() or check_stop_and_pause():
                                        logger.warning(f"🛑 [{section_name}] 详情采集期间检测到停止信号，放弃本批次")
                                        stop_event.set()
                                        task_abandoned = True
                                        m_results = [None] * len(m_urls)
                                        break
                                    
                                    try:
                                        m_results = future.result(timeout=0.5)
                                        batch_elapsed = time.time() - batch_start_time
                                        logger.info(f"✅ [{section_name}] 批量采集完成，耗时 {batch_elapsed:.1f}秒")
                                        break
                                    except concurrent.futures.TimeoutError:
                                        elapsed = time.time() - poll_start
                                        if int(elapsed) % 10 == 0 and int(elapsed) > 0:
                                            logger.info(f"⏳ [{section_name}] 详情采集进行中... 已等待 {int(elapsed)}秒")
                                        if elapsed > detail_timeout:
                                            logger.error(f"🔴 [{section_name}] 详情采集超时 (>{detail_timeout}s)，放弃本批次")
                                            task_abandoned = True
                                            m_results = [None] * len(m_urls)
                                            break
                                        continue
                                    except Exception as e:
                                        logger.error(f"❌ [{section_name}] 详情采集异常: {e}")
                                        m_results = [None] * len(m_urls)
                                        break
                                
                                if task_abandoned:
                                    logger.debug(f"⏳ 等待后台线程响应停止信号...")
                                    try:
                                        future.result(timeout=2.0)
                                    except:
                                        logger.debug(f"⚠️ 后台线程未在2秒内退出，继续主流程")
                        else:
                            # v1.5.3: 默认使用线程池同步模式（稳定可靠）
                            logger.info(f"🔧 [{section_name}] 使用线程池模式采集 {len(m_urls)} 个详情页（稳定模式）")
                            m_results = sht.crawler_details_batch(m_urls, use_batch_mode=True)
                        
                        # --- 步骤 4: 保存结果 ---
                        for idx, data in enumerate(m_results):
                            if idx % 5 == 0 and check_stop_and_pause(): break
                            
                            tid, u_d = batch_tasks[idx]
                            if not data or not data.get('magnet'):
                                reason = "解析失败" if not data else "无磁力链接"
                                if FailedTID.add(tid=tid, section=section_name, url=u_d, reason=reason):
                                    total_failed += 1
                                    per_section[section_name]['failed'] += 1
                                    logger.debug(f"⚠️ {tid} 进入重试列表")
                                continue

                            # 日期过滤
                            pub = (data.get('publish_date') or '').strip()
                            if date_mode == 'day' and date_value and pub != date_value: continue
                            if date_mode == 'month' and date_value and not pub.startswith(date_value): continue

                            with get_flask_app_context().app_context():
                                if sht.save_to_db(data, section_name, tid, u_d):
                                    total_saved += 1
                                    per_section[section_name]['saved'] += 1
                                    try: FailedTID.mark_success(tid)
                                    except: pass
                                    logger.info(f"✅ [{section_name}] 新增: {data.get('title', '')[:40]}...")
                                else:
                                    total_skipped += 1
                                    per_section[section_name]['skipped'] += 1
                    except Exception as e:
                        logger.error(f"❌ 详情批量采集逻辑异常: {e}")

                # --- 批次结算 ---
                processed_in_batch = len(batch_indices)
                i += processed_in_batch
                
                update_crawl_state({
                    'total_saved': total_saved,
                    'total_skipped': total_skipped,
                    'total_failed': total_failed,
                    'processed_pages': resume_offset + i
                })

                if reached_boundary:
                    logger.info(f"🏁 [{section_name}] 增量同步完成")
                    break
                
                if page_batch_size == 1 and i < len(pages_to_process):
                    delay = random.uniform(2, 5)
                    if sleep_interruptible(delay): break
                
                if total_failed >= config_manager.get('GLOBAL_ERROR_THRESHOLD', 300):
                    logger.error("🛑 全局错误过多，终止板块任务")
                    stop_event.set()
                    break
            sections_done = crawl_progress.get('sections_done', 0) + 1
            update_crawl_state({'sections_done': sections_done})
            sync_crawl_state()
            logger.info(f"✅ 分类 [{section_name}] 爬取完成 - 新增: {per_section[section_name]['saved']}, 跳过: {per_section[section_name]['skipped']}")
        
        logger.info(f"🎉 筛选爬取完成 - 总计新增: {total_saved}, 总计跳过: {total_skipped}")

        # --- 🚀 [修复] 最终重试阶段 ---
        failed_pages_list = crawl_progress.get('failed_pages', [])
        retry_stats = {'attempted': 0, 'successful': 0, 'failed': 0, 'saved': 0, 'skipped': 0}
        
        if failed_pages_list and not (stop_event.is_set() or check_stop_and_pause()):
            logger.info(f"🔄 正在对 {len(failed_pages_list)} 个失败任务进行最终重试...")
            update_crawl_state({'message': f'正在重试 {len(failed_pages_list)} 个页面...'})
            
            for fail_item in list(failed_pages_list):
                if stop_event.is_set() or check_stop_and_pause():
                    break
                
                f_sect = fail_item['section_name']
                f_page = fail_item['page']
                f_url = fail_item['url']
                
                logger.info(f"🔄 重试 [{f_sect}] 第{f_page}页: {f_url}")
                retry_stats['attempted'] += 1
                
                # 更新UI状态显示当前正在重试
                update_crawl_state({
                    'message': f'正在重试 [{f_sect}] 第{f_page}页...',
                    'current_section': f_sect,
                    'current_page_actual': f_page,
                    'total_saved': total_saved,
                    'total_skipped': total_skipped
                })
                
                # v1.4.9: 使用标志位控制外层循环退出
                should_stop_retry = False
                
                try:
                    # v1.4.3b: 扫尾重试也加入观察延迟和中断检查
                    if sleep_interruptible(random.uniform(2, 4)): 
                        should_stop_retry = True
                    
                    if not should_stop_retry:
                        # v1.4.8: [关键] 网络操作前再次检查停止信号
                        if stop_event.is_set() or check_stop_and_pause():
                            logger.info(f"🛑 检测到停止信号，终止重试循环")
                            should_stop_retry = True
                    
                    if not should_stop_retry:
                        # 同步获取 TID 列表
                        tid_list = sht.crawler_tid_list(f_url)
                        
                        # v1.4.8: 网络操作后立即检查
                        if stop_event.is_set() or check_stop_and_pause():
                            logger.info(f"🛑 TID获取完成后检测到停止信号")
                            should_stop_retry = True
                    
                    if not should_stop_retry and tid_list:
                        retry_stats['successful'] += 1
                        
                        # 更新页面统计：从原本的失败列表移除，后续记录成功
                        for idx_f, f_p in enumerate(page_stats['failed_pages']):
                            if f_p['section'] == f_sect and f_p['page'] == f_page:
                                page_stats['failed_pages'].pop(idx_f)
                                page_stats['total_pages_failed'] -= 1
                                break
                        
                        with get_flask_app_context().app_context():
                            existing_tids = db.session.query(Resource.tid).filter(Resource.tid.in_(tid_list)).all()
                            ex_set = {t[0] for t in existing_tids}
                        
                        to_crawl = []
                        f_cnt_retry = 0
                        for tid in tid_list:
                            if tid not in ex_set:
                                to_crawl.append((tid, f"https://sehuatang.org/forum.php?mod=viewthread&tid={tid}"))
                            else:
                                total_skipped += 1
                                per_section[f_sect]['skipped'] += 1
                                retry_stats['skipped'] += 1
                                f_cnt_retry += 1
                        
                        if f_cnt_retry > 0:
                            logger.info(f"🔍 [{f_sect}] 第{f_page}页 (重试) 过滤掉 {f_cnt_retry} 个数据库已有资源")
                        
                        p_saved = 0
                        if to_crawl and not should_stop_retry:
                            # v1.4.8: 批量采集前的最后检查
                            if stop_event.is_set() or check_stop_and_pause():
                                logger.info(f"🛑 详情采集前检测到停止信号，跳过剩余 {len(to_crawl)} 个资源")
                                should_stop_retry = True
                            
                            if not should_stop_retry:
                                b_urls = [u for t, u in to_crawl]
                                # 使用线程池并发采集详情
                                res = sht.crawler_details_batch(b_urls, use_batch_mode=True)
                                
                                # v1.4.8: 批量采集完成后立即检查
                                if stop_event.is_set() or check_stop_and_pause():
                                    logger.info(f"🛑 详情采集完成后检测到停止信号，不保存结果")
                                    should_stop_retry = True
                                
                                if not should_stop_retry:
                                    for idx, d in enumerate(res):
                                        # 每 5 个检查一次
                                        if idx % 5 == 0:
                                            if stop_event.is_set() or check_stop_and_pause():
                                                logger.info(f"🛑 保存过程中检测到停止信号，已保存 {idx}/{len(res)} 个")
                                                should_stop_retry = True
                                                break
                                        
                                        if should_stop_retry:
                                            break
                                            
                                        tid_r, url_r = to_crawl[idx]
                                        
                                        # v1.4.4: 对齐主循环逻辑
                                        if not d or not d.get('magnet'):
                                            reason = "重试解析失败" if not d else "重试无磁力链接"
                                            if FailedTID.add(tid=tid_r, section=f_sect, url=url_r, reason=reason):
                                                total_failed += 1
                                                per_section[f_sect]['failed'] += 1
                                            continue

                                        # 日期过滤 (重要：防止重试救回了不符合日期要求的资源)
                                        pub = (d.get('publish_date') or '').strip()
                                        if date_mode == 'day' and date_value and pub != date_value: continue
                                        if date_mode == 'month' and date_value and not pub.startswith(date_value): continue

                                        with get_flask_app_context().app_context():
                                            if sht.save_to_db(d, f_sect, tid_r, url_r):
                                                total_saved += 1
                                                per_section[f_sect]['saved'] += 1
                                                retry_stats['saved'] += 1
                                                p_saved += 1
                                                logger.info(f"✅ [{f_sect}] 新增 (重试): {d.get('title', '')[:40]}...")
                                                try: FailedTID.mark_success(tid_r)
                                                except: pass
                                            else:
                                                total_skipped += 1
                                                per_section[f_sect]['skipped'] += 1
                                                retry_stats['skipped'] += 1
                        
                        if not should_stop_retry:
                            page_stats['successful_pages'].append({
                                'section': f_sect, 'page': f_page, 'saved': p_saved, 
                                'skipped': len(tid_list) - p_saved, 'is_retry': True
                            })
                            page_stats['total_pages_successful'] += 1
                            if fail_item in crawl_progress['failed_pages']:
                                crawl_progress['failed_pages'].remove(fail_item)
                                
                except Exception as e:
                    logger.warning(f"❌ 重试仍失败: {e}")
                    retry_stats['failed'] += 1
                
                # v1.4.9: 检查是否需要终止整个重试循环
                if should_stop_retry:
                    logger.info(f"🛑 重试循环因停止信号而终止")
                    break
            
            update_crawl_state({'total_saved': total_saved})
            sync_crawl_state()
    
        # 判断任务完成状态
        completion_status = "爬取完成"  # 默认
        exception_reason = None
    
        # 检查是否被手动停止
        from .utils import check_stop_and_pause
        if stop_event.is_set() or check_stop_and_pause():
            completion_status = "手动终止"
        elif total_failed > 0 and (total_saved + total_skipped) == 0:
            completion_status = "异常终止"
            exception_reason = "爬取到的资源全部失败，无有效数据"
    
        # 清除所有相关缓存，确保数据立即可见
        try:
            from cache_manager import cache_manager, CacheKeys
            cache_manager.delete(CacheKeys.STATS)  # 清除统计缓存
            cache_manager.delete(CacheKeys.CATEGORIES)  # 清除分类缓存
            logger.info("✅ 已清除统计和分类缓存，新数据将立即可见")
        except Exception as e:
            logger.warning(f"清除缓存失败: {e}")
    
        # 构建详细的爬取摘要
        # 移除冗余局部导入，改用全局导入
        
        # 计算爬取时长
        end_time = time.time()
        start_time = crawl_progress.get('start_time', end_time)  # 如果没有开始时间，使用结束时间
        duration_seconds = int(end_time - start_time)
        duration_minutes = duration_seconds // 60
        duration_seconds_remainder = duration_seconds % 60
        
        # 构建爬取条件描述
        conditions = []
        if section_fids:
            if len(section_fids) == len(SECTION_MAP):
                conditions.append("所有板块")
            else:
                section_names = []
                for fid in section_fids:
                    if fid in SECTION_MAP:
                        section_names.append(SECTION_MAP[fid])
                    else:
                        # 可能传入的是名称
                        section_names.append(str(fid))
                conditions.append(f"板块: {', '.join(section_names)}")
        else:
            conditions.append("所有板块")
        
        conditions.append(f"最大页数: {max_pages}")
        
        if date_mode and date_mode != 'all' and date_value:
            if date_mode == 'day':
                conditions.append(f"日期过滤: {date_value}")
            elif date_mode == 'month':
                conditions.append(f"月份过滤: {date_value}")
            else:
                conditions.append(f"日期过滤: {date_mode}={date_value}")
        
        if dateline:
            seconds = int(dateline)
            if seconds == 86400:
                time_desc = "近1天"
            elif seconds == 604800:
                time_desc = "近1周"
            elif seconds == 2592000:
                time_desc = "近1月"
            elif seconds == 31536000:
                time_desc = "近1年"
            else:
                days = seconds // 86400
                time_desc = f"近{days}天"
            conditions.append(f"时间范围: {time_desc}")
        
        # 统计实际爬取的页数
        actual_pages_crawled = crawl_progress.get('processed_pages', 0)
        
        # 生成页面级别的详细统计
        page_summary = {
            'total_attempted': page_stats['total_pages_attempted'],
            'total_successful': page_stats['total_pages_successful'],
            'total_failed': page_stats['total_pages_failed'],
            'success_rate': round((page_stats['total_pages_successful'] / max(1, page_stats['total_pages_attempted'])) * 100, 1),
            'successful_pages_detail': page_stats['successful_pages'],
            'failed_pages_detail': page_stats['failed_pages'],
            'retry_summary': retry_stats if retry_stats['attempted'] > 0 else None
        }
        
        # 按板块分组的页面统计
        section_page_stats = {}
        for page_info in page_stats['successful_pages']:
            section = page_info['section']
            if section not in section_page_stats:
                section_page_stats[section] = {
                    'successful_pages': [],
                    'total_pages': 0,
                    'total_saved': 0,
                    'total_skipped': 0
                }
            section_page_stats[section]['successful_pages'].append(page_info['page'])
            section_page_stats[section]['total_pages'] += 1
            section_page_stats[section]['total_saved'] += page_info['saved']
            section_page_stats[section]['total_skipped'] += page_info['skipped']
        
        for page_info in page_stats['failed_pages']:
            section = page_info['section']
            if section not in section_page_stats:
                section_page_stats[section] = {
                    'successful_pages': [],
                    'failed_pages': [],
                    'total_pages': 0,
                    'total_saved': 0,
                    'total_skipped': 0
                }
            if 'failed_pages' not in section_page_stats[section]:
                section_page_stats[section]['failed_pages'] = []
            section_page_stats[section]['failed_pages'].append(page_info['page'])
            section_page_stats[section]['total_pages'] += 1
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'unix_time': int(time.time()),
            'task_type_text': "手动爬取任务" if task_type == 'manual' else "自动定时爬取任务",
            'completion_status': completion_status, 
            'exception_reason': exception_reason,
            'engine_set': {
                'mode': config_manager.get('CRAWLER_MODE', 'async'),
                'concurrency': config_manager.get('CRAWLER_MAX_CONCURRENCY', 20) if config_manager.get('CRAWLER_MODE') == 'async' else config_manager.get('CRAWLER_THREAD_COUNT', 10),
                'delay_min': config_manager.get('CRAWLER_ASYNC_DELAY_MIN', 0.5) if config_manager.get('CRAWLER_MODE') == 'async' else config_manager.get('CRAWLER_SYNC_DELAY_MIN', 0.3),
                'delay_max': config_manager.get('CRAWLER_ASYNC_DELAY_MAX', 1.5) if config_manager.get('CRAWLER_MODE') == 'async' else config_manager.get('CRAWLER_SYNC_DELAY_MAX', 0.8),
                'proxy_active': bool(getattr(sht, 'proxies', {}).get('http'))
            },
            'duration': {
                'total_seconds': duration_seconds + duration_minutes * 60,
                'minutes': duration_minutes,
                'seconds': duration_seconds_remainder,
                'formatted': f"{duration_minutes}分{duration_seconds_remainder}秒" if duration_minutes > 0 else f"{duration_seconds_remainder}秒"
            },
            'results': {
                'total_saved': total_saved,
                'total_skipped': total_skipped,
                'total_failed': total_failed,
                'total_processed': total_saved + total_skipped + total_failed,
                'success_rate': round((total_saved / max(1, total_saved + total_skipped + total_failed)) * 100, 1)
            },
            'crawl_conditions': {
                'description': ' | '.join(conditions),
                'target_sections': section_fids or list(SECTION_MAP.keys()),
                'max_pages_per_section': max_pages,
                'actual_pages_crawled': actual_pages_crawled,
                'date_filter': {
                    'mode': date_mode,
                    'value': date_value,
                    'dateline': dateline
                }
            },
            'page_statistics': page_summary,
            'section_page_breakdown': section_page_stats,
            'per_section_results': per_section,
            'performance': {
                'avg_time_per_item': round((duration_seconds + duration_minutes * 60) / max(1, total_saved + total_skipped), 2),
                'items_per_minute': round((total_saved + total_skipped) / max(1, (duration_seconds + duration_minutes * 60) / 60), 1)
            },
            'raw_options': {
                'fids': section_fids,
                'date_mode': date_mode,
                'date_value': date_value,
                'dateline': dateline,
                'max_pages': max_pages
            }
        }
    
        try:
            from configuration import Config
            summary_json_path = Config.get_path('summary_json')
            log_dir = Config.get_path('log_dir')
            os.makedirs(log_dir, exist_ok=True)
            with open(summary_json_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            logger.info(f"📊 详细爬取摘要已保存到 {summary_json_path}")
            logger.info(f"📊 爬取耗时: {summary['duration']['formatted']}, 平均每项: {summary['performance']['avg_time_per_item']}秒")
        except Exception as e:
            logger.error(f"写入爬取摘要失败: {e}")

        # 发送机器人通知
        try:
            success = _send_crawl_report(summary)
            notification_sent = success  # 记录发送状态
        except Exception as e:
            logger.error(f"❌ Telegram通知推送失败: {e}")

        # 通知状态机回到idle状态
        try:
            from crawler_control.cc_control_bridge import get_crawler_control_bridge
            bridge = get_crawler_control_bridge()
            bridge.coordinator.transition_state('idle', {'stopped_at': time.time()})
            logger.info("✅ 已通知状态机：爬虫回到idle状态")
        except Exception as e:
            logger.warning(f"⚠️ 通知状态机失败: {e}")
        
        # 使用统一状态更新，并清理任务特定字段
        final_message = f'爬取完成 - 新增: {total_saved}, 跳过: {total_skipped}'
        if completion_status == "手动终止":
            final_message = f'任务已手动终止 - 新增: {total_saved}'
            
        update_crawl_state({
            'is_crawling': False,
            'is_paused': False,
            'message': final_message,
            # 彻底清理所有进度指标
            'current_section': '',
            'current_page': 0,
            'progress_percent': 100,
            'sections_total': 0,
            'sections_done': 0,
            'current_section_pages': 0,
            'current_section_processed': 0,
            'processed_pages': 0,
            'estimated_total_pages': 0,
            # 区分页码字段一并归零
            'current_page_actual': 0,
            'max_pages_actual': 0,
            'current_page_task': 0,
            'max_pages_task': 0
        })
        sync_crawl_state()
    
        # Task completed successfully (continue to summary and final status)

    except Exception as e:
        import traceback
        logger.error(f"❌ 爬虫任务异常: {traceback.format_exc()}")

        # 即使异常也要发送通知
        try:
            error_msg = f"""❌ *爬虫任务异常终止*
━━━━━━━━━━━━━━
⚠️ 错误信息：{str(e)[:200]}
⏰ 终止时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
💡 建议：检查日志获取详细信息"""
            _send_telegram_message(error_msg, parse_mode='Markdown')
        except Exception as notify_err:
            logger.error(f"发送异常通知失败: {notify_err}")
        
        # 清理状态
        try:
            from crawler_control.cc_control_bridge import get_crawler_control_bridge
            bridge = get_crawler_control_bridge()
            bridge.reset_to_idle()
        except:
            pass
        
        raise  # 重新抛出异常

    finally:
        # finally 确保完成通知一定会发送
        logger.info("🎉 爬虫任务结束 (finally 块执行)")

        # 检查是否已经发送过通知
        if not notification_sent:
            logger.warning("⚠️ 检测到通知未发送，尝试在 finally 块中发送")

            # 无论正常还是异常退出，都发送完成通知
            try:
                from crawler_control.cc_control_bridge import get_crawler_control_bridge

                # 读取保存的摘要文件（如果存在）
                from configuration import Config
                summary_json_path = Config.get_path('summary_json')

                summary_data = {}
                if os.path.exists(summary_json_path):
                    with open(summary_json_path, 'r', encoding='utf-8') as f:
                        summary_data = json.load(f)

                # 如果有摘要数据，发送报告
                if summary_data and summary_data.get('results'):
                    try:
                        success = _send_crawl_report(summary_data)
                        logger.info(f"✅ finally 块中完成通知发送: {success}")
                    except Exception as e:
                        logger.error(f"finally 块发送报告失败: {e}")

                # 强制更新状态为空闲
                try:
                    bridge = get_crawler_control_bridge()
                    bridge.reset_to_idle()
                    logger.info("✅ 已强制重置状态到空闲")
                except Exception as reset_err:
                    logger.warning(f"finally 块重置状态失败: {reset_err}")

            except Exception as notify_err:
                logger.error(f"finally 块发送完成通知失败: {notify_err}")