#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调度工具函数模块 - 提供调度相关的工具函数
"""

import time
import logging
import threading
from configuration import config_manager

logger = logging.getLogger(__name__)

# 控制事件（保留用于向后兼容）
pause_event = threading.Event()
pause_event.set()  # 默认为已设置（即非暂停状态），clear()表示暂停
stop_event = threading.Event()


def pause_crawling_task():
    """暂停爬虫任务"""
    # 使用新的控制桥接器
    try:
        from crawler_control.cc_control_bridge import get_crawler_control_bridge
        bridge = get_crawler_control_bridge()
        signal_id = bridge.send_pause_signal()
        logger.info(f"✅ 已发送暂停信号: {signal_id}")
        return True, "爬虫已暂停"
    except Exception as e:
        logger.error(f"❌ 发送暂停信号失败: {e}")
        # 降级到旧系统
        if not stop_event.is_set():
            pause_event.clear()
            crawl_control = config_manager.get_app_config('CRAWL_CONTROL', {})
            crawl_control['paused'] = True
            config_manager.set_app_config('CRAWL_CONTROL', crawl_control)
            return True, "爬虫已暂停（降级模式）"
        return False, "爬虫未运行或已停止"


def resume_crawling_task():
    """恢复爬虫任务"""
    # 使用新的控制桥接器
    try:
        from crawler_control.cc_control_bridge import get_crawler_control_bridge
        bridge = get_crawler_control_bridge()
        signal_id = bridge.send_resume_signal()
        logger.info(f"✅ 已发送恢复信号: {signal_id}")
        return True, "爬虫已恢复"
    except Exception as e:
        logger.error(f"❌ 发送恢复信号失败: {e}")
        # 降级到旧系统
        if not stop_event.is_set():
            pause_event.set()
            crawl_control = config_manager.get_app_config('CRAWL_CONTROL', {})
            crawl_control['paused'] = False
            config_manager.set_app_config('CRAWL_CONTROL', crawl_control)
            return True, "爬虫已恢复（降级模式）"
        return False, "爬虫未运行"


def stop_crawling_task(force=False):
    """停止爬虫任务"""
    # 使用新的控制桥接器
    try:
        from crawler_control.cc_control_bridge import get_crawler_control_bridge
        bridge = get_crawler_control_bridge()
        signal_id = bridge.send_stop_signal()
        logger.info(f"✅ 已发送停止信号: {signal_id}")
        return True, "正在停止爬虫任务..."
    except Exception as e:
        logger.error(f"❌ 发送停止信号失败: {e}")
        # 降级到旧系统
        stop_event.set()
        pause_event.set()
        crawl_control = config_manager.get_app_config('CRAWL_CONTROL', {})
        crawl_control['stop'] = True
        config_manager.set_app_config('CRAWL_CONTROL', crawl_control)
        return True, "正在停止爬虫任务...（降级模式）"


def check_stop_and_pause():
    """
    集中检查停止和暂停状态

    返回:
        bool: True=需要停止, False=继续执行

    行为:
        - 如果收到停止信号，返回 True
        - 如果收到暂停信号，阻塞等待直到恢复或停止
        - 如果恢复，返回 False 继续执行
        - 如果等待期间收到停止，返回 True
    """
    # 延迟导入避免循环依赖
    from .state import sync_crawl_state

    try:
        # 使用新的控制桥接器检查信号
        from crawler_control.cc_control_bridge import get_crawler_control_bridge
        bridge = get_crawler_control_bridge()

        # 只调用一次，完全信任 bridge 的处理结果
        should_stop = bridge.check_stop_and_pause()

        # 同步状态到缓存（供前端显示）
        sync_crawl_state()

        return should_stop

    except Exception as e:
        logger.error(f"❌ 检查控制信号失败: {e}，降级到旧系统")
        return _check_stop_and_pause_legacy()


def _check_stop_and_pause_legacy():
    """降级到旧系统的检查逻辑（独立函数，便于维护）"""
    from .state import sync_crawl_state

    try:
        from flask import current_app
        crawl_control = current_app.config.get('CRAWL_CONTROL', {})
        crawl_status = current_app.config.get('CRAWL_STATUS', {})
    except Exception:
        try:
            from cache_manager import cache_manager, CacheKeys
            crawl_control = cache_manager.shared_get(CacheKeys.CRAWL_CONTROL) or {}
            crawl_status = cache_manager.shared_get(CacheKeys.CRAWL_STATUS) or {}
        except Exception:
            return False

    # 检查停止信号
    if crawl_control.get('stop'):
        logger.info("⏹️ 收到停止信号（旧系统）")
        crawl_status['message'] = '任务已停止'
        crawl_status['is_crawling'] = False
        sync_crawl_state()
        return True

    # 检查暂停状态
    while crawl_control.get('paused'):
        if crawl_control.get('stop'):
            return True
        crawl_status['message'] = '任务已暂停'
        crawl_status['is_paused'] = True
        sync_crawl_state()
        time.sleep(0.5)

    # 从暂停恢复
    if crawl_status.get('is_paused'):
        crawl_status['is_paused'] = False
        crawl_status['message'] = '正在爬取'
        sync_crawl_state()
        logger.info("▶️ 任务已恢复（旧系统）")

    return False


def sleep_interruptible(seconds):
    """可中断的休眠逻辑"""
    try:
        # 使用新的控制桥接器
        from crawler_control.cc_control_bridge import get_crawler_control_bridge
        bridge = get_crawler_control_bridge()
        
        start_time = time.time()
        while time.time() - start_time < seconds:
            if bridge.should_stop():
                return True
            time.sleep(0.2)
        return False
    except Exception:
        # 降级到旧系统
        try:
            from flask import current_app
            crawl_control = current_app.config.get('CRAWL_CONTROL', {})
        except Exception:
            try:
                from cache_manager import cache_manager, CacheKeys
                crawl_control = cache_manager.shared_get(CacheKeys.CRAWL_CONTROL) or {}
            except Exception:
                time.sleep(seconds)
                return False

        start_time = time.time()
        while time.time() - start_time < seconds:
            if crawl_control.get('stop'):
                return True
            time.sleep(0.2)
        return False