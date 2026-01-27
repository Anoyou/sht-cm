#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫控制桥接模块 - 连接新的信号系统和现有的爬虫代码

这个模块提供了一个桥接层，使得现有的爬虫代码可以使用新的信号队列和状态协调器，
同时保持向后兼容性。
"""

import logging
import time
from typing import Optional
from .cc_signal_queue import SignalQueueManager
from .cc_state_coordinator import StateCoordinator, ControlAction
from .cc_event_loop import EnhancedEventLoop

logger = logging.getLogger(__name__)


class CrawlerControlBridge:
    """爬虫控制桥接器"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """初始化桥接器"""
        if self._initialized:
            return
        
        # 创建核心组件
        self.queue_manager = SignalQueueManager(redis_client=None)  # 使用内存队列
        self.coordinator = StateCoordinator(self.queue_manager)
        self.event_loop = EnhancedEventLoop(self.coordinator, check_interval=0.5)
        
        self._initialized = True
        logger.info("CrawlerControlBridge initialized")
    
    def send_stop_signal(self) -> str:
        """
        发送停止信号
        
        Returns:
            str: 信号ID
        """
        logger.info("Sending stop signal through bridge")
        signal_id = self.queue_manager.send_signal('stop', {'source': 'api'})
        return signal_id
    
    def send_pause_signal(self) -> str:
        """
        发送暂停信号
        
        Returns:
            str: 信号ID
        """
        logger.info("Sending pause signal through bridge")
        signal_id = self.queue_manager.send_signal('pause', {'source': 'api'})
        return signal_id
    
    def send_resume_signal(self) -> str:
        """
        发送继续信号
        
        Returns:
            str: 信号ID
        """
        logger.info("Sending resume signal through bridge")
        signal_id = self.queue_manager.send_signal('resume', {'source': 'api'})
        return signal_id
    
    def check_control_signals(self) -> ControlAction:
        """
        检查控制信号
        
        Returns:
            ControlAction: 需要执行的控制动作
        """
        return self.event_loop.check_and_process_signals()
    
    def should_stop(self) -> bool:
        """
        检查是否应该停止
        
        Returns:
            bool: 是否应该停止
        """
        # 不要在这里处理信号，只检查当前状态
        # 信号处理应该在 check_stop_and_pause() 中统一进行
        current_state = self.coordinator.get_current_state()
        return current_state.current_state == 'idle' and current_state.previous_state in ['running', 'paused', 'stopping']
    
    def should_pause(self) -> bool:
        """
        检查是否应该暂停
        
        Returns:
            bool: 是否应该暂停
        """
        # 不要在这里处理信号，只检查当前状态
        # 信号处理应该在 check_stop_and_pause() 中统一进行
        current_state = self.coordinator.get_current_state()
        return current_state.is_paused
    
    def wait_if_paused(self) -> bool:
        """
        如果暂停则等待，直到恢复或停止

        Returns:
            bool: 是否收到停止信号（True表示停止，False表示恢复）
        """
        logger.info("⏸️ 任务已暂停，等待恢复...")

        while True:
            time.sleep(0.5)

            # 修复：每次循环只检查一次信号
            action = self.check_control_signals()
            current_state = self.coordinator.get_current_state()

            if action.action == 'stop':
                logger.info("⏹️ 暂停期间收到停止信号")
                return True
            elif action.action == 'resume':
                logger.info("▶️ 任务已恢复")
                return False
            elif not current_state.is_paused:
                # 修复：检查状态是否为idle（停止）还是running（恢复）
                if current_state.current_state == 'idle':
                    # 状态已经是idle了，说明被停止了（可能被其他方式停止）
                    logger.info("⏹️ 检测到状态已变为idle，任务已停止")
                    return True
                else:
                    # 状态已经不是暂停了（可能被其他方式恢复）
                    logger.info("▶️ 任务已恢复（状态变更）")
                    return False
    
    def check_stop_and_pause(self) -> bool:
        """
        集中检查停止和暂停状态（兼容旧接口）
        
        Returns:
            bool: 是否应该停止（True表示停止）
        """
        # 修复：只调用一次 check_control_signals，避免信号被重复消费
        action = self.check_control_signals()
        
        # 记录信号处理结果
        if action.action != 'continue':
            logger.info(f"🎯 收到控制信号: {action.action}")
        
        if action.action == 'stop':
            logger.info("⏹️ 收到停止信号，终止爬取")
            return True
        elif action.action == 'pause':
            logger.info("⏸️ 收到暂停信号")
            # 等待恢复或停止
            return self.wait_if_paused()
        
        # 如果当前状态是暂停（可能是之前暂停的），继续等待
        current_state = self.coordinator.get_current_state()
        if current_state.is_paused:
            logger.info("⏸️ 当前处于暂停状态，等待恢复...")
            return self.wait_if_paused()
        
        return False
    
    def get_current_state(self, force_reload: bool = False) -> dict:
        """
        获取当前状态信息

        Args:
            force_reload: 是否强制从文件重新加载（多进程安全）

        Returns:
            dict: 状态信息
        """
        state = self.coordinator.get_current_state(force_reload=force_reload)
        return {
            'current_state': state.current_state,
            'is_crawling': state.is_crawling,
            'is_paused': state.is_paused,
            'progress': state.progress,
            'version': state.version
        }
    
    def start_crawling(self):
        """标记爬虫开始"""
        self.coordinator.transition_state('running', {'started_at': time.time()})
        logger.info("Crawler started")
    
    def stop_crawling(self):
        """标记爬虫停止"""
        self.coordinator.transition_state('idle', {'stopped_at': time.time()})
        logger.info("Crawler stopped")
    
    def reset_to_idle(self):
        """重置到空闲状态（清除所有标志）"""
        self.coordinator.transition_state('idle', {'reset_at': time.time()})
        logger.info("Crawler reset to idle state")
    
    def update_progress(self, progress_data: dict):
        """更新进度信息"""
        self.coordinator.update_progress(progress_data)
    
    def reset(self):
        """重置状态"""
        self.queue_manager.clear_signals()
        self.coordinator.reset_state()
        logger.info("Bridge reset")
    
    def get_performance_stats(self) -> dict:
        """获取性能统计"""
        return self.event_loop.get_performance_stats()


# 全局单例实例
_bridge_instance: Optional[CrawlerControlBridge] = None


def get_crawler_control_bridge() -> CrawlerControlBridge:
    """
    获取爬虫控制桥接器的全局实例
    
    Returns:
        CrawlerControlBridge: 桥接器实例
    """
    global _bridge_instance
    if _bridge_instance is None:
        _bridge_instance = CrawlerControlBridge()
    return _bridge_instance


# 兼容旧接口的函数
def check_stop_and_pause_new() -> bool:
    """
    新的停止和暂停检查函数（使用新架构）
    
    Returns:
        bool: 是否应该停止
    """
    bridge = get_crawler_control_bridge()
    return bridge.check_stop_and_pause()


def send_stop_signal_new() -> str:
    """
    发送停止信号（新架构）
    
    Returns:
        str: 信号ID
    """
    bridge = get_crawler_control_bridge()
    return bridge.send_stop_signal()


def send_pause_signal_new() -> str:
    """
    发送暂停信号（新架构）
    
    Returns:
        str: 信号ID
    """
    bridge = get_crawler_control_bridge()
    return bridge.send_pause_signal()


def send_resume_signal_new() -> str:
    """
    发送继续信号（新架构）
    
    Returns:
        str: 信号ID
    """
    bridge = get_crawler_control_bridge()
    return bridge.send_resume_signal()
