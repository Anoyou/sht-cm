#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强事件循环 - 在爬虫执行过程中定期检查控制信号

负责：
- 定期检查控制信号
- 处理信号响应逻辑
- 管理检查频率
- 执行控制动作
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Callable, Any
from .cc_state_coordinator import StateCoordinator, ControlAction
from .cc_resource_cleanup import get_resource_cleanup_manager

logger = logging.getLogger(__name__)


class EnhancedEventLoop:
    """增强事件循环"""
    
    def __init__(self, state_coordinator: StateCoordinator, check_interval: float = 0.5):
        """
        初始化增强事件循环
        
        Args:
            state_coordinator: 状态协调器
            check_interval: 信号检查间隔（秒），默认500ms
        """
        self.coordinator = state_coordinator
        self.check_interval = check_interval
        self.last_check = 0.0
        self.batch_check_counter = 0
        self.batch_check_interval = 10  # 每处理10个项目检查一次信号
        
        # 资源清理管理器
        self.resource_manager = get_resource_cleanup_manager()
        
        # 性能统计
        self.total_checks = 0
        self.signal_response_times = []
        
        logger.info(f"EnhancedEventLoop initialized with check_interval={check_interval}s")
    
    def should_check_signals(self) -> bool:
        """
        判断是否需要检查信号
        
        Returns:
            bool: 是否需要检查
        """
        current_time = time.time()
        time_since_last_check = current_time - self.last_check
        
        # 基于时间间隔的检查
        if time_since_last_check >= self.check_interval:
            return True
        
        return False
    
    def should_check_signals_batch(self) -> bool:
        """
        判断是否需要在批量处理中检查信号
        
        Returns:
            bool: 是否需要检查
        """
        self.batch_check_counter += 1
        
        # 每处理指定数量的项目检查一次
        if self.batch_check_counter >= self.batch_check_interval:
            self.batch_check_counter = 0
            return True
        
        return False
    
    def check_and_process_signals(self) -> ControlAction:
        """
        检查并处理控制信号
        
        Returns:
            ControlAction: 需要执行的控制动作
        """
        start_time = time.time()
        
        try:
            # 更新最后检查时间
            self.last_check = start_time
            self.total_checks += 1
            
            # 通过状态协调器检查和处理信号
            action = self.coordinator.check_and_process_signals()
            
            # 记录响应时间
            response_time = (time.time() - start_time) * 1000  # 转换为毫秒
            self.signal_response_times.append(response_time)
            
            # 保持最近100次的响应时间记录
            if len(self.signal_response_times) > 100:
                self.signal_response_times = self.signal_response_times[-100:]
            
            # 记录信号处理
            if action.action != 'continue':
                logger.info(f"Signal processed: {action.action}, immediate={action.immediate}, "
                          f"cleanup_required={action.cleanup_required}, response_time={response_time:.2f}ms")
            
            return action
            
        except Exception as e:
            logger.error(f"Error in check_and_process_signals: {e}")
            return ControlAction(
                action='continue',
                immediate=False,
                cleanup_required=False,
                metadata={'error': str(e)}
            )
    
    def handle_stop_signal(self, metadata: dict = None) -> bool:
        """
        处理停止信号
        
        Args:
            metadata: 信号元数据
            
        Returns:
            bool: 是否成功处理
        """
        try:
            logger.info("Handling stop signal - initiating crawler shutdown")
            
            # 更新状态为stopping
            self.coordinator.transition_state('stopping', metadata or {})
            
            # 执行资源清理（这里可以添加具体的清理逻辑）
            self._cleanup_resources()
            
            # 更新状态为idle
            self.coordinator.transition_state('idle', {'stopped_at': datetime.now().isoformat()})
            
            logger.info("Stop signal handled successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error handling stop signal: {e}")
            self.coordinator.transition_state('error', {'error': str(e)})
            return False
    
    def handle_pause_signal(self, metadata: dict = None) -> bool:
        """
        处理暂停信号
        
        Args:
            metadata: 信号元数据
            
        Returns:
            bool: 是否成功处理
        """
        try:
            logger.info("Handling pause signal - pausing crawler")
            
            # 更新状态为pausing
            self.coordinator.transition_state('pausing', metadata or {})
            
            # 等待当前批次完成（这里可以添加具体的暂停逻辑）
            self._wait_for_current_batch()
            
            # 更新状态为paused
            self.coordinator.transition_state('paused', {'paused_at': datetime.now().isoformat()})
            
            logger.info("Pause signal handled successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error handling pause signal: {e}")
            self.coordinator.transition_state('error', {'error': str(e)})
            return False
    
    def handle_resume_signal(self, metadata: dict = None) -> bool:
        """
        处理继续信号
        
        Args:
            metadata: 信号元数据
            
        Returns:
            bool: 是否成功处理
        """
        try:
            logger.info("Handling resume signal - resuming crawler")
            
            # 更新状态为resuming
            self.coordinator.transition_state('resuming', metadata or {})
            
            # 恢复执行（这里可以添加具体的恢复逻辑）
            self._resume_execution()
            
            # 更新状态为running
            self.coordinator.transition_state('running', {'resumed_at': datetime.now().isoformat()})
            
            logger.info("Resume signal handled successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error handling resume signal: {e}")
            self.coordinator.transition_state('error', {'error': str(e)})
            return False
    
    def _cleanup_resources(self):
        """
        清理资源（停止时调用）
        
        执行完整的资源清理流程：
        - 关闭网络连接（不影响远程服务）
        - 关闭数据库连接并提交事务（不删除数据）
        - 关闭文件句柄（不删除持久化文件）
        - 删除临时文件（缓存、中间结果）
        - 释放内存缓存
        - 停止后台线程
        
        重要：已保存到数据库的爬取数据不会被清理
        """
        logger.info("Cleaning up resources...")
        
        try:
            # 使用资源清理管理器清理所有资源
            results = self.resource_manager.cleanup_all(force=True, critical_only=False)
            
            logger.info(f"Resource cleanup completed: {results['success']}/{results['total']} resources cleaned, "
                       f"{results['failed']} failed, duration: {results['duration']:.2f}s")
            
            # 如果有清理失败，记录警告
            if results['failed'] > 0:
                logger.warning(f"{results['failed']} resources failed to cleanup properly")
            
            return results
            
        except Exception as e:
            logger.error(f"Error during resource cleanup: {e}")
            # 即使清理失败，也尝试强制清理
            try:
                return self.resource_manager.force_cleanup_all()
            except Exception as force_error:
                logger.error(f"Force cleanup also failed: {force_error}")
                return {'total': 0, 'success': 0, 'failed': 0, 'duration': 0}
    
    def _wait_for_current_batch(self):
        """
        等待当前批次完成（暂停时调用）
        
        在暂停时，可以选择清理非关键资源以释放内存
        """
        logger.info("Waiting for current batch to complete...")
        
        # 这里可以添加具体的批次等待逻辑
        
        # 对于长时间暂停，清理非关键资源
        try:
            cleaned_count = self.resource_manager.cleanup_non_critical()
            if cleaned_count > 0:
                logger.info(f"Cleaned {cleaned_count} non-critical resources during pause")
        except Exception as e:
            logger.warning(f"Failed to cleanup non-critical resources: {e}")
    
    def _resume_execution(self):
        """恢复执行（继续时调用）"""
        logger.info("Resuming execution...")
        # 这里可以添加具体的恢复逻辑
        pass
    
    def execute_control_action(self, action: ControlAction) -> bool:
        """
        执行控制动作
        
        Args:
            action: 控制动作
            
        Returns:
            bool: 是否成功执行
        """
        if action.action == 'stop':
            return self.handle_stop_signal(action.metadata)
        elif action.action == 'pause':
            return self.handle_pause_signal(action.metadata)
        elif action.action == 'resume':
            return self.handle_resume_signal(action.metadata)
        elif action.action == 'continue':
            # 继续执行，无需特殊处理
            return True
        else:
            logger.warning(f"Unknown control action: {action.action}")
            return False
    
    def crawler_loop_with_signal_check(self, 
                                     work_function: Callable[[], Any],
                                     items_to_process: list,
                                     progress_callback: Optional[Callable[[int, int], None]] = None) -> bool:
        """
        带信号检查的爬虫循环
        
        Args:
            work_function: 工作函数，处理单个项目
            items_to_process: 要处理的项目列表
            progress_callback: 进度回调函数
            
        Returns:
            bool: 是否正常完成（True）或被中断（False）
        """
        total_items = len(items_to_process)
        processed_count = 0
        
        logger.info(f"Starting crawler loop with {total_items} items to process")
        
        try:
            for i, item in enumerate(items_to_process):
                # 检查信号（基于时间间隔）
                if self.should_check_signals():
                    action = self.check_and_process_signals()
                    
                    if action.action != 'continue':
                        logger.info(f"Control action received: {action.action}")
                        
                        # 执行控制动作
                        success = self.execute_control_action(action)
                        
                        if action.action == 'stop':
                            logger.info("Crawler loop stopped by user")
                            return False
                        elif action.action == 'pause':
                            # 暂停循环，等待恢复信号
                            if success:
                                return self._wait_for_resume(items_to_process[i:], progress_callback, processed_count)
                            else:
                                return False
                
                # 检查信号（基于批量处理）
                if self.should_check_signals_batch():
                    action = self.check_and_process_signals()
                    if action.action != 'continue':
                        success = self.execute_control_action(action)
                        if action.action == 'stop':
                            return False
                        elif action.action == 'pause' and success:
                            return self._wait_for_resume(items_to_process[i:], progress_callback, processed_count)
                
                # 执行实际工作
                try:
                    work_function(item)
                    processed_count += 1
                    
                    # 更新进度
                    if progress_callback:
                        progress_callback(processed_count, total_items)
                    
                    # 更新状态协调器的进度信息
                    self.coordinator.update_progress({
                        'processed': processed_count,
                        'total': total_items,
                        'percentage': (processed_count / total_items) * 100
                    })
                    
                except Exception as e:
                    logger.error(f"Error processing item {i}: {e}")
                    # 继续处理下一个项目，不中断整个循环
                    continue
            
            logger.info(f"Crawler loop completed successfully, processed {processed_count}/{total_items} items")
            return True
            
        except Exception as e:
            logger.error(f"Fatal error in crawler loop: {e}")
            self.coordinator.transition_state('error', {'error': str(e)})
            return False
    
    def _wait_for_resume(self, 
                        remaining_items: list,
                        progress_callback: Optional[Callable[[int, int], None]],
                        processed_count: int) -> bool:
        """
        等待恢复信号并继续处理剩余项目
        
        Args:
            remaining_items: 剩余要处理的项目
            progress_callback: 进度回调函数
            processed_count: 已处理的项目数量
            
        Returns:
            bool: 是否成功恢复并完成
        """
        logger.info("Waiting for resume signal...")
        
        # 等待恢复信号
        while True:
            time.sleep(0.1)  # 短暂休眠避免CPU占用过高
            
            action = self.check_and_process_signals()
            
            if action.action == 'resume':
                logger.info("Resume signal received, continuing crawler loop")
                success = self.execute_control_action(action)
                if success:
                    # 递归调用处理剩余项目
                    return self.crawler_loop_with_signal_check(
                        lambda item: None,  # 这里需要传入实际的工作函数
                        remaining_items,
                        progress_callback
                    )
                else:
                    return False
            elif action.action == 'stop':
                logger.info("Stop signal received while paused")
                self.execute_control_action(action)
                return False
    
    def get_performance_stats(self) -> dict:
        """
        获取性能统计信息
        
        Returns:
            dict: 性能统计数据
        """
        if not self.signal_response_times:
            return {
                'total_checks': self.total_checks,
                'avg_response_time_ms': 0,
                'max_response_time_ms': 0,
                'min_response_time_ms': 0
            }
        
        return {
            'total_checks': self.total_checks,
            'avg_response_time_ms': sum(self.signal_response_times) / len(self.signal_response_times),
            'max_response_time_ms': max(self.signal_response_times),
            'min_response_time_ms': min(self.signal_response_times),
            'check_interval_s': self.check_interval,
            'batch_check_interval': self.batch_check_interval
        }
    
    def reset_stats(self):
        """重置性能统计"""
        self.total_checks = 0
        self.signal_response_times = []
        self.last_check = 0.0
        self.batch_check_counter = 0
        logger.info("Performance stats reset")
    
    def set_check_interval(self, interval: float):
        """
        设置信号检查间隔
        
        Args:
            interval: 新的检查间隔（秒）
        """
        old_interval = self.check_interval
        self.check_interval = interval
        logger.info(f"Check interval changed from {old_interval}s to {interval}s")
    
    def set_batch_check_interval(self, interval: int):
        """
        设置批量检查间隔
        
        Args:
            interval: 新的批量检查间隔（项目数量）
        """
        old_interval = self.batch_check_interval
        self.batch_check_interval = interval
        self.batch_check_counter = 0  # 重置计数器
        logger.info(f"Batch check interval changed from {old_interval} to {interval}")
    
    def register_resource(self, resource_id: str, resource_type, name: str, 
                         cleanup_func: Callable, critical: bool = True, 
                         metadata: dict = None) -> bool:
        """
        注册需要清理的资源
        
        Args:
            resource_id: 资源唯一标识
            resource_type: 资源类型（ResourceType枚举）
            name: 资源名称
            cleanup_func: 清理函数
            critical: 是否为关键资源
            metadata: 资源元数据
            
        Returns:
            bool: 是否成功注册
        """
        return self.resource_manager.register_resource(
            resource_id=resource_id,
            resource_type=resource_type,
            name=name,
            cleanup_func=cleanup_func,
            critical=critical,
            metadata=metadata
        )
    
    def unregister_resource(self, resource_id: str) -> bool:
        """
        取消注册资源（资源已被正常清理）
        
        Args:
            resource_id: 资源ID
            
        Returns:
            bool: 是否成功取消注册
        """
        return self.resource_manager.unregister_resource(resource_id)
    
    def get_active_resource_count(self) -> int:
        """
        获取活动资源数量
        
        Returns:
            int: 活动资源数量
        """
        return len(self.resource_manager.get_active_resources())