#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫状态机 - 管理爬虫状态转换

负责：
- 定义状态转换规则
- 验证状态转换有效性
- 提供状态转换逻辑
"""

import logging
from typing import Dict, Tuple, Optional, Set

logger = logging.getLogger(__name__)


class CrawlerStateMachine:
    """爬虫状态机"""
    
    # 状态定义
    STATES = {
        'IDLE': 'idle',
        'STARTING': 'starting',
        'RUNNING': 'running',
        'PAUSING': 'pausing',
        'PAUSED': 'paused',
        'RESUMING': 'resuming',
        'STOPPING': 'stopping',
        'ERROR': 'error'
    }
    
    # 状态转换规则
    # 格式: (当前状态, 动作) -> 新状态
    TRANSITIONS = {
        # 从空闲状态开始
        ('idle', 'start'): 'starting',
        ('idle', 'stop'): 'idle',  # 已经停止，保持空闲
        
        # 启动过程
        ('starting', 'started'): 'running',
        ('starting', 'stop'): 'idle',  # 启动时停止，直接回到idle
        ('starting', 'error'): 'error',
        
        # 运行状态
        ('running', 'pause'): 'paused',  # 直接暂停，不经过pausing
        ('running', 'stop'): 'idle',  # 直接停止，不经过stopping
        ('running', 'error'): 'error',
        
        # 暂停过程（保留用于向后兼容）
        ('pausing', 'paused'): 'paused',
        ('pausing', 'stop'): 'idle',  # 暂停时停止，直接回到idle
        ('pausing', 'error'): 'error',
        
        # 暂停状态
        ('paused', 'resume'): 'running',  # 直接恢复，不经过resuming
        ('paused', 'stop'): 'idle',  # 暂停时停止，直接回到idle
        ('paused', 'error'): 'error',
        
        # 恢复过程（保留用于向后兼容）
        ('resuming', 'resumed'): 'running',
        ('resuming', 'stop'): 'idle',  # 恢复时停止，直接回到idle
        ('resuming', 'error'): 'error',
        
        # 停止过程（保留用于向后兼容）
        ('stopping', 'stopped'): 'idle',
        ('stopping', 'error'): 'error',
        
        # 错误状态
        ('error', 'reset'): 'idle',
        ('error', 'stop'): 'idle',  # 错误时停止，直接回到idle
    }
    
    # 信号到动作的映射
    SIGNAL_TO_ACTION = {
        'stop': 'stop',
        'pause': 'pause',
        'resume': 'resume',
        'start': 'start'
    }
    
    def __init__(self):
        """初始化状态机"""
        self.current_state = self.STATES['IDLE']
        logger.info("CrawlerStateMachine initialized")
    
    def can_transition(self, from_state: str, signal_type: str) -> bool:
        """
        检查是否可以进行状态转换
        
        Args:
            from_state: 当前状态
            signal_type: 信号类型
            
        Returns:
            bool: 是否可以转换
        """
        action = self.SIGNAL_TO_ACTION.get(signal_type)
        if not action:
            logger.warning(f"Unknown signal type: {signal_type}")
            return False
        
        transition_key = (from_state, action)
        can_transition = transition_key in self.TRANSITIONS
        
        if not can_transition:
            logger.debug(f"Invalid transition: {from_state} + {action}")
        
        return can_transition
    
    def get_next_state(self, from_state: str, signal_type: str) -> Optional[str]:
        """
        获取下一个状态
        
        Args:
            from_state: 当前状态
            signal_type: 信号类型
            
        Returns:
            str: 下一个状态，如果转换无效则返回None
        """
        action = self.SIGNAL_TO_ACTION.get(signal_type)
        if not action:
            return None
        
        transition_key = (from_state, action)
        next_state = self.TRANSITIONS.get(transition_key)
        
        if next_state:
            logger.debug(f"State transition: {from_state} + {action} -> {next_state}")
        
        return next_state
    
    def transition(self, signal_type: str) -> bool:
        """
        执行状态转换
        
        Args:
            signal_type: 信号类型
            
        Returns:
            bool: 是否成功转换
        """
        if not self.can_transition(self.current_state, signal_type):
            return False
        
        next_state = self.get_next_state(self.current_state, signal_type)
        if next_state:
            old_state = self.current_state
            self.current_state = next_state
            logger.info(f"State machine transition: {old_state} -> {next_state}")
            return True
        
        return False
    
    def get_valid_signals(self, state: str) -> Set[str]:
        """
        获取指定状态下的有效信号
        
        Args:
            state: 状态
            
        Returns:
            Set[str]: 有效信号集合
        """
        valid_signals = set()
        
        for signal_type, action in self.SIGNAL_TO_ACTION.items():
            if (state, action) in self.TRANSITIONS:
                valid_signals.add(signal_type)
        
        return valid_signals
    
    def get_state_info(self, state: str) -> Dict[str, any]:
        """
        获取状态信息
        
        Args:
            state: 状态
            
        Returns:
            dict: 状态信息
        """
        valid_signals = self.get_valid_signals(state)
        
        # 状态属性
        is_active = state in ['running', 'starting', 'pausing', 'resuming']
        is_paused = state == 'paused'
        is_transitioning = state in ['starting', 'pausing', 'resuming', 'stopping']
        is_stable = state in ['idle', 'running', 'paused', 'error']
        
        return {
            'state': state,
            'valid_signals': list(valid_signals),
            'is_active': is_active,
            'is_paused': is_paused,
            'is_transitioning': is_transitioning,
            'is_stable': is_stable
        }
    
    def reset(self):
        """重置状态机到初始状态"""
        self.current_state = self.STATES['IDLE']
        logger.info("State machine reset to idle")
    
    def get_all_states(self) -> Dict[str, str]:
        """获取所有状态定义"""
        return self.STATES.copy()
    
    def get_all_transitions(self) -> Dict[Tuple[str, str], str]:
        """获取所有转换规则"""
        return self.TRANSITIONS.copy()
    
    def validate_state_machine(self) -> bool:
        """
        验证状态机的完整性
        
        Returns:
            bool: 状态机是否有效
        """
        try:
            # 检查所有状态都有出路（除了idle）
            for state in self.STATES.values():
                if state == 'idle':
                    continue
                
                has_exit = False
                for (from_state, action), to_state in self.TRANSITIONS.items():
                    if from_state == state:
                        has_exit = True
                        break
                
                if not has_exit:
                    logger.error(f"State {state} has no exit transitions")
                    return False
            
            # 检查所有转换的目标状态都存在
            for (from_state, action), to_state in self.TRANSITIONS.items():
                if to_state not in self.STATES.values():
                    logger.error(f"Invalid target state: {to_state}")
                    return False
                
                if from_state not in self.STATES.values():
                    logger.error(f"Invalid source state: {from_state}")
                    return False
            
            logger.info("State machine validation passed")
            return True
            
        except Exception as e:
            logger.error(f"State machine validation failed: {e}")
            return False