#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
状态协调器 - 协调状态变化和冲突解决

负责：
- 处理控制信号
- 管理状态转换
- 解决信号冲突
- 维护状态一致性
- 状态持久化和恢复
- 容错和降级处理
"""

import logging
import json
import os
import threading
import time
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, List
from pathlib import Path
from .cc_signal_queue import Signal, SignalQueueManager
from .cc_state_machine import CrawlerStateMachine
from .cc_fault_tolerance import get_fault_tolerance_manager, FallbackStrategy

logger = logging.getLogger(__name__)


@dataclass
class CrawlerState:
    """爬虫状态数据模型"""
    current_state: str
    previous_state: str
    transition_time: datetime
    metadata: dict
    version: int
    is_crawling: bool
    is_paused: bool
    progress: dict
    
    def to_dict(self) -> dict:
        """转换为字典格式"""
        data = asdict(self)
        data['transition_time'] = self.transition_time.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: dict) -> 'CrawlerState':
        """从字典创建CrawlerState对象"""
        data = data.copy()

        # 处理字段名兼容性：旧文件使用 'state'，新文件使用 'current_state'
        if 'state' in data and 'current_state' not in data:
            data['current_state'] = data.pop('state')

        # 处理 transition_time 字段的反序列化，支持旧格式
        if 'transition_time' in data:
            if isinstance(data['transition_time'], str):
                try:
                    data['transition_time'] = datetime.fromisoformat(data['transition_time'])
                except (ValueError, TypeError):
                    data['transition_time'] = datetime.now()
        else:
            # 旧版本文件没有这个字段，使用当前时间
            data['transition_time'] = datetime.now()

        return cls(**data)


@dataclass
class ControlAction:
    """控制动作数据模型"""
    action: str  # 'continue', 'pause', 'stop', 'none'
    immediate: bool  # 是否需要立即执行
    cleanup_required: bool  # 是否需要资源清理
    metadata: dict


class StateCoordinator:
    """状态协调器"""
    
    # 默认持久化文件路径 - 使用配置管理器
    @staticmethod
    def _get_default_state_file():
        """获取默认状态文件路径"""
        from configuration import Config
        return Config.get_path('crawler_state')
    
    DEFAULT_STATE_FILE = None  # 动态设置
    
    def __init__(self, signal_queue: SignalQueueManager, shared_state_manager=None, 
                 persistence_file: str = None, enable_persistence: bool = True):
        """
        初始化状态协调器
        
        Args:
            signal_queue: 信号队列管理器
            shared_state_manager: 共享状态管理器（可选）
            persistence_file: 持久化文件路径（可选）
            enable_persistence: 是否启用持久化（默认True）
        """
        self.signal_queue = signal_queue
        self.shared_state = shared_state_manager
        self.state_machine = CrawlerStateMachine()
        self.enable_persistence = enable_persistence
        
        # 设置持久化文件路径 - 优先级：传入参数 > 配置值
        if persistence_file:
            self.persistence_file = persistence_file
        else:
            # 使用配置管理器获取路径
            from configuration import Config
            self.persistence_file = Config.get_path('crawler_state')
        
        # 添加文件锁，防止并发读写
        self._file_lock = threading.Lock()

        # 状态通知控制（避免过早/重复通知）
        self._last_notify_state = None
        self._last_notify_time = 0.0
        self._notify_min_interval = 2.0
        self._notify_skip_states = {'starting', 'pausing', 'resuming', 'stopping'}

        # 容错管理器
        self.fault_tolerance = get_fault_tolerance_manager()
        self._register_fallback_handlers()

        # 尝试从持久化存储恢复状态
        if self.enable_persistence:
            restored_state = self._restore_state()
            if restored_state:
                self._current_state = restored_state
                logger.info(f"State restored from persistence: {self.persistence_file}")
            else:
                self._current_state = self._create_initial_state()
                logger.info("No persisted state found, using initial state")
        else:
            self._current_state = self._create_initial_state()
            logger.info("Persistence disabled, using initial state")

        logger.info("StateCoordinator initialized")
    
    def _create_initial_state(self) -> CrawlerState:
        """创建初始状态"""
        return CrawlerState(
            current_state='idle',
            previous_state='idle',
            transition_time=datetime.now(),
            metadata={},
            version=1,
            is_crawling=False,
            is_paused=False,
            progress={}
        )
    
    def process_control_signal(self, signal: Signal) -> bool:
        """
        处理控制信号
        
        Args:
            signal: 控制信号
            
        Returns:
            bool: 是否成功处理
        """
        try:
            logger.info(f"Processing signal: {signal.type} (ID: {signal.id})")
            
            # 检查状态转换是否有效
            if not self.state_machine.can_transition(
                self._current_state.current_state, 
                signal.type
            ):
                logger.warning(
                    f"Invalid transition: {self._current_state.current_state} -> {signal.type}"
                )
                return False
            
            # 执行状态转换
            success = self._execute_transition(signal)
            
            if success:
                # 确认信号已处理
                self.signal_queue.acknowledge_signal(signal.id)
                logger.info(f"Signal processed successfully: {signal.id}")
            else:
                logger.error(f"Failed to process signal: {signal.id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing signal {signal.id}: {e}")
            return False
    
    def _execute_transition(self, signal: Signal) -> bool:
        """执行状态转换"""
        old_state = self._current_state.current_state
        
        # 根据信号类型确定新状态
        new_state = self.state_machine.get_next_state(old_state, signal.type)
        if not new_state:
            return False
        
        # 更新状态
        self._current_state.previous_state = old_state
        self._current_state.current_state = new_state
        self._current_state.transition_time = datetime.now()
        self._current_state.version += 1
        
        # 更新状态标志
        self._update_state_flags(new_state, signal)
        
        # 持久化状态
        if self.enable_persistence:
            self._persist_state()
        
        # 同步到共享状态
        if self.shared_state:
            try:
                self.shared_state.update_state(self._current_state.to_dict())
            except Exception as e:
                logger.error(f"Failed to sync state to shared storage: {e}")
        
        self._notify_state_change(old_state, new_state, signal.payload)
        logger.info(f"State transition: {old_state} -> {new_state}")
        return True
    
    def _update_state_flags(self, new_state: str, signal: Signal):
        """更新状态标志"""
        if new_state == 'running':
            self._current_state.is_crawling = True
            self._current_state.is_paused = False
        elif new_state == 'paused':
            self._current_state.is_crawling = False
            self._current_state.is_paused = True
        elif new_state == 'idle':
            self._current_state.is_crawling = False
            self._current_state.is_paused = False
        
        # 更新元数据
        self._current_state.metadata.update(signal.payload)
    
    def get_current_state(self, force_reload: bool = False) -> CrawlerState:
        """
        获取当前状态

        Args:
            force_reload: 是否强制从文件重新加载状态（多进程安全）

        Returns:
            CrawlerState: 当前状态
        """
        # 修复：多进程环境下，从文件读取最新状态
        if force_reload and self.enable_persistence:
            try:
                self._load_state_from_file()
            except Exception as e:
                logger.debug(f"从文件加载状态失败: {e}")

        return self._current_state

    def _load_state_from_file(self):
        """从持久化文件加载状态（带文件锁）"""
        if not os.path.exists(self.persistence_file):
            return

        with self._file_lock:  # 使用文件锁防止并发读写
            try:
                with open(self.persistence_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # 提取 state 字段（文件格式为 {"state": {...}, "persisted_at": ..., "version": ...}）
                if 'state' in data:
                    state_data = data['state']
                else:
                    # 兼容旧格式（扁平结构）
                    state_data = data

                # 更新内存状态
                self._current_state = CrawlerState.from_dict(state_data)
                # 频繁调用，不打印日志避免刷屏

            except Exception as e:
                logger.error(f"加载状态文件失败: {e}")
    
    def transition_state(self, new_state: str, metadata: dict = None):
        """
        直接状态转换（用于内部状态更新）
        
        Args:
            new_state: 新状态
            metadata: 元数据
        """
        if metadata is None:
            metadata = {}
        
        old_state = self._current_state.current_state
        
        self._current_state.previous_state = old_state
        self._current_state.current_state = new_state
        self._current_state.transition_time = datetime.now()
        self._current_state.version += 1
        self._current_state.metadata.update(metadata)
        
        # 更新状态标志
        if new_state == 'running':
            self._current_state.is_crawling = True
            self._current_state.is_paused = False
        elif new_state == 'paused':
            self._current_state.is_crawling = False
            self._current_state.is_paused = True
        elif new_state == 'idle':
            self._current_state.is_crawling = False
            self._current_state.is_paused = False
        
        # 持久化状态
        if self.enable_persistence:
            self._persist_state()
        
        self._notify_state_change(old_state, new_state, metadata)
        logger.info(f"Direct state transition: {old_state} -> {new_state}")
    
    def _notify_state_change(self, old_state: str, new_state: str, metadata: dict):
        if old_state == new_state:
            return
        if new_state in self._notify_skip_states:
            return
        now = time.time()
        if self._last_notify_state == new_state and (now - self._last_notify_time) < self._notify_min_interval:
            return
        self._last_notify_state = new_state
        self._last_notify_time = now
        try:
            from scheduler.notifier import _send_telegram_message, render_message_template
            state_labels = {
                'idle': '空闲',
                'starting': '启动中',
                'running': '爬取中',
                'pausing': '暂停中',
                'paused': '已暂停',
                'resuming': '恢复中',
                'stopping': '停止中',
                'error': '异常'
            }
            old_label = state_labels.get(old_state, old_state)
            new_label = state_labels.get(new_state, new_state)
            reason = ''
            if isinstance(metadata, dict) and metadata.get('source'):
                reason = f"来源: {metadata.get('source')}"

            msg, parse_mode = render_message_template('state_change', {
                'old_state': old_label,
                'new_state': new_label,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'reason': reason
            })
            if not msg:
                msg = f"🔔 *爬虫状态变更*\n━━━━━━━━━━━━━━\n{old_label} → {new_label}\n⏰ 时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{reason}".rstrip()
                parse_mode = 'Markdown'
            _send_telegram_message(msg, parse_mode=parse_mode)
        except Exception as e:
            logger.debug(f"状态变更通知失败: {e}")

    def resolve_conflicts(self, signals: List[Signal]) -> Optional[Signal]:
        """
        解决信号冲突，返回优先级最高的信号
        
        Args:
            signals: 信号列表
            
        Returns:
            Signal: 优先级最高的信号，如果没有有效信号则返回None
        """
        if not signals:
            return None
        
        # 过滤出有效的信号（可以执行状态转换的）
        valid_signals = []
        current_state = self._current_state.current_state
        
        for signal in signals:
            if self.state_machine.can_transition(current_state, signal.type):
                valid_signals.append(signal)
        
        if not valid_signals:
            logger.warning("No valid signals found for current state")
            return None
        
        # 按优先级排序（优先级数字越小越高）
        valid_signals.sort(key=lambda s: (s.priority, s.timestamp))
        
        selected_signal = valid_signals[0]
        logger.info(f"Signal conflict resolved, selected: {selected_signal.type}")
        
        return selected_signal
    
    def check_and_process_signals(self) -> ControlAction:
        """
        检查并处理控制信号
        
        Returns:
            ControlAction: 需要执行的控制动作
        """
        try:
            # 获取待处理信号
            pending_signals = self.signal_queue.get_pending_signals()
            
            if not pending_signals:
                return ControlAction(
                    action='continue',
                    immediate=False,
                    cleanup_required=False,
                    metadata={}
                )
            
            # 解决信号冲突
            signal_to_process = self.resolve_conflicts(pending_signals)
            
            if not signal_to_process:
                return ControlAction(
                    action='continue',
                    immediate=False,
                    cleanup_required=False,
                    metadata={}
                )
            
            # 处理信号
            success = self.process_control_signal(signal_to_process)
            
            if success:
                # 根据信号类型返回控制动作
                return self._signal_to_action(signal_to_process)
            else:
                return ControlAction(
                    action='continue',
                    immediate=False,
                    cleanup_required=False,
                    metadata={'error': 'Signal processing failed'}
                )
                
        except Exception as e:
            logger.error(f"Error in check_and_process_signals: {e}")
            return ControlAction(
                action='continue',
                immediate=False,
                cleanup_required=False,
                metadata={'error': str(e)}
            )
    
    def _signal_to_action(self, signal: Signal) -> ControlAction:
        """将信号转换为控制动作"""
        if signal.type == 'stop':
            return ControlAction(
                action='stop',
                immediate=True,
                cleanup_required=True,
                metadata=signal.payload
            )
        elif signal.type == 'pause':
            return ControlAction(
                action='pause',
                immediate=False,  # 等待当前批次完成
                cleanup_required=False,
                metadata=signal.payload
            )
        elif signal.type == 'resume':
            return ControlAction(
                action='resume',
                immediate=True,
                cleanup_required=False,
                metadata=signal.payload
            )
        else:
            return ControlAction(
                action='continue',
                immediate=False,
                cleanup_required=False,
                metadata=signal.payload
            )
    
    def update_progress(self, progress_data: dict):
        """更新进度信息"""
        self._current_state.progress.update(progress_data)
        self._current_state.version += 1
        
        # 持久化状态
        if self.enable_persistence:
            self._persist_state()
        
        # 同步到共享状态
        if self.shared_state:
            try:
                self.shared_state.update_state(self._current_state.to_dict())
            except Exception as e:
                logger.error(f"Failed to sync progress to shared storage: {e}")
    
    def save_page_loop_state(self, section_name: str, page_idx: int, progress_idx: int, 
                            pages_to_crawl_list: list, current_offset: int = 0):
        """
        保存页面循环状态，用于暂停/恢复
        
        Args:
            section_name: 当前分类名称
            page_idx: 当前页码
            progress_idx: 当前进度索引（1-based）
            pages_to_crawl_list: 完整的页码列表
            current_offset: 当前在列表中的偏移量（0-based）
        """
        loop_state = {
            'section_name': section_name,
            'current_page': page_idx,
            'progress_idx': progress_idx,
            'pages_to_crawl': pages_to_crawl_list,
            'current_offset': current_offset,
            'saved_at': datetime.now().isoformat()
        }
        
        self._current_state.progress['page_loop_state'] = loop_state
        self._current_state.version += 1
        
        # 持久化状态
        if self.enable_persistence:
            self._persist_state()
        
        logger.debug(f"Page loop state saved: section={section_name}, page={page_idx}, offset={current_offset}")
    
    def get_page_loop_state(self) -> Optional[dict]:
        """
        获取保存的页面循环状态
        
        Returns:
            dict: 页面循环状态，如果不存在则返回None
        """
        return self._current_state.progress.get('page_loop_state')
    
    def get_state_version(self) -> int:
        """获取状态版本号"""
        return self._current_state.version
    
    def reset_state(self):
        """重置状态到初始状态"""
        self._current_state = self._create_initial_state()
        
        # 持久化重置后的状态
        if self.enable_persistence:
            self._persist_state()
        
        logger.info("State reset to initial state")
    
    def _persist_state(self) -> bool:
        """
        持久化当前状态到文件（带文件锁）

        Returns:
            bool: 是否成功持久化
        """
        if not self.enable_persistence:
            return False

        with self._file_lock:  # 使用文件锁防止并发写入
            try:
                # 确保目录存在
                state_file_path = Path(self.persistence_file)
                state_file_path.parent.mkdir(parents=True, exist_ok=True)

                # 序列化状态
                state_data = self._current_state.to_dict()

                # 添加持久化元数据
                persistence_data = {
                    'state': state_data,
                    'persisted_at': datetime.now().isoformat(),
                    'version': '1.0'
                }

                # 写入文件（原子操作）
                temp_file = f"{self.persistence_file}.tmp"
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(persistence_data, f, indent=2, ensure_ascii=False)

                # 原子替换
                os.replace(temp_file, self.persistence_file)

                logger.debug(f"State persisted to {self.persistence_file}")
                return True

            except Exception as e:
                logger.error(f"Failed to persist state: {e}")
                return False
    
    def _restore_state(self) -> Optional[CrawlerState]:
        """
        从持久化文件恢复状态
        
        Returns:
            CrawlerState: 恢复的状态，如果失败则返回None
        """
        if not self.enable_persistence:
            return None
        
        try:
            state_file_path = Path(self.persistence_file)
            
            if not state_file_path.exists():
                logger.debug(f"No persisted state file found: {self.persistence_file}")
                return None
            
            # 读取文件
            with open(self.persistence_file, 'r', encoding='utf-8') as f:
                persistence_data = json.load(f)
            
            # 验证数据格式
            if 'state' not in persistence_data:
                logger.warning("Invalid persisted state format: missing 'state' key")
                return None
            
            # 恢复状态
            state_data = persistence_data['state']
            restored_state = CrawlerState.from_dict(state_data)
            
            # 验证状态一致性
            if self._validate_state_consistency(restored_state):
                logger.info(f"State restored successfully from {self.persistence_file}")
                return restored_state
            else:
                logger.warning("Restored state failed consistency validation")
                return None
                
        except Exception as e:
            logger.error(f"Failed to restore state: {e}")
            return None
    
    def _validate_state_consistency(self, state: CrawlerState) -> bool:
        """
        验证状态一致性
        
        Args:
            state: 要验证的状态
            
        Returns:
            bool: 状态是否一致
        """
        try:
            # 检查必需字段
            if not state.current_state or not state.previous_state:
                logger.warning("State validation failed: missing required fields")
                return False
            
            # 检查状态值是否有效
            valid_states = ['idle', 'starting', 'running', 'pausing', 'paused', 
                          'resuming', 'stopping', 'error']
            if state.current_state not in valid_states:
                logger.warning(f"State validation failed: invalid current_state '{state.current_state}'")
                return False
            
            if state.previous_state not in valid_states:
                logger.warning(f"State validation failed: invalid previous_state '{state.previous_state}'")
                return False
            
            # 检查版本号
            if state.version < 1:
                logger.warning(f"State validation failed: invalid version {state.version}")
                return False
            
            # 检查状态标志一致性
            if state.current_state == 'running' and not state.is_crawling:
                logger.warning("State validation failed: running state but is_crawling=False")
                return False
            
            if state.current_state == 'paused' and not state.is_paused:
                logger.warning("State validation failed: paused state but is_paused=False")
                return False
            
            if state.current_state == 'idle' and (state.is_crawling or state.is_paused):
                logger.warning("State validation failed: idle state but is_crawling or is_paused=True")
                return False
            
            logger.debug("State consistency validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Error during state validation: {e}")
            return False
    
    def force_persist(self) -> bool:
        """
        强制持久化当前状态（用于关键时刻）
        
        Returns:
            bool: 是否成功持久化
        """
        return self._persist_state()
    
    def get_persistence_info(self) -> Dict[str, Any]:
        """
        获取持久化信息
        
        Returns:
            dict: 持久化信息
        """
        info = {
            'enabled': self.enable_persistence,
            'file': self.persistence_file,
            'exists': False,
            'last_modified': None,
            'size_bytes': None
        }
        
        if self.enable_persistence:
            state_file_path = Path(self.persistence_file)
            if state_file_path.exists():
                info['exists'] = True
                stat = state_file_path.stat()
                info['last_modified'] = datetime.fromtimestamp(stat.st_mtime).isoformat()
                info['size_bytes'] = stat.st_size
        
        return info

    def _register_fallback_handlers(self):
        """注册降级处理器"""
        # 状态持久化失败时的降级处理
        self.fault_tolerance.register_fallback(
            'persist_state',
            lambda: self._fallback_memory_state()
        )
        
        # 共享状态访问失败时的降级处理
        self.fault_tolerance.register_fallback(
            'shared_state_access',
            lambda: self._fallback_local_state()
        )
        
        logger.info("Fallback handlers registered")
    
    def _fallback_memory_state(self) -> Dict[str, Any]:
        """
        持久化失败时的降级方案：使用内存状态
        
        Returns:
            dict: 当前内存中的状态
        """
        logger.warning("Using memory state as fallback for persistence failure")
        return self._current_state.to_dict()
    
    def _fallback_local_state(self) -> Dict[str, Any]:
        """
        共享状态访问失败时的降级方案：使用本地状态
        
        Returns:
            dict: 本地状态数据
        """
        logger.warning("Using local state as fallback for shared state access failure")
        return self._current_state.to_dict()
    
    def get_state_with_fallback(self) -> tuple[bool, CrawlerState, bool]:
        """
        获取状态，失败时使用降级方案
        
        Returns:
            tuple: (是否成功, 状态对象, 是否使用了降级)
        """
        def get_state_operation():
            if self.shared_state:
                # 尝试从共享状态获取
                return self.shared_state.get_state()
            else:
                # 使用本地状态
                return self._current_state
        
        success, result, used_fallback = self.fault_tolerance.execute_with_fallback(
            get_state_operation,
            'shared_state_access',
            FallbackStrategy.LOCAL
        )
        
        if success:
            if used_fallback:
                logger.info("State retrieved using fallback")
            return True, result, used_fallback
        else:
            logger.error("Failed to get state even with fallback")
            return False, self._current_state, True
    
    def persist_state_with_retry(self) -> bool:
        """
        持久化状态，失败时重试
        
        Returns:
            bool: 是否成功持久化
        """
        if not self.enable_persistence:
            return True
        
        def persist_operation():
            return self._persist_state()
        
        success, result = self.fault_tolerance.execute_with_retry(
            persist_operation,
            'persist_state',
            timeout=5.0,  # 5秒超时
            max_attempts=3  # 最多3次重试
        )
        
        if not success:
            logger.warning("State persistence failed after retries, using memory state")
        
        return success
    
    def get_fault_tolerance_stats(self) -> Dict[str, Any]:
        """
        获取容错统计信息
        
        Returns:
            dict: 容错统计数据
        """
        return self.fault_tolerance.get_stats()
