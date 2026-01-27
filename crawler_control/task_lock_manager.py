#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务锁和进度管理 - 防止任务并发执行，保存任务进度便于恢复
"""

import os
import json
import time
import logging
import threading
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Iterator
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class TaskLockManager:
    """
    任务执行锁管理器
    防止同一任务在多个进程中并发运行
    支持进度保存和恢复
    """

    def __init__(self, lock_dir: str = None):
        if lock_dir:
            self.lock_dir = lock_dir
        else:
            from configuration import Config
            self.lock_dir = Config.get_path('task_lock_dir')
        os.makedirs(self.lock_dir, exist_ok=True)
        self.progress_file = os.path.join(self.lock_dir, 'task_progress.json')
        self.lock_file_template = os.path.join(self.lock_dir, 'task_lock_{task_id}.lock')
        self.lock_timeout = 3600  # 锁超时时间（秒），防止死锁
        self.acquire_timeout = 30  # 获取锁的超时时间（秒）
        self._file_lock = threading.Lock()  # 文件锁，防止并发写入

    def acquire_lock(self, task_id: str) -> bool:
        """
        尝试获取任务锁（带超时）

        返回:
            True: 成功获取锁（可以执行任务）
            False: 锁被占用（有其他进程在执行此任务）或超时
        """
        start_time = time.time()
        lock_file = self.lock_file_template.format(task_id=task_id)

        # 在超时时间内重试获取锁
        while time.time() - start_time < self.acquire_timeout:
            with self._file_lock:  # 使用文件锁防止并发
                # 检查现有锁
                if os.path.exists(lock_file):
                    try:
                        with open(lock_file, 'r') as f:
                            lock_data = json.load(f)
                            lock_time = lock_data.get('acquired_at', 0)
                            lock_pid = lock_data.get('pid')

                            # 检查锁是否过期
                            elapsed = time.time() - lock_time
                            if elapsed < self.lock_timeout:
                                logger.debug(f"⏳ 任务 {task_id} 已被进程 {lock_pid} 锁定 ({elapsed:.0f}秒前)")
                                time.sleep(0.5)  # 等待0.5秒后重试
                                continue
                            else:
                                logger.warning(f"⚠️ 任务 {task_id} 的旧锁已过期 ({elapsed:.0f}秒)，清除锁")
                                try:
                                    os.remove(lock_file)
                                except:
                                    pass  # 文件可能已被其他进程删除
                    except Exception as e:
                        logger.error(f"❌ 读取任务锁失败: {e}")
                        time.sleep(0.5)  # 等待0.5秒后重试
                        continue

                # 创建新锁
                try:
                    lock_data = {
                        'task_id': task_id,
                        'pid': os.getpid(),
                        'acquired_at': time.time(),
                        'acquired_time': datetime.now(timezone.utc).isoformat()
                    }
                    with open(lock_file, 'w') as f:
                        json.dump(lock_data, f, indent=2)
                    logger.info(f"🔒 获取任务锁: {task_id} (PID: {os.getpid()})")
                    return True
                except Exception as e:
                    logger.error(f"❌ 创建任务锁失败: {e}")
                    time.sleep(0.5)  # 等待0.5秒后重试
                    continue

        # 超时
        logger.warning(f"⏱️ 获取任务锁超时: {task_id} (等待了 {self.acquire_timeout} 秒)")
        return False

    def release_lock(self, task_id: str) -> bool:
        """
        释放任务锁
        """
        lock_file = self.lock_file_template.format(task_id=task_id)

        try:
            if os.path.exists(lock_file):
                with self._file_lock:  # 使用文件锁防止并发
                    os.remove(lock_file)
                logger.info(f"🔓 释放任务锁: {task_id}")
            return True
        except Exception as e:
            logger.error(f"❌ 释放任务锁失败: {e}")
            return False

    @contextmanager
    def task_lock(self, task_id: str) -> Iterator[bool]:
        """
        任务锁上下文管理器 - 确保锁在异常情况下自动释放

        使用方式:
            with task_lock_manager.task_lock('my_task_id') as acquired:
                if acquired:
                    # 执行任务
                    pass
        """
        acquired = False
        try:
            acquired = self.acquire_lock(task_id)
            if not acquired:
                yield False
                return

            yield True

        finally:
            if acquired:
                self.release_lock(task_id)

    def save_progress(self, task_id: str, progress_data: Dict[str, Any]) -> bool:
        """
        保存任务进度

        progress_data 应包含：
        {
            'last_section': 'section_name',  # 最后处理的板块
            'last_page': 123,                 # 最后处理的页码
            'total_saved': 456,               # 总共保存的资源数
            'total_skipped': 789,             # 总共跳过的资源数
            'total_failed': 10,               # 总共失败的资源数
            'retry_count': 0,                 # 重试次数
            'saved_at': '2026-01-18T11:00:00'  # 保存时间
        }
        """
        try:
            # 读取现有进度数据
            all_progress = {}
            if os.path.exists(self.progress_file):
                try:
                    with open(self.progress_file, 'r') as f:
                        all_progress = json.load(f)
                except Exception as e:
                    logger.warning(f"⚠️ 读取进度文件失败: {e}")

            # 更新此任务的进度
            all_progress[task_id] = {
                **progress_data,
                'saved_at': datetime.now(timezone.utc).isoformat()
            }

            # 写回进度文件
            with open(self.progress_file, 'w') as f:
                json.dump(all_progress, f, indent=2, ensure_ascii=False)

            logger.debug(f"💾 已保存任务进度: {task_id}")
            return True
        except Exception as e:
            logger.error(f"❌ 保存进度失败: {e}")
            return False

    def load_progress(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        加载任务进度

        返回：
            进度数据字典，如果不存在则返回 None
        """
        try:
            if not os.path.exists(self.progress_file):
                return None

            with open(self.progress_file, 'r') as f:
                all_progress = json.load(f)
                progress = all_progress.get(task_id)

                if progress:
                    logger.info(f"📖 已加载任务进度: {task_id}")
                    return progress
                else:
                    return None
        except Exception as e:
            logger.error(f"❌ 加载进度失败: {e}")
            return None

    def clear_progress(self, task_id: str) -> bool:
        """
        清除任务进度（任务完成时调用）
        """
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    all_progress = json.load(f)

                if task_id in all_progress:
                    del all_progress[task_id]

                    with open(self.progress_file, 'w') as f:
                        json.dump(all_progress, f, indent=2, ensure_ascii=False)

                    logger.info(f"🗑️ 已清除任务进度: {task_id}")

            return True
        except Exception as e:
            logger.error(f"❌ 清除进度失败: {e}")
            return False


# 全局单例
_task_lock_manager = None

def get_task_lock_manager(lock_dir: str = None) -> TaskLockManager:
    """获取任务锁管理器单例"""
    global _task_lock_manager
    if _task_lock_manager is None:
        _task_lock_manager = TaskLockManager(lock_dir)
    return _task_lock_manager
