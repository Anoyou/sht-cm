#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务管理器 - 统一管理所有后台任务和worker
"""

import os
import time
import threading
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

class TaskType(Enum):
    """任务类型"""
    PERIODIC = "periodic"  # 周期性任务
    SCHEDULED = "scheduled"  # 定时任务
    MANUAL = "manual"  # 手动任务
    SERVICE = "service"  # 服务任务

class TaskStatus(Enum):
    """任务状态"""
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"
    WAITING = "waiting"
    PAUSED = "paused"

@dataclass
class Task:
    """任务定义"""
    id: str
    name: str
    description: str
    task_type: TaskType
    status: TaskStatus = TaskStatus.STOPPED
    source_file: str = ""
    function: Optional[Callable] = None
    interval: Optional[int] = None  # 周期性任务的间隔（秒）
    schedule_time: Optional[str] = None  # 定时任务的时间（HH:MM格式）
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    thread: Optional[threading.Thread] = None
    error_message: str = ""
    run_count: int = 0
    manual_executable: bool = True
    auto_restart: bool = True
    paused_at: Optional[datetime] = None  # 暂停时间

class TaskManager:
    """任务管理器"""
    
    def __init__(self):
        self.tasks: Dict[str, Task] = {}
        self.running = False
        self.manager_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self.app = None  # Flask应用实例，稍后设置
        
        # 注册所有系统任务
        self._register_system_tasks()
    
    def set_app(self, app):
        """设置Flask应用实例"""
        self.app = app
    
    def _register_system_tasks(self):
        """注册系统中的所有任务"""

        # 1. Telegram机器人服务
        self.register_task(Task(
            id="telegram_bot",
            name="Telegram机器人",
            description="提供Telegram机器人服务",
            task_type=TaskType.SERVICE,
            source_file="bot.py",
            function=self._run_telegram_bot,
            auto_restart=True
        ))

        # 2. 数据库维护任务
        self.register_task(Task(
            id="database_maintenance",
            name="数据库维护",
            description="定期清理重复数据、优化数据库",
            task_type=TaskType.SCHEDULED,
            source_file="maintenance_tools.py",
            schedule_time="02:00",  # 每天凌晨2点
            function=self._run_database_maintenance
        ))

        # 3. 每日定时增量爬取任务
        from configuration import config_manager
        auto_enabled = config_manager.get('AUTO_CRAWL_ENABLED', False)
        auto_time = config_manager.get('AUTO_CRAWL_TIME', '03:00')
        
        self.register_task(Task(
            id="daily_crawl",
            name="每日定时爬取",
            description="每天定时抓取48小时内的3页资源（全板块）",
            task_type=TaskType.SCHEDULED,
            source_file="scheduler/core.py",
            schedule_time=auto_time,
            function=self._run_daily_crawl,
            status=TaskStatus.WAITING if auto_enabled else TaskStatus.PAUSED
        ))

    def register_task(self, task: Task):
        """注册任务"""
        with self._lock:
            self.tasks[task.id] = task
            logger.info(f"注册任务: {task.name} ({task.id})")
    
    def start_manager(self):
        """启动任务管理器"""
        if self.running:
            logger.warning("任务管理器已在运行")
            return
        
        self.running = True
        self.manager_thread = threading.Thread(target=self._manager_loop, daemon=True, name='TaskManager')
        self.manager_thread.start()
        logger.info("任务管理器已启动")
        
        # 启动所有自动任务
        self._start_auto_tasks()
    
    def stop_manager(self):
        """停止任务管理器"""
        self.running = False
        
        # 停止所有任务
        with self._lock:
            for task in self.tasks.values():
                if task.status == TaskStatus.RUNNING:
                    self._stop_task(task)
        
        if self.manager_thread:
            self.manager_thread.join(timeout=5)
        
        logger.info("任务管理器已停止")
    
    def _start_auto_tasks(self):
        """启动所有自动任务"""
        with self._lock:
            for task in self.tasks.values():
                if task.task_type in [TaskType.PERIODIC, TaskType.SCHEDULED, TaskType.SERVICE]:
                    self._calculate_next_run(task)
                    if task.task_type == TaskType.SERVICE:
                        # 服务类型任务立即启动
                        self._start_task(task)
    
    def _manager_loop(self):
        """管理器主循环"""
        from crawler_control.task_lock_manager import get_task_lock_manager
        lock_manager = get_task_lock_manager()

        while self.running:
            try:
                current_time = datetime.now()

                with self._lock:
                    for task in self.tasks.values():
                        # 跳过暂停的任务
                        if task.status == TaskStatus.PAUSED:
                            continue

                        # 检查是否需要执行任务
                        if (task.next_run and
                            current_time >= task.next_run and
                            task.status != TaskStatus.RUNNING):

                            if task.task_type in [TaskType.PERIODIC, TaskType.SCHEDULED]:
                                # 尝试获取任务锁，防止并发执行
                                if lock_manager.acquire_lock(task.id):
                                    self._start_task(task)
                                else:
                                    logger.warning(f"⏳ 任务 {task.name} 正在执行中，跳过本次启动")

                        # 检查服务类型任务是否需要重启
                        elif (task.task_type == TaskType.SERVICE and
                              task.status == TaskStatus.ERROR and
                              task.auto_restart):
                            logger.info(f"重启服务任务: {task.name}")
                            # 服务任务也需要获取锁
                            if lock_manager.acquire_lock(task.id):
                                self._start_task(task)

                time.sleep(30)  # 每30秒检查一次

            except Exception as e:
                logger.error(f"任务管理器循环异常: {e}")
                time.sleep(60)
    
    def _start_task(self, task: Task):
        """启动任务"""
        if task.status == TaskStatus.RUNNING:
            return
        
        try:
            task.status = TaskStatus.RUNNING
            task.error_message = ""
            
            if task.task_type == TaskType.SERVICE:
                # 服务类型任务持续运行
                task.thread = threading.Thread(
                    target=self._run_service_task, 
                    args=(task,), 
                    daemon=True, 
                    name=f'Task-{task.id}'
                )
            else:
                # 其他类型任务运行一次
                task.thread = threading.Thread(
                    target=self._run_single_task, 
                    args=(task,), 
                    daemon=True, 
                    name=f'Task-{task.id}'
                )
            
            task.thread.start()
            logger.info(f"启动任务: {task.name}")
            
        except Exception as e:
            task.status = TaskStatus.ERROR
            task.error_message = str(e)
            logger.error(f"启动任务失败 {task.name}: {e}")
    
    def _stop_task(self, task: Task):
        """停止任务"""
        if task.status != TaskStatus.RUNNING:
            return
        
        # 对于Telegram Bot任务，调用特殊的停止函数
        if task.id == "telegram_bot":
            try:
                # 导入并调用Bot的停止函数
                import bot
                bot.stop_bot()
                logger.info("已发送停止信号给Telegram Bot")
            except Exception as e:
                logger.error(f"停止Telegram Bot失败: {e}")
        
        task.status = TaskStatus.STOPPED
        # 注意：这里不能强制终止线程，只能设置状态让任务自行退出
        logger.info(f"停止任务: {task.name}")
    
    def _run_single_task(self, task: Task):
        """运行单次任务"""
        from crawler_control.task_lock_manager import get_task_lock_manager
        lock_manager = get_task_lock_manager()

        try:
            task.last_run = datetime.now()
            task.run_count += 1
            logger.info(f"🚀 开始执行任务: {task.name} (第 {task.run_count} 次)")

            if task.function:
                task.function()

            task.status = TaskStatus.WAITING
            self._calculate_next_run(task)

            # 任务成功完成，清除进度
            lock_manager.clear_progress(task.id)
            logger.info(f"✅ 任务完成: {task.name}")

        except Exception as e:
            task.status = TaskStatus.ERROR
            task.error_message = str(e)
            logger.error(f"❌ 任务执行失败 {task.name}: {e}")

            # 记录任务失败的进度，便于下次恢复
            import traceback
            progress_data = {
                'last_error': str(e)[:200],
                'last_traceback': traceback.format_exc()[:500],
                'failed_at': datetime.now(timezone.utc).isoformat(),
                'run_count': task.run_count
            }
            lock_manager.save_progress(task.id, progress_data)

        finally:
            # 必须释放任务锁，允许下次执行
            lock_manager.release_lock(task.id)
            logger.info(f"🔓 释放任务锁: {task.name}")
    
    def _run_service_task(self, task: Task):
        """运行服务类型任务"""
        from crawler_control.task_lock_manager import get_task_lock_manager
        lock_manager = get_task_lock_manager()

        try:
            task.last_run = datetime.now()
            task.run_count += 1
            
            if task.function:
                task.function()
            
        except Exception as e:
            task.status = TaskStatus.ERROR
            task.error_message = str(e)
            logger.error(f"服务任务异常 {task.name}: {e}")
        finally:
            lock_manager.release_lock(task.id)
            logger.info(f"🔓 释放任务锁: {task.name}")
    
    def _calculate_next_run(self, task: Task):
        """计算下次执行时间"""
        if task.task_type == TaskType.PERIODIC and task.interval:
            # 周期性任务：当前时间 + 间隔
            task.next_run = datetime.now() + timedelta(seconds=task.interval)
            
        elif task.task_type == TaskType.SCHEDULED and task.schedule_time:
            # 定时任务：下一个指定时间点
            now = datetime.now()
            hour, minute = map(int, task.schedule_time.split(':'))
            
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if next_run <= now:
                # 如果今天的时间已过，设置为明天
                next_run += timedelta(days=1)
            
            task.next_run = next_run
    
    def manual_execute(self, task_id: str) -> bool:
        """手动执行任务"""
        with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                return False
            
            if not task.manual_executable:
                return False
            
            if task.status == TaskStatus.RUNNING:
                return False
            
            # 手动执行任务
            self._start_task(task)
            
            # 如果是周期性任务，重新计算下次执行时间
            if task.task_type == TaskType.PERIODIC:
                self._calculate_next_run(task)
            
            return True
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        task = self.tasks.get(task_id)
        if not task:
            return None
        
        return {
            'id': task.id,
            'name': task.name,
            'description': task.description,
            'type': task.task_type.value,
            'status': task.status.value,
            'source_file': task.source_file,
            'last_run': task.last_run.isoformat() if task.last_run else None,
            'next_run': task.next_run.isoformat() if task.next_run else None,
            'run_count': task.run_count,
            'error_message': task.error_message,
            'manual_executable': task.manual_executable,
            'interval': task.interval,
            'schedule_time': task.schedule_time,
            'paused_at': task.paused_at.isoformat() if hasattr(task, 'paused_at') and task.paused_at else None
        }
    
    def pause_task(self, task_id: str) -> bool:
        """暂停任务"""
        with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                logger.error(f"任务不存在: {task_id}")
                return False
            
            if task.status == TaskStatus.PAUSED:
                logger.warning(f"任务已经暂停: {task.name}")
                return True
            
            # 停止正在运行的任务
            if task.status == TaskStatus.RUNNING:
                self._stop_task(task)
                # 等待任务完全停止
                import time
                time.sleep(1)
            
            # 设置为暂停状态
            task.status = TaskStatus.PAUSED
            task.paused_at = datetime.now()
            
            logger.info(f"暂停任务: {task.name}")
            return True
    
    def resume_task(self, task_id: str) -> bool:
        """恢复任务"""
        with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                logger.error(f"任务不存在: {task_id}")
                return False
            
            # 无论当前状态如何，都尝试恢复
            if task.status == TaskStatus.PAUSED:
                logger.info(f"恢复暂停的任务: {task.name}")
            else:
                logger.info(f"重新启动任务: {task.name}")
            
            # 恢复任务状态
            task.status = TaskStatus.STOPPED
            task.paused_at = None
            
            # 对于服务类型任务，立即启动
            if task.task_type == TaskType.SERVICE:
                # 重置Bot的停止标志
                if task.id == "telegram_bot":
                    try:
                        import bot
                        bot.bot_stop_event.clear()  # 清除停止标志
                        logger.info("已清除Telegram Bot停止标志")
                    except Exception as e:
                        logger.error(f"清除Bot停止标志失败: {e}")
                
                self._start_task(task)
            else:
                # 重新计算下次执行时间
                self._calculate_next_run(task)
            
            logger.info(f"恢复任务: {task.name}")
            return True

    def update_task_schedule(self, task_id: str, new_time: str = None, enabled: bool = None) -> bool:
        """更新任务调度配置"""
        with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                logger.error(f"任务不存在: {task_id}")
                return False
            
            changes_made = False
            
            # 更新时间
            if new_time and task.schedule_time != new_time:
                task.schedule_time = new_time
                changes_made = True
                logger.info(f"任务 {task.name} 定时时间更新为: {new_time}")
            
            # 更新启用状态
            if enabled is not None:
                should_be_paused = not enabled
                is_currently_paused = task.status == TaskStatus.PAUSED
                
                if should_be_paused and not is_currently_paused:
                    # 需要暂停
                    self._stop_task(task) # 如果正在运行，先停止
                    task.status = TaskStatus.PAUSED
                    task.paused_at = datetime.now()
                    changes_made = True
                    logger.info(f"任务 {task.name} 已根据配置暂停")
                    
                elif not should_be_paused and is_currently_paused:
                    # 需要恢复
                    task.status = TaskStatus.WAITING
                    task.paused_at = None
                    changes_made = True
                    logger.info(f"任务 {task.name} 已根据配置启用")

            # 如果有变动且任务未被暂停，重新计算下次运行时间
            if changes_made and task.status != TaskStatus.PAUSED:
                self._calculate_next_run(task)
                
            return True
    
    def get_all_tasks_status(self) -> List[Dict[str, Any]]:
        """获取所有任务状态"""
        with self._lock:
            return [self.get_task_status(task_id) for task_id in self.tasks.keys()]
    
    # ==================== 任务实现函数 ====================

    def _run_telegram_bot(self):
        """运行Telegram机器人服务"""
        try:
            # 检查是否配置了Bot Token
            import os
            bot_token = os.environ.get('TG_BOT_TOKEN')
            if not bot_token:
                logger.warning("Telegram Bot Token未配置，跳过启动")
                return
            
            # 为Telegram机器人创建新的事件循环
            import asyncio
            import threading
            
            def run_bot_with_loop():
                try:
                    # 创建新的事件循环
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    # 导入并启动机器人
                    from bot import main as bot_main
                    bot_main()
                    
                except Exception as e:
                    logger.error(f"Telegram机器人运行异常: {e}")
                    import traceback
                    logger.error(f"错误详情: {traceback.format_exc()}")
                finally:
                    try:
                        loop.close()
                    except:
                        pass
            
            # 在新线程中运行机器人
            bot_thread = threading.Thread(target=run_bot_with_loop, daemon=True, name='TelegramBot')
            bot_thread.start()
            
            # 等待线程启动
            import time
            time.sleep(2)
            
            # 检查线程是否还活着
            if bot_thread.is_alive():
                logger.info("Telegram机器人已在独立线程中启动")
            else:
                logger.warning("Telegram机器人线程启动失败")
            
        except Exception as e:
            logger.error(f"Telegram机器人服务失败: {e}")
            raise
    
    def _run_database_maintenance(self):
        """运行数据库维护"""
        try:
            if self.app:
                with self.app.app_context():
                    from maintenance_tools import DatabaseMaintenance
                    maintenance = DatabaseMaintenance()
                    maintenance.run_full_maintenance()
            else:
                logger.warning("数据库维护任务跳过：Flask应用未设置")
        except Exception as e:
            logger.error(f"数据库维护任务失败: {e}")
            raise

    def _run_daily_crawl(self):
        """运行每日定时爬取 - 支持进度恢复和自动重试"""
        from crawler_control.task_lock_manager import get_task_lock_manager
        lock_manager = get_task_lock_manager()

        max_retries = 3
        retry_count = 0

        while retry_count <= max_retries:
            try:
                if self.app:
                    with self.app.app_context():
                        from scheduler.core import run_crawling_with_options

                        # 检查是否有之前的进度需要恢复
                        saved_progress = lock_manager.load_progress('daily_crawl')
                        if saved_progress and retry_count == 0:
                            logger.info(f"📖 检测到上次任务的失败记录，本次将继续恢复爬取")
                            logger.info(f"   ✓ 上次错误: {saved_progress.get('last_error', '未知')}")
                            logger.info(f"   ✓ 失败时间: {saved_progress.get('failed_at', '未知')}")

                        if retry_count > 0:
                            # 重试时等待一段时间
                            wait_time = 30 * (2 ** (retry_count - 1))  # 指数退避：30s, 60s, 120s
                            logger.info(f"⏳ 准备第 {retry_count} 次重试，等待 {wait_time} 秒...")
                            import time
                            time.sleep(wait_time)

                        logger.info(f"🕒 开始每日定时增量爬取... (重试: {retry_count}/{max_retries})")

                        # 48小时 = 172800秒, 3页
                        run_crawling_with_options(
                            section_fids=None,
                            dateline=172800,
                            max_pages=3,
                            page_mode='fixed',
                            task_type='scheduled'
                        )

                        logger.info("✅ 每日定时增量爬取完成")
                        # 任务成功，清除进度记录
                        lock_manager.clear_progress('daily_crawl')
                        break  # 成功，退出重试循环

                else:
                    logger.warning("每日定时爬取任务跳过：Flask应用未设置")
                    break

            except Exception as e:
                retry_count += 1
                logger.error(f"❌ 每日定时爬取任务失败 (第{retry_count}次): {str(e)[:200]}")

                if retry_count <= max_retries:
                    # 保存进度用于恢复
                    import traceback
                    progress_data = {
                        'last_error': str(e)[:200],
                        'last_traceback': traceback.format_exc()[:500],
                        'failed_at': datetime.now(timezone.utc).isoformat(),
                        'retry_count': retry_count
                    }
                    lock_manager.save_progress('daily_crawl', progress_data)
                    logger.warning(f"⚠️ 已保存失败进度，将在 {30 * (2 ** (retry_count - 1))} 秒后进行第 {retry_count + 1} 次重试")
                else:
                    logger.error(f"❌ 每日定时爬取任务已失败 {max_retries} 次，放弃重试")
                    # 任务最终失败，通知管理员
                    try:
                        from scheduler.notifier import _send_telegram_message
                        error_msg = f"""❌ *每日定时爬取任务最终失败*
━━━━━━━━━━━━━━
⚠️ 已重试 {max_retries} 次仍未成功
📝 错误信息：{str(e)[:200]}
⏰ 失败时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

💡 建议：检查网络连接、代理配置或目标网站状态"""
                        _send_telegram_message(error_msg, parse_mode='Markdown')
                    except:
                        pass
                    raise

# 全局任务管理器实例
task_manager = TaskManager()

def start_task_manager(app=None):
    """启动任务管理器"""
    if app:
        task_manager.set_app(app)
    task_manager.start_manager()

def stop_task_manager():
    """停止任务管理器"""
    task_manager.stop_manager()

if __name__ == '__main__':
    # 测试任务管理器
    import argparse
    
    parser = argparse.ArgumentParser(description='任务管理器')
    parser.add_argument('--start', action='store_true', help='启动任务管理器')
    parser.add_argument('--status', action='store_true', help='显示任务状态')
    
    args = parser.parse_args()
    
    if args.start:
        print("启动任务管理器...")
        start_task_manager()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n停止任务管理器...")
            stop_task_manager()
    elif args.status:
        tasks = task_manager.get_all_tasks_status()
        print(f"共有 {len(tasks)} 个任务:")
        for task in tasks:
            print(f"- {task['name']} ({task['id']}): {task['status']}")
    else:
        parser.print_help()