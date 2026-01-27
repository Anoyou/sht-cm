#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
健康管理核心模块 - 整合系统监控、性能分析与数据一致性验证
取代了原 system_monitor.py 和 validation.py
"""

import os
import re
import json
import time
import logging
import psutil
import requests
from threading import Thread
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urlparse, parse_qs
from sqlalchemy import text

from models import db, Resource, Category, ValidationLog
from cache_manager import cache_manager, CacheKeys

logger = logging.getLogger(__name__)

# ==================== 1. 数据验证逻辑 (原 validation.py) ====================

class DataValidator:
    """数据一致性验证器"""
    
    def __init__(self):
        self.rules = {
            'tid_url_match': True,
            'title_consistency': True,
            'content_sanity': True,
            'series_logic': True,
            'duplicate_check': True,
        }
    
    def validate_batch(self, detail_urls: List[Tuple[int, str]], batch_results: List[Dict]) -> List[Dict]:
        """批量验证抓取结果"""
        validated = []
        for (tid, url), data in zip(detail_urls, batch_results):
            if not data:
                validated.append(None)
                continue
                
            res = self._validate_single(tid, url, data)
            if res['valid']:
                validated.append(data)
                # 记录成功日志
                ValidationLog.log(tid, data.get('title'), url, 'passed')
            else:
                validated.append(None)
                # 记录失败日志
                ValidationLog.log(tid, data.get('title'), url, 'failed', res['reasons'])
        return validated

    def _validate_single(self, tid: int, url: str, data: Dict) -> Dict:
        reasons = []
        # TID 匹配
        if not self._check_tid(tid, url): reasons.append('tid_mismatch')
        # 磁力链接校验
        if not data.get('magnet') or not data['magnet'].startswith('magnet:'): reasons.append('invalid_magnet')
        # 标题校验
        if not data.get('title') or len(data['title']) < 2: reasons.append('invalid_title')
        
        return {'valid': len(reasons) == 0, 'reasons': reasons}

    def _check_tid(self, tid: int, url: str) -> bool:
        try:
            url_tid = parse_qs(urlparse(url).query).get('tid', [None])[0]
            if url_tid and int(url_tid) == tid: return True
            match = re.search(r'tid[=:](\d+)', url)
            return int(match.group(1)) == tid if match else False
        except: return False

class RealTimeValidator:
    """实时验证器 - 通过二次请求确认"""
    def __init__(self, proxies: Dict = None):
        self.proxies = proxies
    
    def live_check(self, tid: int, url: str, expected_title: str) -> bool:
        try:
            from utils.retry_utils import retry_request, RETRY_CONFIG

            config = RETRY_CONFIG['health_check']
            resp = retry_request(
                requests.get,
                url=url,
                proxies=self.proxies,
                raise_on_fail=False,
                **config
            )
            if not resp or resp.status_code != 200: return False
            from pyquery import PyQuery as pq
            page_title = pq(resp.text)('h2.n5_bbsnrbt').text().strip()
            return expected_title.lower() in page_title.lower() or page_title.lower() in expected_title.lower()
        except: return False

# ==================== 2. 系统监控逻辑 (原 system_monitor.py) ====================

class SystemMonitor:
    """系统资源监控器"""
    def __init__(self, app=None):
        self.app = app
        self.monitoring = False
        self.history = []
        self.max_history = 60

    def collect(self) -> Dict[str, Any]:
        """手动采集一次指标"""
        metrics = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'system': self._get_sys_info(),
            'app': self._get_app_info(),
            'cache': cache_manager.get_stats(),
        }
        
        # 只有在 Flask app 上下文中才能获取数据库指标
        from flask import current_app, has_app_context
        
        app_to_use = self.app
        if not app_to_use and has_app_context():
            app_to_use = current_app._get_current_object()

        if app_to_use:
            try:
                with app_to_use.app_context():
                    metrics['db'] = self._get_db_info()
                    metrics['validation'] = ValidationLog.get_recent_stats()
            except Exception as e:
                logger.error(f"获取数据库健康指标失败: {e}")
                metrics['db'] = {'error': str(e)}
        elif has_app_context():
            # 已经在上下文中，直接尝试
            try:
                metrics['db'] = self._get_db_info()
                metrics['validation'] = ValidationLog.get_recent_stats()
            except Exception as e:
                logger.error(f"直接获取数据库健康指标失败: {e}")
                metrics['db'] = {'error': str(e)}
        
        self.history.append(metrics)
        if len(self.history) > self.max_history: self.history.pop(0)
        
        # 存入缓存供 API 快速读取
        cache_manager.set(CacheKeys.HEALTH, metrics, ttl=600)
        return metrics

    def _get_sys_info(self):
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        return {
            'cpu': psutil.cpu_percent(),
            'memory': {'percent': mem.percent, 'used_mb': mem.used // 1048576},
            'disk': {'percent': disk.percent, 'free_gb': disk.free // (1024**3)}
        }

    def _get_app_info(self):
        proc = psutil.Process()
        return {
            'memory_rss_mb': proc.memory_info().rss // 1048576,
            'threads': proc.num_threads(),
            'uptime_sec': int(time.time() - proc.create_time())
        }

    def _get_db_info(self):
        """获取数据库统计信息 - 使用更稳健的查询方式"""
        try:
            from sqlalchemy import func
            # 使用 db.session 直接查询以确保在当前上下文中有效
            res_count = db.session.query(func.count(Resource.id)).scalar() or 0
            cat_count = db.session.query(func.count(Category.id)).scalar() or 0
            # 使用 execute(text(...)) 避免 SQLAlchemy 2.0 的 select_from(text) 错误
            failed_count = db.session.execute(text("SELECT count(*) FROM failed_tid")).scalar() or 0
            
            return {
                'resources': res_count,
                'categories': cat_count,
                'failed_tids': failed_count,
                'status': 'ok'
            }
        except Exception as e:
            logger.error(f"数据库健康检查失败详情: {e}")
            return {'error': str(e), 'status': 'error', 'resources': 0, 'categories': 0}

    def get_summary(self) -> Dict[str, Any]:
        """获取健康摘要及建议"""
        metrics = self.collect()
        score = 100
        issues = []
        
        sys = metrics.get('system', {})
        if sys.get('cpu', 0) > 80: score -= 20; issues.append("CPU 使用率过高")
        if sys.get('memory', {}).get('percent', 0) > 90: score -= 30; issues.append("内存几近耗尽")
        if sys.get('disk', {}).get('percent', 0) > 95: score -= 40; issues.append("磁盘空间严重不足")
        
        val = metrics.get('validation', {})
        if val.get('success_rate', 100) < 80: score -= 15; issues.append("数据验证通过率偏低")

        status = "healthy" if score >= 80 else ("warning" if score >= 60 else "critical")
        
        return {
            'status': status,
            'score': max(0, score),
            'issues': issues,
            'metrics': metrics
        }

# ==================== 3. 全局单例与助手函数 ====================

# 验证器单例
validator = DataValidator()
# 监控器单例 (app 会在 start_monitoring 时注入)
monitor = SystemMonitor()

def validate_batch_results(detail_urls, results):
    """供外部调用的批量验证快捷方式"""
    return validator.validate_batch(detail_urls, results)

def start_monitoring(app, interval: int = 600):
    """启动后台监控线程"""
    monitor.app = app
    if monitor.monitoring: return
    
    def loop():
        monitor.monitoring = True
        logger.info(f"🚀 系统健康监控已启动，间隔: {interval}s")
        while monitor.monitoring:
            try:
                monitor.collect()
            except Exception as e:
                logger.error(f"健康监控采集异常: {e}")
            time.sleep(interval)
            
    thread = Thread(target=loop, daemon=True, name="HealthMonitor")
    thread.start()

def stop_monitoring():
    """停止监控"""
    monitor.monitoring = False
