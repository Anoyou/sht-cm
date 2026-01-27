#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SHT 爬虫核心引擎 - 负责网站页面的解析、数据提取及 FastCrawler 高性能并发爬取实现
集成了多种防屏蔽策略和精细的解析逻辑，是系统数据获取的基础层
"""

import os
from urllib.parse import urlparse, parse_qs, urlencode, quote
from curl_cffi import requests
from pyquery import PyQuery as pq
import re
import random
from datetime import datetime, timedelta
import bencodepy
import hashlib
import binascii
from typing import List, Dict, Any, Optional

import logging
from configuration import Config
from models import Resource, Category, db
from utils import retry_on_lock
from health import validator, validate_batch_results

# ----------------- FastCrawler Integration -----------------
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from curl_cffi.requests import Session
import time
import threading

class FastCrawler:
    """高性能爬取器 - 兼容curl_cffi"""
    
    def __init__(self, max_workers: int = 10, max_connections: int = 20, 
                 delay_min: float = 0.3, delay_max: float = 0.8):
        self.max_workers = max_workers
        self.max_connections = max_connections
        self.delay_min = delay_min
        self.delay_max = delay_max
        
        # 创建会话池
        self.session_pool = Queue(maxsize=max_connections)
        self._init_session_pool()
        
        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # 统计信息
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_time': 0,
            'avg_response_time': 0
        }
    
    def _init_session_pool(self):
        """初始化会话池"""
        for _ in range(self.max_connections):
            session = Session()
            session.timeout = 30
            self.session_pool.put(session)
    
    def get_session(self) -> Session:
        return self.session_pool.get()
    
    def return_session(self, session: Session):
        self.session_pool.put(session)
    
    def fetch_url(self, url: str, headers: Dict = None, cookies: Dict = None, 
                  proxies: Dict = None) -> Optional[str]:
        """获取单个URL内容"""
        # 💤 随机延迟: 模拟真人操作，对齐异步模式的防封逻辑
        if self.delay_max > 0:
            delay = random.uniform(self.delay_min, self.delay_max)
            time.sleep(delay)

        session = None
        start_time = time.time()
        
        try:
            session = self.get_session()
            
            request_kwargs = {
                'timeout': 30,
                'allow_redirects': True,
                'impersonate': 'chrome110'
            }
            
            if headers: request_kwargs['headers'] = headers
            if cookies: request_kwargs['cookies'] = cookies
            if proxies: request_kwargs['proxies'] = proxies
            
            self.stats['total_requests'] += 1
            
            response = session.get(url, **request_kwargs)
            response.raise_for_status()
            
            self.stats['successful_requests'] += 1
            
            response_time = time.time() - start_time
            self.stats['total_time'] += response_time
            self.stats['avg_response_time'] = self.stats['total_time'] / self.stats['successful_requests']
            
            return response.text
            
        except Exception as e:
            self.stats['failed_requests'] += 1
            logger.warning(f"! [API] 获取URL失败: {url}, 错误: {e}")
            return None
        finally:
            if session:
                self.return_session(session)
    
    def fetch_urls_batch(self, urls: List[str], headers: Dict = None, 
                        cookies: Dict = None, proxies: Dict = None) -> List[Optional[str]]:
        """批量获取URL内容 - 保持顺序"""
        logger.info(f"[CRAWLER] 开始批量获取 {len(urls)}个URL")
        
        futures_with_index = []
        for i, url in enumerate(urls):
            future = self.executor.submit(
                self.fetch_url, url, headers, cookies, proxies
            )
            futures_with_index.append((i, future))
        
        results = [None] * len(urls)
        completed = 0
        
        for index, future in futures_with_index:
            try:
                result = future.result(timeout=30)
                results[index] = result
                completed += 1
                if completed % 10 == 0 or completed == len(urls):
                    logger.info(f"[CRAWLER] 批量获取进度: {completed}/{len(urls)}")
            except Exception as e:
                logger.warning(f"! [CRAWLER] 批量获取任务失败 (索引{index}): {e}")
                results[index] = None
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        return self.stats.copy()
    
    def close(self):
        self.executor.shutdown(wait=True)
        while not self.session_pool.empty():
            session = self.session_pool.get()
            session.close()

class BatchProcessor:
    """批量处理器"""
    
    def __init__(self, batch_size: int = 10, max_workers: int = 5, 
                 delay_min: float = 0.3, delay_max: float = 0.8):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.crawler = FastCrawler(
            max_workers=max_workers, 
            delay_min=delay_min, 
            delay_max=delay_max
        )
    
    def process_urls_in_batches(self, urls: List[str], process_func, 
                               headers: Dict = None, cookies: Dict = None, 
                               proxies: Dict = None) -> List[Any]:
        """分批处理URL列表"""
        logger.info(f"[CRAWLER] 开始分批处理 {len(urls)}个URL，批次大小: {self.batch_size}")
        all_results = []
        
        for i in range(0, len(urls), self.batch_size):
            batch_urls = urls[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            total_batches = (len(urls) + self.batch_size - 1) // self.batch_size
            
            logger.info(f" 🕷️ 处理批次 {batch_num}/{total_batches}: {len(batch_urls)}个URL")
            
            html_contents = self.crawler.fetch_urls_batch(
                batch_urls, headers, cookies, proxies
            )
            
            batch_results = []
            for url, html in zip(batch_urls, html_contents):
                if html:
                    try:
                        result = process_func(url, html)
                        batch_results.append(result)
                    except Exception as e:
                        logger.warning(f"! [CRAWLER] 处理URL失败: {url}, 错误: {e}")
                        batch_results.append(None)
                else:
                    batch_results.append(None)
            
            all_results.extend(batch_results)
            
            if batch_num < total_batches:
                time.sleep(0.1)
        
        success_count = sum(1 for r in all_results if r is not None)
        logger.info(f"✓ [CRAWLER] 分批处理完成: 成功 {success_count}/{len(urls)}")
        return all_results
    
    def get_crawler_stats(self) -> Dict[str, Any]:
        return self.crawler.get_stats()
    
    def close(self):
        self.crawler.close()

# -----------------------------------------------------------

# 获取日志记录器 - 不再重复配置basicConfig
logger = logging.getLogger(__name__)


def extract_and_convert_video_size(html_content):
    doc = pq(html_content)
    message_text = doc('.message').text()
    clean_text = re.sub(r'\s+', ' ', message_text).strip()
    pattern = r"(\d+\.?\d*)([GM])"
    match = re.search(pattern, clean_text)
    if not match:
        return None
    size_num_str, unit = match.groups()
    try:
        size_num = float(size_num_str)
    except ValueError:
        return None

    if unit.upper() == 'G':
        mb_size = size_num * 1024
    elif unit.upper() == 'M':
        mb_size = size_num
    else:
        return None
    return int(mb_size)


def extract_safeid(html_content):
    doc = pq(html_content)
    for script_elem in doc('script'):
        script_text = pq(script_elem).text().strip()
        if not script_text or 'safeid' not in script_text:
            continue
        match = re.search(r"safeid\s*=\s*['\"]([^'\"]+)['\"]", script_text)
        if match:
            return match.group(1)
    return None


def extract_exact_datetime(html_content):
    doc = pq(html_content)
    date_text = doc('dt.z.cl').eq(0).text().strip()
    if not date_text:
        return ""
    processed_text = date_text.replace('&nbsp;', ' ').strip()
    processed_text = re.sub(r'\s+', ' ', processed_text)
    today = datetime.now().date()
    if re.match(r'^\d+ 小时前$', processed_text):
        return today.strftime('%Y-%m-%d')
    elif processed_text.startswith('半小时前'):
        return today.strftime('%Y-%m-%d')
    elif re.match(r'^\d+ 分钟前$', processed_text):
        return today.strftime('%Y-%m-%d')
    elif re.match(r'^\d+ 秒前$', processed_text):
        return today.strftime('%Y-%m-%d')
    elif processed_text.startswith('昨天 '):
        yesterday = today - timedelta(days=1)
        return yesterday.strftime('%Y-%m-%d')
    elif processed_text.startswith('前天 '):
        day_before_yesterday = today - timedelta(days=2)
        return day_before_yesterday.strftime('%Y-%m-%d')
    elif re.match(r'^\d+ 天前$', processed_text):
        days = int(re.search(r'(\d+) 天前', processed_text).group(1))
        target_date = today - timedelta(days=days)
        return target_date.strftime('%Y-%m-%d')
    elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', processed_text):
        pure_date_str = processed_text.split(' ')[0]
        return pure_date_str
    else:
        logger.warning(f"! [CRAWLER] 无法解析日期格式: {date_text}")
        return None


def extract_bracket_content(html_content):
    doc = pq(html_content)
    h2_text = doc('h2.n5_bbsnrbt').text()
    clean_text = h2_text.strip()
    pattern = r"\[(.*?)\]"
    match = re.search(pattern, clean_text)

    if match:
        return match.group(1)
    else:
        return None




class SHT:
    proxy: str = None
    proxies = {}
    headers = {}
    cookie = {}
    bypass = None
    flare_solver = None

    def __init__(self):
        # 使用原版经过验证的iPhone User-Agent，成功率更高
        ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1"
        self.headers = {
            'User-Agent': ua
        }
        # 简化cookie管理，只维护必要的状态
        self.cookie = {
            '_safe': ''
        }

        # 添加实例级别的板块信息缓存，避免重复网络请求
        self._forums_cache = None
        self._forums_cache_time = 0
        self._cache_duration = 300  # 5分钟缓存

        # 代理配置优先级：环境变量 > 配置文件
        env_proxy = os.environ.get("PROXY")
        config_proxy = getattr(Config, 'PROXY', None) if hasattr(Config, 'PROXY') else None

        proxy_val = env_proxy or config_proxy
        self.proxies = {"http": proxy_val, "https": proxy_val} if proxy_val else {}

        # 输出代理配置用于调试
        if self.proxies:
            logger.info(f"[NET] 使用代理: {proxy_val}")
        else:
            logger.warning(f"! 未配置代理，可能遇到访问问题")

        self.bypass = os.environ.get("BYPASS_URL") or getattr(Config, 'BYPASS_URL', None)
        self.flare_solver = os.environ.get("FLARE_SOLVERR_URL") or getattr(Config, 'FLARE_SOLVERR_URL', None)

        # Session管理 - 保持连接和cookie持久化
        self._session = None

        # 反爬检测和自适应延迟机制
        self._consecutive_failures = 0  # 连续失败计数
        self._slow_mode = False  # 慢速模式标志
        self._failure_threshold = 3  # 连续失败阈值（3次后降级）
        self._slow_mode_delay = (1.0, 3.0)  # 慢速模式延迟范围（秒）
        self._normal_mode_delay = (0.3, 0.8)  # 正常模式延迟范围（秒）

        # 错误类型计数器 - 避免日志刷屏和被反爬
        self._error_type_counter = {}  # 记录各类错误的出现次数
        self._error_threshold = 15  # 相同错误类型的阈值
        self._should_stop_crawling = False  # 停止爬取标志

    def _get_session(self):
        """获取或创建Session实例 - 保持连接和cookie持久化"""
        if self._session is None:
            from curl_cffi.requests import Session as CurlSession
            self._session = CurlSession()
            # 设置基本参数
            self._session.timeout = 10
            logger.debug("创建新的Session实例")
        return self._session

    def _close_session(self):
        """关闭Session实例"""
        if self._session:
            try:
                self._session.close()
                logger.debug("Session已关闭")
            except:
                pass
            self._session = None

    def _record_failure(self):
        """记录失败，检查是否需要降级到慢速模式"""
        self._consecutive_failures += 1
        if not self._slow_mode and self._consecutive_failures >= self._failure_threshold:
            self._slow_mode = True
            logger.warning(f"! 检测到连续{self._consecutive_failures}次失败，降级到慢速模式（延迟{self._slow_mode_delay[0]}-{self._slow_mode_delay[1]}秒）")

    def _record_error_type(self, error_type: str) -> bool:
        """
        记录错误类型并检查是否应该停止爬取

        Args:
            error_type: 错误类型标识（如 "parse_error", "encoding_error"等）

        Returns:
            True 如果应该停止爬取，False 继续
        """
        if self._should_stop_crawling:
            return True

        # 记录错误次数
        self._error_type_counter[error_type] = self._error_type_counter.get(error_type, 0) + 1
        count = self._error_type_counter[error_type]

        # 检查是否超过阈值
        if count >= self._error_threshold:
            self._should_stop_crawling = True
            logger.error(f"⛔ [CRAWLER] 错误类型 '{error_type}' 已出现 {count} 次，超过阈值 {self._error_threshold}，停止爬取")
            logger.error(f"⚠️ [CRAWLER] 可能遇到反爬或服务异常，避免继续请求")
            return True
        elif count % 5 == 0:  # 每5次打印一次警告
            logger.warning(f"⚠️ [CRAWLER] 错误类型 '{error_type}' 已出现 {count} 次")

        return False

    def _record_success(self):
        """记录成功，重置失败计数"""
        if self._consecutive_failures > 0:
            logger.debug(f"✓ 请求成功，重置失败计数（之前连续失败{self._consecutive_failures}次）")
            self._consecutive_failures = 0
        # 如果连续成功3次，可以尝试恢复正常模式
        if self._slow_mode:
            # 简单策略：成功后立即恢复正常模式
            self._slow_mode = False
            logger.info(f"✓ 请求成功，恢复正常速度模式")

    def _adaptive_delay(self):
        """自适应延迟 - 根据模式选择延迟时间"""
        import random
        if self._slow_mode:
            delay = random.uniform(*self._slow_mode_delay)
            logger.debug(f"慢速模式延迟 {delay:.2f} 秒...")
        else:
            delay = random.uniform(*self._normal_mode_delay)
            logger.debug(f"正常延迟 {delay:.2f} 秒...")
        time.sleep(delay)

    def get_original(self, url):
        """获取原始页面内容 - 基于原始sht.py的稳定实现，提高成功率"""
        try:
            # 使用Session保持连接和cookie持久化
            session = self._get_session()

            # 使用固定参数组合，与原版保持一致
            res = session.get(url,
                             proxies=self.proxies,
                             cookies=self.cookie,
                             headers=self.headers,
                             allow_redirects=True,
                             timeout=10,
                             impersonate="chrome110")

            html = res.text.encode('utf-8')
            doc = pq(html)
            page_title = doc('head>title').text()

            # 检查CloudFlare - 使用原版的精准检测
            if 'Just a moment' in page_title:
                logger.info(f"[API] 检测到CloudFlare，尝试绕过")
                html = self.bypass_cf(url)
                if not html:
                    logger.error(f"✗ [API] CloudFlare绕过失败")
                    self._record_failure()  # 记录失败
                    return None

            # 检查18禁验证 - 使用原版的检测方式
            doc = pq(html)
            if "var safeid" in doc.text():
                logger.info(f"[API] 检测到年龄验证，尝试获取safeid")
                html = self.bypass_r18(html, url)
                if not html:
                    logger.error(f"✗ [API] 年龄验证绕过失败")
                    self._record_failure()  # 记录失败
                    return None

            # 验证页面有效性 - 使用原版的成功标志
            doc = pq(html)
            page_title = doc('head>title').text()
            # 接受多种有效页面标题：98堂论坛页面、首页、门户等
            valid_titles = ["98堂", "门户", "forum", "Discuz"]
            if any(keyword in page_title for keyword in valid_titles):
                logger.debug(f"年龄验证绕过成功")
                self._record_success()  # 记录成功
                return html
            else:
                logger.warning(f"年龄验证后页面标题异常: {page_title}")
                self._record_failure()  # 记录失败
                return None

        except Exception as e:
            logger.error(f"✗ [API] 获取页面失败: {url}, 错误: {e}")
            self._record_failure()  # 记录失败
            return None

    def bypass_cf(self, url):
        """绕过CloudFlare - 简化版本，参考sht.py"""
        if not self.flare_solver:
            logger.warning(f"! [CONFIG] 未配置FLARE_SOLVERR_URL，无法绕过Cloudflare")
            return None
            
        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": 60000,
            "proxy": {"url": self.proxies['http']} if self.proxies and 'http' in self.proxies else None,
            "cookies": [{"name": k, "value": v} for k, v in self.cookie.items()]
        }
        try:
            res = requests.post(self.flare_solver, headers={"Content-Type": "application/json"}, json=payload)
            result = res.json()
            if result['solution']['status'] != 200:
                return None
            html = result['solution']['response'].encode('utf-8')
            doc = pq(html)
            if "var safeid" in doc.text():
                safeid = extract_safeid(html)
                self.cookie['_safe'] = safeid
                return self.bypass_cf(url)  # 递归尝试
            return html
        except Exception as e:
            logger.error(f"✗ [API] 绕过Cloudflare失败: {e}")
            return None

    def bypass_r18(self, html, url):
        """绕过年龄验证 - 基于原始sht.py的实现，简化cookie管理"""
        safeid = extract_safeid(html)
        if safeid:
            # 立即更新cookie，保持状态同步
            self.cookie['_safe'] = safeid
            logger.debug(f"获取到safeid: {safeid[:8]}...")

            # 使用Session保持连接
            session = self._get_session()

            # 使用相同的请求参数重新访问
            res = session.get(url,
                             proxies=self.proxies,
                             cookies=self.cookie,
                             headers=self.headers,
                             allow_redirects=True,
                             timeout=10,
                             impersonate="chrome110")
            html = res.text.encode('utf-8')
            doc = pq(html)
            page_title = doc('head>title').text()

            # 使用原版的成功验证方式
            if "98堂" in page_title:
                logger.debug("年龄验证绕过成功")
                return html
            else:
                logger.warning(f"年龄验证后页面标题异常: {page_title}")
        else:
            logger.warning("未能提取到safeid")
        return None

    def _retry_with_delay(self, func, func_name, max_attempts=3, delay_seconds=5):
        """带重试机制的函数调用包装器

        Args:
            func: 要执行的函数
            func_name: 函数名称（用于日志）
            max_attempts: 最大尝试次数（默认3次）
            delay_seconds: 失败后的延迟时间（默认5秒）

        Returns:
            函数执行结果，如果所有尝试都失败则返回None或{}
        """
        import time

        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f" [{func_name}] 第 {attempt}/{max_attempts} 次尝试")
                result = func()

                # 如果成功获取到数据，立即返回
                if result:
                    if attempt > 1:
                        logger.info(f"✓ [{func_name}] 第 {attempt} 次尝试成功")
                    return result
                else:
                    logger.warning(f"! [{func_name}] 第 {attempt} 次尝试未获取到数据")

            except Exception as e:
                logger.error(f"✗ [{func_name}] 第 {attempt} 次尝试出错: {e}")

            # 如果不是最后一次尝试，等待后重试
            if attempt < max_attempts:
                logger.info(f"⏳ [{func_name}] {delay_seconds}秒后进行第 {attempt + 1} 次尝试...")
                time.sleep(delay_seconds)

        logger.error(f"✗ [{func_name}] 所有 {max_attempts} 次尝试均失败")
        return {} if isinstance(result, dict) else None

    def get_all_forums_info(self):
        """获取所有板块信息 - 双重策略：优先手机版列表页，失败则尝试桌面版首页"""
        import time

        # 检查实例缓存是否有效
        current_time = time.time()
        if self._forums_cache and (current_time - self._forums_cache_time) < self._cache_duration:
            logger.info(f"✓ 使用实例缓存的板块信息（{int(self._cache_duration - (current_time - self._forums_cache_time))}秒后过期）")
            return self._forums_cache

        logger.info("[CRAWLER] 开始获取所有板块信息（准确数据，双重策略，带重试机制）")

        # 【策略A】优先尝试手机版板块列表页（带重试机制：最多3次，间隔5秒）
        forums_info = self._retry_with_delay(
            func=self._get_forums_from_mobile_list,
            func_name="策略A-手机版列表页",
            max_attempts=3,
            delay_seconds=5
        )

        # 【策略B】如果手机版失败，尝试桌面版首页作为备份（带重试机制）
        if not forums_info:
            logger.warning(f"! 手机版板块列表页获取失败，尝试桌面版首页作为备份")
            forums_info = self._retry_with_delay(
                func=self._get_forums_from_desktop_home,
                func_name="策略B-桌面版首页",
                max_attempts=3,
                delay_seconds=5
            )

        # 如果两种方法都失败，使用预定义列表
        if not forums_info:
            logger.warning(f"! 两种方法均失败，使用预定义板块列表")
            forums_info = self._get_default_forums()

        # 更新缓存
        self._forums_cache = forums_info
        self._forums_cache_time = current_time

        return forums_info

    def _get_forums_from_mobile_list(self):
        """策略A：从手机版板块列表页提取准确数据"""
        try:
            # 只获取预设的11个板块
            from constants import VALID_FIDS
            
            logger.info(f"[CRAWLER] [策略A] 尝试从手机版板块列表页获取（仅限{len(VALID_FIDS)}个预设板块）")

            url = "https://sehuatang.org/forum.php?forumlist=1&mobile=2"
            html = self.get_original(url)

            if not html:
                logger.warning(f"! [策略A] 无法获取板块列表页")
                return {}

            doc = pq(html)
            forum_items = doc('div.sub_forum ul li')

            if len(forum_items) == 0:
                logger.warning(f"! [策略A] 未找到板块列表项")
                return {}

            logger.debug(f"[CRAWLER] [策略A] 在板块列表页找到 {len(forum_items)} 个板块，筛选预设板块")
            forums_info = {}

            for item in forum_items:
                try:
                    item_pq = pq(item)

                    # 提取板块链接和fid
                    link_elem = item_pq('a.btdb').eq(0)
                    if not link_elem:
                        continue

                    href = link_elem.attr('href')
                    if not href:
                        continue

                    fid_match = re.search(r'fid=(\d+)', href)
                    if not fid_match:
                        continue

                    fid = fid_match.group(1)

                    # 只处理预设的板块
                    if fid not in VALID_FIDS:
                        continue

                    # 提取板块名称（去除<span class="num">）
                    name_text = link_elem.text().strip()
                    num_span = link_elem.find('span.num')
                    if num_span:
                        num_text = num_span.text().strip()
                        name = name_text.replace(num_text, '').strip()
                    else:
                        name = name_text

                    if not name:
                        continue

                    # 提取主题数：<i>主题:<span title="41167">4万</span> 帖数:...</i>
                    total_topics = None
                    stats_elem = item_pq('i').eq(0)
                    if stats_elem:
                        stats_text = stats_elem.text()
                        if '主题' in stats_text:
                            # 优先从span title提取
                            topic_spans = stats_elem.find('span[title]')
                            for span in topic_spans:
                                span_pq = pq(span)
                                span_html = stats_elem.html()
                                span_outer = span_pq.outerHtml()

                                span_index = span_html.find(span_outer) if span_outer else -1
                                if span_index > 0:
                                    before_text = span_html[:span_index]
                                    if '主题' in before_text and before_text.rfind('主题') > before_text.rfind('帖数'):
                                        title_value = span_pq.attr('title')
                                        if title_value and title_value.isdigit():
                                            total_topics = int(title_value)
                                            logger.debug(f"✅ [策略A] [{name}] 从title属性提取: {total_topics}")
                                            break

                            # 备用：从文本提取
                            if total_topics is None:
                                topic_match = re.search(r'主题[：:]\s*(\d+)', stats_text)
                                if topic_match:
                                    match_pos = topic_match.start()
                                    if '主题' in stats_text[:match_pos + 10]:
                                        total_topics = int(topic_match.group(1))
                                        logger.debug(f"✅ [策略A] [{name}] 从文本提取: {total_topics}")

                    forums_info[fid] = {
                        'fid': fid,
                        'name': name,
                        'total_topics': total_topics,
                        'total_pages': None,
                    }

                    if total_topics is not None:
                        logger.info(f"[CRAWLER] [策略A] {name} (fid={fid}) - {total_topics}主题")

                except Exception as e:
                    logger.debug(f"! [策略A] 解析板块项失败: {e}")
                    continue

            if forums_info:
                logger.info(f"✓ [策略A] 成功获取 {len(forums_info)} 个板块信息")
            return forums_info

        except Exception as e:
            logger.error(f"✗ [策略A] 失败: {e}")
            return {}

    def _get_forums_from_desktop_home(self):
        """策略B：从桌面版首页提取准确数据（备用方案）"""
        try:
            # 只获取预设的11个板块
            from constants import VALID_FIDS
            
            logger.info(f"[CRAWLER] [策略B] 尝试从桌面版首页获取（仅限{len(VALID_FIDS)}个预设板块）")

            # 临时切换到桌面版UA
            original_ua = self.headers.get('User-Agent')
            desktop_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            self.headers['User-Agent'] = desktop_ua

            try:
                url = "https://sehuatang.org/forum.php"
                html = self.get_original(url)
            finally:
                self.headers['User-Agent'] = original_ua

            if not html:
                logger.warning(f"! [策略B] 无法获取首页")
                return {}

            doc = pq(html)
            forum_cards = doc('td.fl_g')

            if len(forum_cards) == 0:
                logger.warning(f"! [策略B] 未找到板块卡片")
                return {}

            logger.debug(f"📋 [策略B] 在首页找到 {len(forum_cards)} 个板块卡片，筛选预设板块")
            forums_info = {}

            for card in forum_cards:
                try:
                    card_pq = pq(card)

                    # 提取板块链接和名称
                    link_elem = card_pq('dt a').eq(0)
                    if not link_elem:
                        continue

                    name = link_elem.text().strip()
                    href = link_elem.attr('href')

                    if not href or not name:
                        continue

                    # 从href提取fid: forum-2-1.html -> fid=2
                    fid_match = re.search(r'forum-(\d+)-', href)
                    if not fid_match:
                        continue

                    fid = fid_match.group(1)

                    # 只处理预设的板块
                    if fid not in VALID_FIDS:
                        continue

                    # 提取主题数：<dd><em>主题: <span title="68250">6万</span></em>...
                    total_topics = None
                    topic_dd = card_pq('dd').eq(0)
                    if topic_dd:
                        topic_text = topic_dd.text()
                        if '主题' in topic_text:
                            # 优先从span title提取
                            topic_spans = topic_dd.find('span[title]')
                            for span in topic_spans:
                                span_pq = pq(span)
                                parent_text = span_pq.parent().text()

                                if '主题' in parent_text and '帖数' not in parent_text:
                                    title_value = span_pq.attr('title')
                                    if title_value and title_value.isdigit():
                                        total_topics = int(title_value)
                                        logger.debug(f"✅ [策略B] [{name}] 从title属性提取: {total_topics}")
                                        break

                            # 备用：从文本提取
                            if total_topics is None:
                                topic_match = re.search(r'主题[：:]\s*(\d+)', topic_text)
                                if topic_match:
                                    total_topics = int(topic_match.group(1))
                                    logger.debug(f"✅ [策略B] [{name}] 从文本提取: {total_topics}")

                    forums_info[fid] = {
                        'fid': fid,
                        'name': name,
                        'total_topics': total_topics,
                        'total_pages': None,
                    }

                    if total_topics is not None:
                        logger.info(f"📋 [策略B] {name} (fid={fid}) - {total_topics}主题")

                except Exception as e:
                    logger.debug(f"⚠️ [策略B] 解析板块卡片失败: {e}")
                    continue

            if forums_info:
                logger.info(f"✓ [策略B] 成功获取 {len(forums_info)} 个板块信息")
            return forums_info

        except Exception as e:
            logger.error(f"✗ [策略B] 失败: {e}")
            return {}

    def _get_default_forums(self):
        """获取预定义的板块列表"""
        default_forums = {
            '2': '国产原创',
            '36': '亚洲无码原创', 
            '37': '亚洲有码原创',
            '103': '高清中文字幕',
            '107': '三级写真',
            '160': 'VR视频区',
            '104': '素人有码系列',
            '38': '欧美无码',
            '151': '4K原版',
            '152': '韩国主播',
            '39': '动漫原创'
        }
        
        forums_info = {}
        for fid, name in default_forums.items():
            forums_info[fid] = {
                'fid': fid,
                'name': name,
                'total_topics': None,  # 不使用估算值，返回None表示需要同步
                'total_pages': None    # 不使用估算值，返回None表示需要同步
            }

        return forums_info
    def _test_topic_parsing(self, test_html):
        """测试主题数解析逻辑"""
        try:
            # 模拟用户提供的HTML结构
            stats_text = "主题:2427 帖数:7万"
            stats_html = test_html
            
            # 方法1: 直接从文本中提取 "主题:数字" (最可靠)
            topic_match = re.search(r'主题[：:]\s*(\d+)', stats_text)
            if topic_match:
                return f"成功提取主题数: {topic_match.group(1)}"
            
            # 方法2: 从HTML中提取，但要避免span title中的数字
            clean_html = re.sub(r'<span[^>]*title="[^"]*"[^>]*>.*?</span>', '', stats_html)
            topic_match_clean = re.search(r'主题[：:]\s*(\d+)', clean_html)
            if topic_match_clean:
                return f"从清理HTML提取主题数: {topic_match_clean.group(1)}"
            
            return "解析失败"
        except Exception as e:
            return f"解析异常: {e}"

    def get_forum_info(self, fid, all_forums_cache=None):
        """获取单个板块信息：总页数、主题数量等

        Args:
            fid: 板块ID
            all_forums_cache: 可选的板块信息缓存，避免重复获取
        """
        logger.debug(f" 获取板块信息: fid={fid}")

        try:
            # 优先从首页获取准确的主题数，而不是从单个板块页面推断
            # 如果没有传入缓存，则调用get_all_forums_info()获取首页数据
            if not all_forums_cache:
                logger.debug("📋 未传入缓存，从首页获取所有板块信息")
                all_forums_cache = self.get_all_forums_info()

            # 使用首页获取的准确数据
            if all_forums_cache and fid in all_forums_cache:
                forum_info = {
                    'fid': fid,
                    'name': all_forums_cache[fid]['name'],
                    'total_topics': all_forums_cache[fid].get('total_topics'),  # 使用首页的准确数据
                    'total_pages': all_forums_cache[fid].get('total_pages')     # 可能为None
                }
                logger.debug(f"✅ 从首页数据获取到准确信息: total_topics={forum_info['total_topics']}")
            else:
                # 尝试从持久化缓存获取
                cat = Category.query.filter_by(fid=str(fid)).first()
                cached_forum = cat.to_dict() if cat else None

                if cached_forum:
                    forum_info = {
                        'fid': fid,
                        'name': cached_forum['name'],
                        'total_topics': cached_forum.get('total_topics'),
                        'total_pages': cached_forum.get('total_pages')
                    }
                    logger.debug(f"✅ 从持久化缓存获取到信息: {forum_info}")
                else:
                    # 如果缓存中没有找到，使用默认值
                    forum_info = {
                        'fid': fid,
                        'name': f'板块{fid}',
                        'total_topics': None,
                        'total_pages': None
                    }
                    logger.warning(f"! 缓存中未找到fid={fid}，使用默认信息")

            # 只在需要时获取页数信息（首页没有页数，需要单独获取）
            # 如果主题数为None，说明首页也没获取到，需要访问板块页面
            if forum_info['total_pages'] is None:
                url = f"https://sehuatang.org/forum.php?mod=forumdisplay&fid={fid}&mobile=2"
                logger.debug(f" 获取页数信息: {url}")

                html = self.get_original(url)
                if html:
                    doc = pq(html)

                    pages_found = False

                    # 方法1: 使用用户提供的具体选择器获取总页数
                    page_span = doc('#fd_page_top > div > label > span')
                    if page_span and page_span.length > 0:
                        page_text = page_span.text()
                        logger.debug(f"📄 页数文本: {page_text}")

                        # 从 "/ 2272 页" 格式中提取页数
                        page_match = re.search(r'/\s*(\d+)\s*页', page_text)
                        if page_match:
                            forum_info['total_pages'] = int(page_match.group(1))
                            logger.debug(f"✅ 从页数span提取总页数: {forum_info['total_pages']}")
                            pages_found = True
                        else:
                            # 备用方法：从title属性提取
                            title_attr = page_span.attr('title')
                            if title_attr:
                                title_match = re.search(r'共\s*(\d+)\s*页', title_attr)
                                if title_match:
                                    forum_info['total_pages'] = int(title_match.group(1))
                                    logger.debug(f"✅ 从title属性提取总页数: {forum_info['total_pages']}")
                                    pages_found = True

                    # 方法2: 如果上述方法都失败，使用通用分页选择器
                    if not pages_found:
                        pg_elements = doc('div.pg, .pg, [class*="pg"], .pages, .pagination')
                        for pg_element in pg_elements:
                            pg_pq = pq(pg_element)
                            page_text = pg_pq.text()
                            logger.debug(f"📄 备用分页文本: {page_text}")

                            # 查找 "/ XX 页" 的文本
                            page_match = re.search(r'/\s*(\d+)\s*页', page_text)
                            if page_match:
                                forum_info['total_pages'] = int(page_match.group(1))
                                logger.debug(f"✅ 备用方法解析总页数: {forum_info['total_pages']}")
                                pages_found = True
                                break

                            # 查找最后一页的链接
                            last_links = pg_pq.find('a.last, a[class*="last"], a:contains("末页"), a:contains("最后")')
                            for last_link in last_links:
                                last_href = pq(last_link).attr('href')
                                if last_href:
                                    page_match = re.search(r'page=(\d+)', last_href)
                                    if not page_match:
                                        page_match = re.search(r'-(\d+)\.html', last_href)
                                    if page_match:
                                        forum_info['total_pages'] = int(page_match.group(1))
                                        logger.debug(f"✅ 从链接解析总页数: {forum_info['total_pages']}")
                                        pages_found = True
                                        break

                            if pages_found:
                                break

                    # 方法3: 查找所有包含页数的链接
                    if not pages_found:
                        all_links = doc('a[href*="page="], a[href*="forum.php"]')
                        max_page = 0
                        for link in all_links:
                            href = pq(link).attr('href')
                            if href:
                                page_match = re.search(r'page=(\d+)', href)
                                if page_match:
                                    page_num = int(page_match.group(1))
                                    if page_num > max_page:
                                        max_page = page_num

                        if max_page > 1:
                            forum_info['total_pages'] = max_page
                            logger.debug(f"✅ 从链接中找到最大页数: {forum_info['total_pages']}")
                            pages_found = True

                    if not pages_found:
                        logger.warning(f"! 未找到页数信息，保持为None")
                else:
                    logger.warning(f"! 无法获取板块页面，页数保持为None")

            # 构建显示文本
            if forum_info['total_topics'] is None:
                topics_display = "未知"
            else:
                topics_display = str(forum_info['total_topics'])

            if forum_info['total_pages'] is None:
                pages_display = "未知"
            else:
                pages_display = str(forum_info['total_pages'])

            logger.info(f"📋 板块信息 [{forum_info['name']}]: 总计{topics_display}主题, 共{pages_display}页")

            return forum_info
            
        except Exception as e:
            logger.error(f"✗ 获取板块信息失败: fid={fid}, 错误: {e}")
            logger.debug(f" 详细错误信息", exc_info=True)
            return None

    def crawler_tid_list(self, url):
        """爬取页面中的tid列表 - 基于原始sht.py的稳定实现，增加重试机制"""
        # 添加重试机制，提高成功率
        for retry in range(3):
            try:
                html = self.get_original(url)
                if html:
                    doc = pq(html)
                    # 使用原版的精确选择器
                    items = doc("div.n5_htnrys.cl")[1:]  # 跳过第一个元素（通常是标题行）
                    id_list = []
                    for item in items:
                        pq_item = pq(item)
                        link = pq_item("div a").eq(0).attr('href')  # 提取href属性
                        if link:
                            parsed_url = urlparse(link)
                            query_params = parse_qs(parsed_url.query)  # 解析为字典（值为列表）
                            tid = query_params.get('tid', [''])[0]
                            if tid and tid.isdigit():
                                id_list.append(int(tid))
                    
                    if id_list:
                        logger.debug(f"成功提取到 {len(id_list)} 个tid")
                        return id_list
                    else:
                        logger.warning(f"页面无有效tid，重试 {retry + 1}/3")
                else:
                    logger.warning(f"获取页面失败，重试 {retry + 1}/3")
                    
            except Exception as e:
                logger.warning(f"爬取{url}失败，重试 {retry + 1}/3: {e}")
                
            # 重试前短暂延迟
            if retry < 2:
                import time
                time.sleep(1)
        
        logger.error(f"连续3次爬取失败: {url}")
        return []
    
    def _fix_mobile_session_and_retry(self, original_url):
        """修复会话问题 - 改用桌面版访问策略"""
        logger.info("🔧 开始修复会话问题...")
        
        try:
            import time
            import random
            
            # 1. 先访问桌面版论坛首页
            desktop_forum_url = "https://sehuatang.org/forum.php"
            logger.info(f"🖥️ 访问桌面版论坛首页: {desktop_forum_url}")
            
            forum_html = self.get_original(desktop_forum_url)
            if not forum_html:
                logger.error(f"✗ 桌面版论坛首页访问失败")
                return None
            
            logger.info(f"✓ 桌面版论坛首页访问成功，长度: {len(forum_html)}")
            
            # 2. 等待一段时间，模拟用户浏览
            delay = random.uniform(3, 6)
            logger.debug(f"😴 等待 {delay:.1f} 秒，模拟用户浏览...")
            time.sleep(delay)
            
            # 3. 将原始URL转换为桌面版URL（移除mobile=2参数）
            desktop_url = original_url.replace('&mobile=2', '').replace('mobile=2&', '').replace('mobile=2', '')
            
            # 如果URL中有backforums=1，也移除它，因为这可能是手机版特有的
            desktop_url = desktop_url.replace('&backforums=1', '').replace('backforums=1&', '').replace('backforums=1', '')
            
            logger.info(f" 使用桌面版URL重新访问: {desktop_url}")
            
            retry_html = self.get_original(desktop_url)
            if not retry_html:
                logger.error(f"✗ 桌面版URL访问失败")
                return None
            
            # 4. 检查是否修复成功
            if len(retry_html) > 20000:  # 正常页面应该比较大
                logger.info(f"✓ 会话修复成功，获取到正常桌面版页面")
                return retry_html
            elif "手机版" in retry_html and "现在就登录" in retry_html:
                logger.warning(f"! 仍然是手机版引导页面，修复失败")
                return None
            else:
                logger.info(f"✓ 会话可能已修复，返回新内容")
                return retry_html
                
        except Exception as e:
            logger.error(f"✗ 会话修复过程中出现异常: {e}")
            return None
    
    def _extract_tids_with_regex(self, html: str, url: str) -> List[int]:
        """使用正则表达式提取tid - 备用方法"""
        logger.debug("🔧 使用正则表达式备用方法提取tid")
        
        try:
            import re
            
            # 查找所有包含tid的链接
            tid_pattern = r'mod=viewthread&tid=(\d+)'
            matches = re.findall(tid_pattern, html)
            
            id_list = []
            for match in matches:
                try:
                    tid_int = int(match)
                    if tid_int not in id_list:  # 去重
                        id_list.append(tid_int)
                except ValueError:
                    continue
            
            logger.info(f"📋 正则表达式方法提取到 {len(id_list)} 个tid: {url}")
            return id_list
            
        except Exception as e:
            logger.error(f"✗ 正则表达式提取失败: {e}")
            return []

    def crawler_detail(self, url):
        """爬取单个详情页 - 基于原始sht.py的实现，优化数据提取稳定性"""
        try:
            html = self.get_original(url)
            if html:
                doc = pq(html)
                
                # 使用原版的精确磁力链接提取方式
                all_text = doc('div.blockcode').text()
                magnet_pattern = r'magnet:\?xt=urn:btih:[0-9a-fA-F]+'
                match = re.search(magnet_pattern, all_text)
                magnet = None
                
                if match:
                    magnet = match.group()
                    logger.debug(f"找到磁力链接: {url}")
                
                # 备用torrent处理 - 保持原版逻辑
                if not magnet:
                    torrent = doc("a:contains('.torrent')").eq(0)
                    if torrent:
                        torrent_url = torrent.attr('href')
                        logger.debug(f"尝试解析torrent文件: {url}")
                        magnet = self.parse_torrent_get_magnet(url, f"https://sehuatang.org/{torrent_url}")
                
                if magnet:
                    # 提取其他信息 - 使用原版的选择器
                    date = extract_exact_datetime(html)
                    size = extract_and_convert_video_size(html)
                    sub_type = extract_bracket_content(html)
                    
                    # 标题处理 - 保持原版逻辑
                    title = doc('h2.n5_bbsnrbt').text()
                    pattern = r"^\[.*?\]"
                    title = re.sub(pattern, "", title).strip()
                    
                    # 预览图片提取
                    img_elements = doc('div.message img')
                    img_src_list = []
                    for img in img_elements.items():
                        src = img.attr('src')
                        if src:
                            img_src_list.append(src.strip())
                    
                    result = {
                        "title": title,
                        "sub_type": sub_type,
                        "publish_date": date,
                        "magnet": magnet,
                        "preview_images": ",".join(img_src_list),
                        "size": size
                    }
                    
                    logger.debug(f"解析成功: {url}, 标题: {title[:50]}...")
                    return result
                else:
                    logger.warning(f"未找到磁力链接: {url}")
                    return {}
            else:
                logger.warning(f"获取页面失败: {url}")
                return {}
                
        except Exception as e:
            logger.error(f"解析详情页失败: {url}, 错误: {e}")
            return {}

    def parse_torrent_get_magnet(self, refer, torrent_source, is_local=False):
        """解析torrent文件获取磁力链接 - 基于原始sht.py的实现，增强错误处理"""
        try:
            import bencodepy
            import hashlib
            import binascii
            from urllib.parse import urlencode
            
            torrent_bin = None
            if is_local:
                with open(torrent_source, "rb") as f:
                    torrent_bin = f.read()
                if len(torrent_bin) == 0:
                    logger.error("错误：本地 torrent 文件为空")
                    return None
            else:
                # 使用与get_original相同的请求参数，保持一致性
                header = self.headers.copy()
                header['Referer'] = refer
                resp = requests.get(
                    torrent_source,
                    proxies=self.proxies,
                    cookies=self.cookie,
                    headers=header,
                    allow_redirects=True,
                    timeout=10,
                    impersonate="chrome110"
                )
                resp.raise_for_status()
                torrent_bin = resp.content
                if len(torrent_bin) < 100:
                    logger.warning(f"警告：下载内容过小（{len(torrent_bin)} 字节），非合法 torrent 文件")
                    return None

            # 使用bencodepy解析torrent文件
            torrent_dict = bencodepy.decode(torrent_bin)
            info_dict = None
            if b"info" in torrent_dict:
                info_dict = torrent_dict[b"info"]
            elif "info" in torrent_dict:
                info_dict = torrent_dict["info"]
            else:
                logger.error("错误：种子缺少 info 核心字段（非合法 torrent 文件）")
                return None

            # 计算info hash
            info_bin = bencodepy.encode(info_dict)
            info_hash = hashlib.sha1(info_bin).digest()
            info_hash_hex = binascii.hexlify(info_hash).decode("utf-8")

            # 提取torrent名称
            torrent_name = "Unknown_Torrent"
            if b"name" in info_dict:
                torrent_name = info_dict[b"name"]
            elif "name" in info_dict:
                torrent_name = info_dict["name"]

            # 处理编码
            if isinstance(torrent_name, bytes):
                try:
                    torrent_name = torrent_name.decode("utf-8")
                except UnicodeDecodeError:
                    torrent_name = torrent_name.decode("utf-8", errors="ignore")
            elif isinstance(torrent_name, str):
                pass
            else:
                torrent_name = str(torrent_name)
            
            # 构建磁力链接
            encoded_name = urlencode({"dn": torrent_name})[3:]
            magnet_link = f"magnet:?xt=urn:btih:{info_hash_hex}&dn={encoded_name}"
            
            logger.debug(f"成功解析torrent文件: {torrent_name[:50]}...")
            return magnet_link
            
        except Exception as e:
            logger.error(f"解析torrent文件失败：{e}")
            return None
    
    def crawler_details_batch(self, urls: List[str], use_batch_mode: bool = False) -> List[Dict]:
        """批量爬取详情页 - 可选择批量或单个处理模式"""
        logger.info(f" 开始{'批量' if use_batch_mode else '单个'}爬取 {len(urls)} 个详情页")

        # 每次批量爬取开始时重置状态
        self._consecutive_failures = 0
        self._slow_mode = False
        self._error_type_counter = {}  # v1.3.0: 重置错误类型计数器
        self._should_stop_crawling = False  # v1.3.0: 重置停止标志
        logger.info(f"[CRAWLER] 初始化爬取状态：正常模式，延迟{self._normal_mode_delay[0]}-{self._normal_mode_delay[1]}秒")

        try:
            if use_batch_mode:
                # 批量处理模式
                try:
                    # 使用内部定义的 BatchProcessor
                    # from fast_crawler import batch_processor (已移除)

                    # 获取配置的线程数和延迟
                    from configuration import config_manager
                    max_workers = config_manager.get('CRAWLER_THREAD_COUNT', 10)
                    delay_min = config_manager.get('CRAWLER_SYNC_DELAY_MIN', 0.3)
                    delay_max = config_manager.get('CRAWLER_SYNC_DELAY_MAX', 0.8)
                    
                    # 创建新的处理器实例 - 修正参数传递错误，明确指定 batch_size 和 max_workers
                    # 同时传入配置的随机延迟，确保线程模式也具备防封能力
                    local_batch_processor = BatchProcessor(
                        batch_size=max_workers, 
                        max_workers=max_workers,
                        delay_min=delay_min,
                        delay_max=delay_max
                    )

                    def process_detail_html(url: str, html: str) -> Dict:
                        """处理详情页HTML - 使用原版的稳定解析逻辑"""
                        return self._parse_detail_html_stable(url, html)

                    # 批量处理，使用与原版相同的请求参数
                    results = local_batch_processor.process_urls_in_batches(
                        urls,
                        process_detail_html,
                        headers=self.headers,
                        cookies=self.cookie,
                        proxies=self.proxies
                    )

                    # 关闭处理器
                    local_batch_processor.close()

                    # 确保结果数量与输入URL数量一致，失败的位置用None填充

                    # 确保结果数量与输入URL数量一致，失败的位置用None填充
                    if len(results) != len(urls):
                        logger.warning(f"结果数量不匹配: 输入{len(urls)}个URL，返回{len(results)}个结果")
                        # 补齐到相同长度
                        while len(results) < len(urls):
                            results.append(None)

                    # 统计有效结果
                    valid_count = sum(1 for r in results if r and r.get('magnet'))

                    logger.info(f"✓ 批量爬取完成: 成功 {valid_count}/{len(urls)}")

                    # 输出统计信息
                    try:
                        stats = local_batch_processor.get_crawler_stats()
                        logger.info(f"[STATS] 爬取统计: 平均响应时间 {stats['avg_response_time']:.2f}s, "
                                   f"成功率 {stats['successful_requests']}/{stats['total_requests']}")
                    except:
                        pass

                    return results  # 返回完整结果列表，包含None

                except Exception as e:
                    logger.error(f"批量爬取失败: {e}")
                    logger.info("降级到单个处理模式")
                    use_batch_mode = False  # 降级到单个模式

            if not use_batch_mode:
                # 单个处理模式 - 更稳定可靠
                logger.info("使用单个处理模式（更稳定）")
                results = []
                success_count = 0

                for i, url in enumerate(urls):
                    # 检查是否应该停止爬取
                    if self._should_stop_crawling:
                        logger.error(f"⛔ [CRAWLER] 检测到停止标志，剩余 {len(urls) - i} 个URL未爬取")
                        # 填充剩余的None
                        results.extend([None] * (len(urls) - i))
                        break

                    try:
                        result = self.crawler_detail(url)
                        if result and result.get('magnet'):
                            results.append(result)
                            success_count += 1
                        else:
                            results.append(None)

                        # 进度日志
                        if (i + 1) % 10 == 0 or (i + 1) == len(urls):
                            logger.info(f"单个处理进度: {i + 1}/{len(urls)}, 成功: {success_count}")
                            # 显示当前模式
                            mode_info = "慢速模式" if self._slow_mode else "正常模式"
                            logger.info(f"[CRAWLER] 当前爬取模式: {mode_info}，连续失败次数: {self._consecutive_failures}")

                        # 使用自适应延迟，根据失败情况动态调整
                        if i < len(urls) - 1:  # 最后一个不需要延迟
                            self._adaptive_delay()

                    except Exception as detail_e:
                        logger.warning(f"单个处理失败: {url}, 错误: {detail_e}")
                        results.append(None)

                logger.info(f"✓ 单个处理完成: 成功 {success_count}/{len(urls)}")
                return results
        finally:
            # 爬取结束时清理Session
            self._close_session()
            logger.debug("爬取完成，已清理Session")
    
    def _parse_detail_html_stable(self, url: str, html) -> Dict:
        """解析详情页HTML内容 - 基于原版sht.py的稳定解析逻辑"""
        try:
            import re  # 确保re模块在方法开始时就导入

            # 处理bytes输入 - 转换为字符串
            if isinstance(html, bytes):
                html = html.decode('utf-8', errors='ignore')

            # 基本检查
            if not html or len(html) < 100:
                logger.warning(f"页面内容过短或为空: {url}, 长度: {len(html) if html else 0}")
                return {}

            # 移除 XML/HTML encoding 声明（pyquery 不支持带声明的 Unicode 字符串）
            # 这修复了 "Unicode strings with encoding declaration are not supported" 错误
            html = re.sub(r'<\?xml[^>]+\?>', '', html)

            # 检查常见错误页面 - 使用原版的检测方式
            if "抱歉，指定的主题不存在或已被删除或正在被审核" in html:
                logger.warning(f"主题不存在或已删除: {url}")
                return {}

            if "您无权进行当前操作" in html:
                logger.warning(f"无权访问页面: {url}")
                return {}

            # 检查是否需要年龄验证
            if "var safeid" in html:
                logger.warning(f"页面需要年龄验证: {url}")
                return {}

            # 检查页面标题确认有效性
            doc = pq(html)
            page_title = doc('head>title').text()
            if "98堂" not in page_title:
                logger.warning(f"页面标题异常: {url}, 标题: {page_title}")
                return {}
            
            # 检查标题
            title = doc('h2.n5_bbsnrbt').text()
            if not title:
                logger.warning(f"未找到标题: {url}")
                return {}
            
            # 使用原版的精确磁力链接提取方式
            all_text = doc('div.blockcode').text()
            magnet_pattern = r'magnet:\?xt=urn:btih:[0-9a-fA-F]+'
            match = re.search(magnet_pattern, all_text)
            magnet = None
            
            if match:
                magnet = match.group()
                logger.debug(f"找到磁力链接: {url}")
            else:
                # 尝试查找torrent链接 - 保持原版逻辑
                torrent = doc("a:contains('.torrent')").eq(0)
                if torrent:
                    torrent_url = torrent.attr('href')
                    logger.debug(f"找到torrent链接，尝试解析: {url}")
                    magnet = self.parse_torrent_get_magnet(url, f"https://sehuatang.org/{torrent_url}")
                else:
                    logger.warning(f"未找到磁力链接或torrent链接: {url}")
                    logger.debug(f"blockcode内容长度: {len(all_text)}, 预览: {all_text[:100]}...")
            
            if magnet:
                # 提取其他信息 - 使用原版的函数
                date = extract_exact_datetime(html)
                size = extract_and_convert_video_size(html)
                sub_type = extract_bracket_content(html)
                
                # 清理标题 - 保持原版逻辑
                pattern = r"^\[.*?\]"
                title = re.sub(pattern, "", title).strip()
                
                # 提取预览图片
                img_elements = doc('div.message img')
                img_src_list = []
                for img in img_elements.items():
                    src = img.attr('src')
                    if src:
                        img_src_list.append(src.strip())
                
                result = {
                    "title": title,
                    "sub_type": sub_type,
                    "publish_date": date,
                    "magnet": magnet,
                    "preview_images": ",".join(img_src_list),
                    "size": size
                }
                
                logger.debug(f"解析成功: {url}, 标题: {title[:50]}...")
                return result
            else:
                logger.warning(f"页面解析失败，无磁力链接: {url}")
                return {}
                
        except Exception as e:
            error_msg = str(e)
            # 识别错误类型
            if "Unicode strings with encoding declaration" in error_msg:
                error_type = "encoding_declaration_error"
            elif "ValueError" in str(type(e)):
                error_type = "value_error"
            elif "PyQueryError" in str(type(e)) or "etree" in error_msg:
                error_type = "parse_error"
            else:
                error_type = "unknown_error"

            # 记录错误类型并检查是否应该停止
            should_stop = self._record_error_type(error_type)
            if should_stop:
                logger.error(f"解析详情页失败 ({error_type}): {url}, 错误: {e}")
                logger.error(f"⛔ 已停止爬取，避免继续请求")
            else:
                logger.error(f"解析详情页失败 ({error_type}): {url}, 错误: {e}")
                import traceback
                logger.debug(f"详细错误信息: {traceback.format_exc()}")

        return {}

    def save_to_db(self, data, section, tid, detail_url):
        """将爬取的数据保存到数据库 - 优化版本，增加数据库锁重试机制"""
        # 调用实际的保存方法
        return self._save_to_db(data, tid, section, detail_url)

    @retry_on_lock(max_retries=3, initial_delay=0.5)
    def _save_to_db(self, data: Dict, tid: int, section: str = None, detail_url: str = None) -> bool:
        """保存数据到数据库 (包含自动重试机制)"""
        # 延迟导入防止循环引用
        from models import db, Resource
        
        try:
            # 兼容性处理：创建应用上下文
            # 注意：由于我们在类初始化时已经有了app，这里可以直接使用
            # 但为了保险起见，我们使用 get_flask_app_context
            
            # 检查验证配置
            # 验证数据
            try:
                # 执行单条记录验证
                validation_result = validator._validate_single(tid, detail_url, data)

                if not validation_result['valid']:
                    logger.warning(f"保存前验证失败: tid={tid}, 原因: {', '.join(validation_result['reasons'])}")
                    # 默认策略：仅警告，继续保存
            except Exception as e:
                # 验证过程不应阻塞保存
                logger.warning(f"保存前验证异常: {e}，继续保存")

            # 检查是否已存在（修复 bug：正确的 exists 查询）
            existing_resource = Resource.query.filter_by(tid=tid).first()
            if existing_resource:
                logger.debug(f"资源已存在，跳过: tid={tid}")
                return False
            
            # 数据清理和标准化
            title = data.get('title', '').strip()[:500]  # 限制长度并去除空白
            sub_type = data.get('sub_type', '').strip()[:200] if data.get('sub_type') else None
            publish_date = self._normalize_date(data.get('publish_date', ''))
            
            # 🔍 TID与标题的最终一致性检查
            if detail_url and f"tid={tid}" not in detail_url:
                logger.warning(f"! TID与URL不匹配: tid={tid}, url={detail_url}")
            
            # 创建新资源记录
            resource = Resource(
                title=title,
                sub_type=sub_type,
                publish_date=publish_date,
                magnet=data.get('magnet'),
                preview_images=data.get('preview_images'),
                size=data.get('size'),
                tid=tid,
                section=section[:100] if section else None,
                detail_url=detail_url[:500] if detail_url else None
            )
            
            # 使用add添加
            db.session.add(resource)
            db.session.commit()

            # 清除相关缓存
            try:
                from cache_manager import cache_manager, CacheKeys
                cache_manager.delete(CacheKeys.STATS)  # 清除统计缓存
                cache_manager.delete(CacheKeys.CATEGORIES)  # 清除分类缓存
                logger.debug(f"已清除相关缓存: tid={tid}")
            except Exception as e:
                logger.warning(f"清除缓存失败: {e}")

            logger.info(f"✓ 成功保存资源: tid={tid}, title={title[:50]}...")
            return True
            
        except ImportError:
            # 忽略导入错误
            pass
        except Exception as e:
            # 关键：如果是数据库锁定错误，重新抛出让装饰器处理
            if "database is locked" in str(e).lower():
                raise e
                
            # 其他错误记录并返回失败
            logger.error(f"保存资源到数据库失败: tid={tid}, 错误: {e}")
            try:
                db.session.rollback()
            except:
                pass
            return False
    
    def _normalize_date(self, date_str):
        """标准化日期格式"""
        if not date_str:
            return None
        
        # 如果已经是标准格式，直接返回
        if len(date_str) == 10 and date_str.count('-') == 2:
            return date_str
        
        # 这里可以添加更多日期格式转换逻辑
        # 例如：将 "2024年1月1日" 转换为 "2024-01-01"
        
        return date_str[:20]  # 限制长度
