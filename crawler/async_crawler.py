#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SHT 异步爬虫模块 (v1.3.0)
提供高性能并发爬取能力，使用 httpx 实现异步HTTP请求
"""

from __future__ import annotations  # 启用延迟类型注解评估，避免循环导入

import asyncio
from curl_cffi.requests import AsyncSession
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from collections import deque
import logging
import time
import random
from pyquery import PyQuery as pq
import re
from urllib.parse import urlparse, parse_qs

# 导入原有的解析函数和SHT类
from .sync_crawler import SHT
from .parser import (
    extract_and_convert_video_size,
    extract_safeid,
    extract_exact_datetime,
    extract_bracket_content
)

logger = logging.getLogger(__name__)


class AsyncSHTCrawler:
    """异步SHT爬虫 - 使用httpx实现高性能并发爬取"""

    def __init__(
        self,
        max_connections: int = 20,
        timeout: float = 30.0,
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[Dict[str, str]] = None,
        proxy: Optional[str] = None,
        delay_min: float = 0.5,
        delay_max: float = 1.5
    ):
        """
        初始化异步爬虫

        Args:
            max_connections: 最大并发连接数
            timeout: 请求超时时间（秒）
            headers: 自定义请求头
            cookies: Cookie字典
            proxy: 代理URL（如 "http://proxy.example.com:8080"）
            delay_min: 最小随机延迟(秒)
            delay_max: 最大随机延迟(秒)
        """
        self.max_connections = max_connections
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(max_connections)
        self.client: Optional[AsyncSession] = None
        self.impersonate = "chrome110"  # 浏览器指纹伪装,绕过 Cloudflare
        
        self.delay_min = delay_min
        self.delay_max = delay_max

        # 默认请求头（使用iPhone UA以提高成功率）
        self.headers = headers or {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1'
        }

        # Cookie配置（关键：_safe cookie用于绕过年龄验证）
        self.cookies = cookies or {'_safe': ''}

        self.proxy = proxy

        # 创建SHT实例用于复用解析逻辑
        self._parser = SHT()

        # 统计信息
        self.stats = {
            'total_requests': 0,
            'success_count': 0,
            'failed_count': 0,
            'total_time': 0.0,
            'avg_response_time': 0.0
        }

        # v1.3.0: 错误时间窗计数器 - 避免日志刷屏和被反爬
        self._error_window = {}  # 格式: {error_type: [(timestamp, count), ...]}
        self._error_threshold = 15  # 时间窗内错误阈值
        self._time_window_seconds = 300  # 时间窗: 5分钟
        self._should_stop_crawling = False
        
        # Cookie更新锁（防止并发修改导致竞争）
        self._cookie_lock = asyncio.Lock()
        self._control_lock = asyncio.Lock()

    async def __aenter__(self):
        """异步上下文管理器入口"""
        # 使用 curl_cffi 的 AsyncSession (支持浏览器指纹伪装)
        self.client = AsyncSession()

        # 输出配置信息
        safe_cookie = self.cookies.get('_safe', '')
        safe_preview = f"{safe_cookie[:8]}..." if safe_cookie else "未设置"
        
        if self.proxy:
            logger.info(f"[ASYNC] 异步爬虫已启动 - 最大并发: {self.max_connections}, 代理: {self.proxy}, 伪装: {self.impersonate}, _safe: {safe_preview}")
        else:
            logger.info(f"[ASYNC] 异步爬虫已启动 - 最大并发: {self.max_connections}, 无代理, 伪装: {self.impersonate}, _safe: {safe_preview}")
        return self

    async def __aexit__(self, *args):
        """异步上下文管理器退出"""
        if self.client:
            try:
                # v1.4.3: [关键修复] 必须 await close()，并加入最后超时保护
                # 防止由于残留连接导致的 Session 关闭挂死
                await asyncio.wait_for(self.client.close(), timeout=5.0)
            except:
                pass

        logger.info(
            f"[ASYNC] 异步爬虫已关闭 - "
            f"总请求: {self.stats['total_requests']}, "
            f"成功: {self.stats['success_count']}, "
            f"失败: {self.stats['failed_count']}"
        )

    async def _wait_if_paused_async(self, bridge) -> bool:
        logger.info("⏸️ [ASYNC] 任务已暂停，等待恢复...")
        while True:
            await asyncio.sleep(0.5)
            action = bridge.check_control_signals()
            current_state = bridge.coordinator.get_current_state()

            if action.action == 'stop':
                self._should_stop_crawling = True
                self._parser._should_stop_crawling = True
                logger.info("⏹️ [ASYNC] 暂停期间收到停止信号")
                return True
            elif action.action == 'resume':
                logger.info("▶️ [ASYNC] 任务已恢复")
                return False
            elif not current_state.is_paused:
                if current_state.current_state == 'idle':
                    self._should_stop_crawling = True
                    self._parser._should_stop_crawling = True
                    logger.info("⏹️ [ASYNC] 检测到状态已变为idle，任务已停止")
                    return True
                logger.info("▶️ [ASYNC] 任务已恢复（状态变更）")
                return False

    async def _maybe_handle_control_signal(self) -> bool:
        if self._should_stop_crawling:
            return True
        try:
            from crawler_control.cc_control_bridge import get_crawler_control_bridge
            bridge = get_crawler_control_bridge()
            pending = bridge.queue_manager.get_pending_signals()
            has_control = any(s.type in ('stop', 'pause', 'resume') and not s.processed for s in pending)
            current_state = bridge.coordinator.get_current_state()
            if not has_control and not current_state.is_paused:
                return False

            async with self._control_lock:
                if self._should_stop_crawling:
                    return True
                action = bridge.check_control_signals()

                if action.action == 'stop':
                    self._should_stop_crawling = True
                    self._parser._should_stop_crawling = True
                    logger.info("⛔ [ASYNC] 收到停止信号，终止后续请求")
                    return True

                if action.action == 'pause' or current_state.is_paused:
                    should_stop = await self._wait_if_paused_async(bridge)
                    return should_stop
        except Exception as e:
            logger.debug(f"[ASYNC] 检查控制信号失败: {e}")
        return False

    async def fetch(self, url: str, max_retries: int = 3, **kwargs) -> Optional[Any]:
        """
        异步获取单个URL (带重试)

        Args:
            url: 目标URL
            max_retries: 最大重试次数
            **kwargs: 传递给 curl_cffi 的额外参数

        Returns:
            Response对象,失败返回None
        """
        for retry in range(max_retries):
            # 💤 随机延迟
            if await self._maybe_handle_control_signal():
                return None
            
            base_delay = random.uniform(self.delay_min, self.delay_max)
            delay = base_delay * (retry + 1) if retry > 0 else base_delay
            await asyncio.sleep(delay)

            if await self._maybe_handle_control_signal():
                return None

            # v1.4.3: [监控] 进入信号量前
            logger.debug(f"[ASYNC] URL排队中: {url[:60]}...")
            async with self.semaphore:  # 限流
                self.stats['total_requests'] += 1
                logger.debug(f"[ASYNC] 🛫 开始请求: {url[:60]}")
                start_time = time.time()

                try:
                    # 构建请求参数
                    request_params = {
                        'headers': self.headers,
                        'cookies': self.cookies,
                        'timeout': self.timeout,
                        'allow_redirects': True,
                        'impersonate': self.impersonate,
                    }
                    if self.proxy:
                        request_params['proxies'] = {'http': self.proxy, 'https': self.proxy}
                    request_params.update(kwargs)

                    # v1.4.1: 硬超时保护
                    hard_timeout = self.timeout + 15
                    
                    try:
                        response = await asyncio.wait_for(
                            self.client.get(url, **request_params),
                            timeout=hard_timeout
                        )
                    except asyncio.TimeoutError:
                        raise Exception(f"硬超时拦截 (>{hard_timeout}s)")

                    logger.debug(f"[ASYNC] 🛬 响应到达 (Status: {response.status_code}): {url[:60]}")
                    
                    if response.status_code >= 400:
                        raise Exception(f"HTTP {response.status_code}")

                    # 🔍 检查年龄验证
                    html_text = response.text
                    if "var safeid" in html_text:
                        logger.debug(f"[ASYNC] 检测到年龄验证: {url}")
                        from .parser import extract_safeid
                        safeid = extract_safeid(html_text.encode('utf-8'))
                        if safeid:
                            async with self._cookie_lock:
                                self.cookies['_safe'] = safeid
                                if hasattr(self, '_parser') and self._parser:
                                    self._parser.cookie['_safe'] = safeid
                            
                            request_params['cookies'] = self.cookies
                            # 二次请求也加入超时保护
                            response = await asyncio.wait_for(
                                self.client.get(url, **request_params),
                                timeout=hard_timeout
                            )
                            if response.status_code >= 400:
                                raise Exception(f"HTTP {response.status_code} (Verified)")

                    elapsed = time.time() - start_time
                    self.stats['success_count'] += 1
                    # ...
                    logger.debug(f"[ASYNC] ✓ {url} - {elapsed:.2f}s")
                    return response

                except Exception as e:
                    if retry < max_retries - 1:
                        logger.warning(f"[ASYNC] 重试 {retry + 1}/{max_retries}: {url} - {e}")
                        continue
                    else:
                        self.stats['failed_count'] += 1
                        logger.error(f"[ASYNC] ✗ {url} 最终失败: {e}")
                        return None

    async def fetch_batch(
        self,
        urls: List[str],
        max_retries: int = 3,
        **kwargs
    ) -> List[Optional[Any]]:
        """
        并发获取多个URL (带重试)

        Args:
            urls: URL列表
            max_retries: 每个URL的最大重试次数
            **kwargs: 传递给 curl_cffi 的额外参数

        Returns:
            Response对象列表（失败的为None）
        """
        logger.info(f"[ASYNC] 开始批量获取 {len(urls)} 个URL")

        if await self._maybe_handle_control_signal():
            return [None] * len(urls)

        tasks = [self.fetch(url, max_retries=max_retries, **kwargs) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        success_count = sum(1 for r in results if r is not None)
        logger.info(
            f"[ASYNC] 批量获取完成 - "
            f"成功: {success_count}/{len(urls)}"
        )

        return results

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return self.stats.copy()

    def _record_error_type(self, error_type: str) -> bool:
        """
        记录错误类型（时间窗计数）并检查是否应该停止爬取

        Args:
            error_type: 错误类型标识

        Returns:
            True 如果应该停止爬取，False 继续
        """
        if self._should_stop_crawling:
            return True

        # 获取当前时间
        current_time = time.time()

        # 初始化该错误类型的时间窗队列
        if error_type not in self._error_window:
            self._error_window[error_type] = deque(maxlen=100)

        # 记录错误时间
        self._error_window[error_type].append(current_time)

        # 清理时间窗外的旧错误
        window_start = current_time - self._time_window_seconds
        while self._error_window[error_type] and self._error_window[error_type][0] < window_start:
            self._error_window[error_type].popleft()

        # 获取时间窗内的错误计数
        error_count = len(self._error_window[error_type])

        # 检查是否超过阈值
        if error_count >= self._error_threshold:
            self._should_stop_crawling = True
            logger.error(f"⛔ [ASYNC] 错误类型 '{error_type}' 在{self._time_window_seconds}秒内已出现 {error_count} 次，超过阈值 {self._error_threshold}，停止爬取")
            logger.error(f"⚠️ [ASYNC] 可能遇到反爬或服务异常，避免继续请求")
            return True
        elif error_count % 5 == 0:
            logger.warning(f"⚠️ [ASYNC] 错误类型 '{error_type}' 已出现 {error_count} 次（时间窗内）")

        return False

    def _parse_detail_html(self, url: str, html: str) -> Optional[Dict[str, Any]]:
        """
        解析详情页HTML（复用原有的稳定解析逻辑）

        Args:
            url: 详情页URL
            html: HTML内容

        Returns:
            解析后的资源数据字典，失败返回None
        """
        try:
            # 复用原有的稳定解析逻辑，确保数据格式一致
            result = self._parser._parse_detail_html_stable(url, html)

            if result:
                logger.debug(f"[ASYNC] 解析成功: {url}")
            else:
                logger.debug(f"[ASYNC] 解析返回空结果: {url}")

            return result

        except Exception as e:
            logger.error(f"[ASYNC] 解析详情页失败: {url}, 错误: {e}")
            return None

    async def crawl_detail_page(self, url: str) -> Optional[Dict[str, Any]]:
        """
        异步爬取单个详情页

        Args:
            url: 详情页URL

        Returns:
            解析后的资源数据字典，失败返回None
        """
        response = await self.fetch(url)
        if not response:
            return None

        return self._parse_detail_html(url, response.text)

    async def crawl_details_batch(self, urls: List[str]) -> List[Optional[Dict[str, Any]]]:
        """
        异步批量爬取多个详情页

        Args:
            urls: 详情页URL列表

        Returns:
            解析后的资源数据列表
        """
        logger.info(f"[ASYNC] 开始批量爬取 {len(urls)} 个详情页")

        if await self._maybe_handle_control_signal():
            return [None] * len(urls)

        # 并发获取所有页面
        responses = await self.fetch_batch(urls)

        # 解析所有响应
        results = []
        for i, (url, response) in enumerate(zip(urls, responses)):
            # 检查是否应该停止解析（使用 SHT 实例的停止标志）
            if self._parser._should_stop_crawling and i > 0:
                logger.error(f"⛔ [ASYNC] 检测到停止标志，剩余 {len(urls) - i} 个响应未解析")
                # 填充剩余的None
                results.extend([None] * (len(urls) - i))
                break

            if response:
                data = self._parse_detail_html(url, response.text)
                results.append(data)
            else:
                results.append(None)

        success_count = sum(1 for r in results if r is not None)
        logger.info(
            f"[ASYNC] 详情页批量爬取完成 - "
            f"成功: {success_count}/{len(urls)}"
        )

        return results

    def _parse_tid_list(self, html: str) -> List[int]:
        """解析TID列表"""
        try:
            # 统一转换为 bytes 给 pq
            html_bytes = html if isinstance(html, bytes) else html.encode('utf-8')
            doc = pq(html_bytes)
            # 使用精确选择器 (同步版逻辑)
            items = doc("div.n5_htnrys.cl")[1:]  # 跳过第一个元素（通常是标题行）
            id_list = []
            for item in items:
                pq_item = pq(item)
                link = pq_item("div a").eq(0).attr('href')
                if link:
                    parsed_url = urlparse(link)
                    query_params = parse_qs(parsed_url.query)
                    tid = query_params.get('tid', [''])[0]
                    if tid and tid.isdigit():
                        id_list.append(int(tid))
            return id_list
        except Exception as e:
            logger.debug(f"[ASYNC] 解析TID列表失败: {e}")
            return []

    async def crawl_tid_list(self, url: str) -> List[int]:
        """异步爬取单个TID列表页"""
        response = await self.fetch(url)
        if not response:
            return []
        return self._parse_tid_list(response.text)

    async def crawl_tids_batch(self, urls: List[str]) -> List[List[int]]:
        """异步批量爬取多个TID列表页"""
        logger.info(f"[ASYNC] 开始批量获取 {len(urls)} 个TID列表页")
        
        if await self._maybe_handle_control_signal():
            return [[]] * len(urls)

        responses = await self.fetch_batch(urls)
        
        results = []
        for response in responses:
            if response:
                results.append(self._parse_tid_list(response.text))
            else:
                results.append([])
        
        total_found = sum(len(r) for r in results)
        logger.info(f"[ASYNC] 批量TID获取完成 - 共从 {len(urls)} 页中发现 {total_found} 个TID")
        return results
