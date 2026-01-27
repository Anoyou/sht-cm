#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SHT 爬虫基础类
提供网络请求、会话管理、防屏蔽等基础功能
"""

import os
import time
import random
import logging
import threading
from typing import Optional, Dict, Callable, Any
from pyquery import PyQuery as pq
from curl_cffi import requests

from configuration import Config
from .parser import extract_safeid

logger = logging.getLogger(__name__)


class SHTBase:
    """
    SHT 爬虫基础类
    
    提供核心功能：
    - 网络请求管理
    - Session 会话保持
    - CloudFlare 绕过
    - 年龄验证绕过
    - 自适应延迟和重试机制
    """
    
    # 类变量
    proxy: Optional[str] = None
    proxies: Dict = {}
    headers: Dict = {}
    cookie: Dict = {}
    bypass: Optional[str] = None
    flare_solver: Optional[str] = None

    def __init__(self):
        """初始化爬虫基础配置"""
        # 使用 iPhone User-Agent，成功率更高
        ua = (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/18.5 Mobile/15E148 Safari/604.1"
        )
        self.headers = {'User-Agent': ua}
        
        # 简化 cookie 管理，只维护必要的状态
        self.cookie = {'_safe': ''}

        # 板块信息缓存（避免重复网络请求）
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
            logger.warning("! 未配置代理，可能遇到访问问题")

        # 绕过服务配置
        self.bypass = os.environ.get("BYPASS_URL") or getattr(Config, 'BYPASS_URL', None)
        self.flare_solver = (
            os.environ.get("FLARE_SOLVERR_URL") or 
            getattr(Config, 'FLARE_SOLVERR_URL', None)
        )

        # Session 管理 - 保持连接和 cookie 持久化
        self._session = None
        self._session_created_at = None  # Session创建时间
        self._session_request_count = 0  # Session请求计数

        # 反爬检测和自适应延迟机制
        self._consecutive_failures = 0  # 连续失败计数
        self._slow_mode = False  # 慢速模式标志
        self._failure_threshold = 3  # 连续失败阈值（3次后降级）
        self._slow_mode_delay = (1.0, 3.0)  # 慢速模式延迟范围（秒）
        self._normal_mode_delay = (0.3, 0.8)  # 正常模式延迟范围（秒）

        # 错误类型计数器 - 避免日志刷屏和被反爬
        self._error_type_counter: Dict[str, int] = {}  # 记录各类错误的出现次数
        self._error_threshold = 15  # 相同错误类型的阈值
        self._should_stop_crawling = False  # 停止爬取标志
        self._lock = threading.RLock()  # 使用递归锁，支持嵌套调用

    def _get_session(self):
        """获取或创建 Session 实例 - 保持连接和 cookie 持久化"""
        with self._lock:  # 使用锁保护 Session 的创建和获取
            # 检查是否需要刷新Session
            if self._session is not None:
                self._maybe_refresh_session()

            if self._session is None:
                from curl_cffi.requests import Session as CurlSession
                from configuration import config_manager

                self._session = CurlSession()
                # 使用配置文件的超时值
                timeout = config_manager.get('REQUEST_TIMEOUT', 30)
                self._session.timeout = timeout
                self._session_created_at = time.time()
                self._session_request_count = 0
                logger.debug(f"创建新的 Session 实例，超时设置: {timeout}秒")
            return self._session

    def _maybe_refresh_session(self):
        """
        检查是否需要刷新Session
        - 每1000个请求刷新一次
        - 或者每2小时刷新一次
        """
        if self._session is None:
            return

        should_refresh = False

        # 检查请求数
        if self._session_request_count >= 1000:
            logger.info(f"📊 Session请求数达到{self._session_request_count}，刷新Session")
            should_refresh = True

        # 检查存活时间
        if self._session_created_at and (time.time() - self._session_created_at) > 7200:  # 2小时
            elapsed_hours = (time.time() - self._session_created_at) / 3600
            logger.info(f"⏰ Session存在超过{elapsed_hours:.1f}小时，刷新Session")
            should_refresh = True

        if should_refresh:
            self._close_session()
            logger.info("♻️ Session已刷新")

    def _close_session(self) -> None:
        """关闭 Session 实例"""
        if self._session:
            try:
                self._session.close()
                logger.debug("Session 已关闭")
            except Exception:
                pass
            self._session = None

    @property
    def session(self):
        """Session 属性 - 获取或创建 Session 实例"""
        return self._get_session()

    def _record_failure(self) -> None:
        """记录失败，检查是否需要降级到慢速模式"""
        self._consecutive_failures += 1
        if not self._slow_mode and self._consecutive_failures >= self._failure_threshold:
            self._slow_mode = True
            logger.warning(
                f"! 检测到连续 {self._consecutive_failures} 次失败，"
                f"降级到慢速模式（延迟 {self._slow_mode_delay[0]}-{self._slow_mode_delay[1]} 秒）"
            )

    def _record_error_type(self, error_type: str) -> bool:
        """
        记录错误类型并检查是否应该停止爬取

        Args:
            error_type: 错误类型标识（如 "parse_error", "encoding_error" 等）

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
            logger.error(
                f"⛔ [CRAWLER] 错误类型 '{error_type}' 已出现 {count} 次，"
                f"超过阈值 {self._error_threshold}，停止爬取"
            )
            logger.error("⚠️ [CRAWLER] 可能遇到反爬或服务异常，避免继续请求")

            # 发送停止通知
            try:
                from scheduler.notifier import _send_telegram_message, render_message_template
                stop_msg, parse_mode = render_message_template('crawler_error_stop', {
                    'error_type': error_type,
                    'count': count,
                    'details': f'连续相同错误达到阈值({self._error_threshold}次)',
                    'suggestion': '检查网络连接或目标网站状态'
                })
                if not stop_msg:
                    stop_msg = f"""🛑 *爬虫因错误过多已停止*
━━━━━━━━━━━━━━
❌ 错误类型：{error_type}
🔢 错误次数：{count}
📝 详情：连续相同错误达到阈值({self._error_threshold}次)
💡 建议：检查网络连接或目标网站状态"""
                    parse_mode = 'Markdown'
                _send_telegram_message(stop_msg, parse_mode=parse_mode)
            except Exception as e:
                logger.debug(f"发送错误停止通知失败: {e}")

            return True
        elif count % 5 == 0:  # 每5次打印一次警告
            logger.warning(f"⚠️ [CRAWLER] 错误类型 '{error_type}' 已出现 {count} 次")

        return False

    def _record_success(self) -> None:
        """记录成功，重置失败计数"""
        if self._consecutive_failures > 0:
            logger.debug(
                f"✓ 请求成功，重置失败计数（之前连续失败 {self._consecutive_failures} 次）"
            )
            self._consecutive_failures = 0
        
        # 如果在慢速模式，成功后立即恢复正常模式
        if self._slow_mode:
            self._slow_mode = False
            logger.info("✓ 请求成功，恢复正常速度模式")

    def _adaptive_delay(self) -> None:
        """自适应延迟 - 根据模式选择延迟时间"""
        if self._slow_mode:
            delay = random.uniform(*self._slow_mode_delay)
            logger.debug(f"慢速模式延迟 {delay:.2f} 秒...")
        else:
            delay = random.uniform(*self._normal_mode_delay)
            logger.debug(f"正常延迟 {delay:.2f} 秒...")
        time.sleep(delay)

    def get_original(self, url: str) -> Optional[bytes]:
        """
        获取原始页面内容
        
        自动处理：
        - CloudFlare 验证
        - 年龄验证（18+）
        - Session 保持
        
        Args:
            url: 目标 URL
            
        Returns:
            页面 HTML 内容（bytes），失败返回 None
        """
        try:
            # 确保对 Session 的并发访问安全
            with self._lock:
                # 使用 Session 保持连接和 cookie 持久化
                session = self._get_session()

                # 增加请求计数
                self._session_request_count += 1

                # 使用配置的超时值
                from configuration import config_manager
                timeout = config_manager.get('REQUEST_TIMEOUT', 30)

                # 发送请求
                res = session.get(
                    url,
                    proxies=self.proxies,
                    cookies=self.cookie,
                    headers=self.headers,
                    allow_redirects=True,
                    timeout=timeout,
                    impersonate="chrome110"
                )

            html = res.text.encode('utf-8')
            doc = pq(html)
            page_title = doc('head>title').text()

            # 检查 CloudFlare 验证
            if 'Just a moment' in page_title:
                logger.info("[API] 检测到 CloudFlare，尝试绕过")
                html = self.bypass_cf(url)
                if not html:
                    logger.error("✗ [API] CloudFlare 绕过失败")
                    self._record_failure()
                    return None

            # 检查年龄验证（18+）
            doc = pq(html)
            if "var safeid" in doc.text():
                logger.info("[API] 检测到年龄验证，尝试获取 safeid")
                html = self.bypass_r18(html, url)
                if not html:
                    logger.error("✗ [API] 年龄验证绕过失败")
                    self._record_failure()
                    return None

            # 定义合法标题库
            valid_keywords = ["98堂", "门户", "forum", "Discuz"]
            
            # 验证页面有效性
            doc = pq(html)
            page_title = doc('head>title').text()
            
            # 核心突围逻辑 (P0)
            if any(k in page_title for k in valid_keywords):
                logger.debug("页面获取成功")
                self._record_success()
                return html
            else:
                # 判定为拦截页（如名言页）
                if "forum.php" in url and "mod=" not in url:
                    return None

                logger.warning(f"⚠️ [ANTIBOT] 检测到拦截('{page_title}')，启动桌面级伪装突围...")
                
                # 1. 暂时保存并窃取桌面版指纹
                original_ua = self.headers.get('User-Agent')
                desktop_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                self.headers['User-Agent'] = desktop_ua
                
                try:
                    # 2. 转换 URL 为桌面版（核心关键）
                    desktop_url = url.replace('&mobile=2', '').replace('mobile=2&', '').replace('mobile=2', '')
                    
                    # 3. 访问桌面首页洗白 Session
                    self.session.get(
                        "https://sehuatang.org/forum.php",
                        proxies=self.proxies,
                        cookies=self.cookie,
                        headers=self.headers,
                        timeout=15,
                        impersonate="chrome110"
                    )
                    
                    # 4. 短暂休眠，模拟用户从首页点击进入
                    time.sleep(random.uniform(2.0, 3.5))
                    
                    # 5. 使用桌面指纹重新冲锋转后的 URL
                    res = self.session.get(
                        desktop_url,
                        proxies=self.proxies,
                        cookies=self.cookie,
                        headers=self.headers,
                        allow_redirects=True,
                        timeout=timeout,
                        impersonate="chrome110"
                    )
                    new_html = res.text.encode('utf-8')
                    new_title = pq(new_html)('head>title').text()
                    
                    if any(k in new_title for k in valid_keywords) or len(new_html) > 50000:
                        logger.info("✅ [ANTIBOT] 桌面指纹突围成功！")
                        self._record_success()
                        return new_html
                    else:
                        logger.warning(f"❌ [ANTIBOT] 桌面突围亦告失败: {new_title}")
                finally:
                    # 既然已经重试完成，必须恢复原始 UA 保证后续一致性
                    self.headers['User-Agent'] = original_ua

                self._record_failure()
                return None

        except Exception as e:
            logger.error(f"✗ [API] 获取页面失败: {url}, 错误: {e}")
            self._record_failure()
            return None

    def bypass_cf(self, url: str) -> Optional[bytes]:
        """
        绕过 CloudFlare 验证
        
        需要配置 FLARE_SOLVERR_URL 环境变量
        
        Args:
            url: 目标 URL
            
        Returns:
            页面 HTML 内容（bytes），失败返回 None
        """
        if not self.flare_solver:
            logger.warning("! [CONFIG] 未配置 FLARE_SOLVERR_URL，无法绕过 Cloudflare")
            return None
            
        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": 60000,
            "proxy": (
                {"url": self.proxies['http']} 
                if self.proxies and 'http' in self.proxies 
                else None
            ),
            "cookies": [{"name": k, "value": v} for k, v in self.cookie.items()]
        }
        
        try:
            res = requests.post(
                self.flare_solver,
                headers={"Content-Type": "application/json"},
                json=payload
            )
            result = res.json()
            
            if result['solution']['status'] != 200:
                return None
                
            html = result['solution']['response'].encode('utf-8')
            doc = pq(html)
            
            # 检查是否还需要年龄验证
            if "var safeid" in doc.text():
                safeid = extract_safeid(html)
                self.cookie['_safe'] = safeid
                return self.bypass_cf(url)  # 递归尝试
                
            return html
            
        except Exception as e:
            logger.error(f"✗ [API] 绕过 Cloudflare 失败: {e}")
            return None

    def bypass_r18(self, html: bytes, url: str) -> Optional[bytes]:
        """绕过年龄验证（18+）- 增加对多种标题的支持"""
        safeid = extract_safeid(html)
        if safeid:
            self.cookie['_safe'] = safeid
            logger.debug(f"获取到 safeid: {safeid[:8]}...")

            res = self.session.get(
                url,
                proxies=self.proxies,
                cookies=self.cookie,
                headers=self.headers,
                allow_redirects=True,
                timeout=15,
                impersonate="chrome110"
            )
            
            resp_html = res.text.encode('utf-8')
            page_title = pq(resp_html)('head>title').text()

            # 此处判定需与 get_original 保持一致
            if any(k in page_title for k in ["98堂", "门户", "forum", "Discuz"]):
                logger.debug("年龄验证绕过成功")
                return resp_html
            else:
                logger.warning(f"年龄验证后标题异常: {page_title}")
        return None

    def _retry_with_delay(
        self,
        func: Callable,
        func_name: str,
        max_attempts: int = 3,
        delay_seconds: int = 5
    ) -> Any:
        """
        带重试机制的函数调用包装器

        Args:
            func: 要执行的函数
            func_name: 函数名称（用于日志）
            max_attempts: 最大尝试次数（默认3次）
            delay_seconds: 失败后的延迟时间（默认5秒）

        Returns:
            函数执行结果，如果所有尝试都失败则返回 None 或 {}
        """
        result = None
        
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"[{func_name}] 第 {attempt}/{max_attempts} 次尝试")
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
                logger.info(
                    f"⏳ [{func_name}] {delay_seconds} 秒后进行第 {attempt + 1} 次尝试..."
                )
                time.sleep(delay_seconds)

        logger.error(f"✗ [{func_name}] 所有 {max_attempts} 次尝试均失败")
        return {} if isinstance(result, dict) else None
