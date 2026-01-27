#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高性能多线程爬虫模块
使用连接池和线程池实现高并发爬取，支持批量处理
"""

import time
import random
import logging
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from curl_cffi.requests import Session

logger = logging.getLogger(__name__)


class FastCrawler:
    """
    高性能爬取器 - 基于 curl_cffi 的多线程爬虫
    
    特性：
    - 会话池管理，复用连接
    - 线程池并发处理
    - 自适应延迟，模拟真人操作
    - 统计信息收集
    """
    
    def __init__(
        self,
        max_workers: int = 10,
        max_connections: int = 20,
        delay_min: float = 0.3,
        delay_max: float = 0.8,
        session_get_timeout: float = 10.0  # 获取 Session 超时时间（秒）
    ):
        """
        初始化高性能爬取器

        Args:
            max_workers: 最大工作线程数
            max_connections: 最大连接数（会话池大小）
            delay_min: 最小延迟时间（秒）
            delay_max: 最大延迟时间（秒）
            session_get_timeout: 获取 Session 的超时时间（秒）
        """
        self.max_workers = max_workers
        self.max_connections = max_connections
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.session_get_timeout = session_get_timeout

        # 创建会话池
        self.session_pool: Queue = Queue(maxsize=max_connections)
        self._init_session_pool()

        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        # 统计信息
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_time': 0.0,
            'avg_response_time': 0.0
        }

    def _init_session_pool(self) -> None:
        """初始化会话池，预创建指定数量的 Session 对象"""
        for _ in range(self.max_connections):
            session = Session()
            session.timeout = 30
            # 为每个 Session 添加创建时间戳用于健康检查
            session._created_at = time.time()
            self.session_pool.put(session)

    def get_session(self) -> Optional[Session]:
        """从会话池获取一个 Session 对象（带超时）"""
        try:
            return self.session_pool.get(timeout=self.session_get_timeout)
        except Exception as e:
            logger.warning(f"! [FAST_CRAWLER] 获取 Session 超时: {e}")
            # 超时时创建一个新的 Session
            session = Session()
            session.timeout = 30
            session._created_at = time.time()
            return session

    def return_session(self, session: Session) -> None:
        """将 Session 对象归还到会话池（带健康检查）"""
        try:
            # 检查 Session 是否需要替换
            # 检查 Session 是否过期（超过1小时）
            session_age = time.time() - getattr(session, '_created_at', time.time())
            if session_age > 3600:  # 1小时 = 3600秒
                logger.debug(f"[FAST_CRAWLER] Session 已过期 ({session_age:.0f}秒)，创建新的 Session")
                # 创建新的 Session 替换旧 Session
                new_session = Session()
                new_session.timeout = 30
                new_session._created_at = time.time()
                self.session_pool.put(new_session)
            else:
                self.session_pool.put(session)
        except Exception as e:
            logger.warning(f"! [FAST_CRAWLER] 归还 Session 失败: {e}")
    
    def fetch_url(
        self,
        url: str,
        headers: Optional[Dict] = None,
        cookies: Optional[Dict] = None,
        proxies: Optional[Dict] = None
    ) -> Optional[str]:
        """
        获取单个 URL 的内容
        
        Args:
            url: 目标 URL
            headers: 请求头
            cookies: Cookie 字典
            proxies: 代理配置
            
        Returns:
            页面内容（HTML 文本），失败返回 None
        """
        # 随机延迟：模拟真人操作，防止被封
        if self.delay_max > 0:
            delay = random.uniform(self.delay_min, self.delay_max)
            time.sleep(delay)

        session = None
        start_time = time.time()
        
        try:
            session = self.get_session()
            
            # 构建请求参数
            request_kwargs = {
                'timeout': 30,
                'allow_redirects': True,
                'impersonate': 'chrome110'  # 伪装成 Chrome 浏览器
            }
            
            if headers:
                request_kwargs['headers'] = headers
            if cookies:
                request_kwargs['cookies'] = cookies
            if proxies:
                request_kwargs['proxies'] = proxies
            
            self.stats['total_requests'] += 1
            
            # 发送请求
            response = session.get(url, **request_kwargs)
            response.raise_for_status()

            # 请求层自动绕过年龄验证 (P1)
            html_text = response.text
            if "var safeid" in html_text:
                logger.debug(f"[FAST_CRAWLER] 检测到年龄验证: {url}")
                from .parser import extract_safeid
                safeid = extract_safeid(html_text.encode('utf-8'))
                if safeid:
                    session.cookies.set('_safe', safeid, domain="sehuatang.org")
                    response = session.get(url, **request_kwargs)
                    response.raise_for_status()
                    html_text = response.text

            # 请求层自动突围拦截/名言页 (P0)
            from pyquery import PyQuery as pq
            doc = pq(html_text.encode('utf-8'))
            page_title = doc('head>title').text()
            valid_keywords = ["98堂", "门户", "forum", "Discuz"]
            
            if not any(k in page_title for k in valid_keywords) and len(html_text) < 30000:
                logger.warning(f"[FAST_CRAWLER] 触发拦截('{page_title}')，启动桌面指纹突围...")
                
                # 1. 切换到桌面级 UA
                desktop_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                request_kwargs['headers'] = (request_kwargs.get('headers') or {}).copy()
                request_kwargs['headers']['User-Agent'] = desktop_ua
                
                # 2. 转换桌面版 URL
                desktop_url = url.replace('&mobile=2', '').replace('mobile=2&', '').replace('mobile=2', '')
                
                # 3. 访问首页洗白
                try:
                    session.get("https://sehuatang.org/forum.php", **request_kwargs)
                    time.sleep(random.uniform(1.5, 3.0))
                    
                    # 4. 最终冲锋
                    response = session.get(desktop_url, **request_kwargs)
                    response.raise_for_status()
                    html_text = response.text
                    logger.info(f"✅ [FAST_CRAWLER] 桌面突围完成: {url}")
                except Exception as e:
                    logger.error(f"❌ [FAST_CRAWLER] 突围失败: {e}")

            self.stats['successful_requests'] += 1
            return html_text
            
        except Exception as e:
            self.stats['failed_requests'] += 1
            logger.warning(f"! [FAST_CRAWLER] 获取URL失败: {url}, 错误: {e}")
            return None
        finally:
            if session:
                self.return_session(session)
    
    def fetch_urls_batch(
        self,
        urls: List[str],
        headers: Optional[Dict] = None,
        cookies: Optional[Dict] = None,
        proxies: Optional[Dict] = None
    ) -> List[Optional[str]]:
        """
        批量获取 URL 内容 - 保持顺序
        
        Args:
            urls: URL 列表
            headers: 请求头
            cookies: Cookie 字典
            proxies: 代理配置
            
        Returns:
            页面内容列表，与输入 URL 顺序一致，失败的位置为 None
        """
        logger.info(f"[FAST_CRAWLER] 开始批量获取 {len(urls)} 个URL")
        
        # 提交所有任务到线程池
        futures_with_index = []
        for i, url in enumerate(urls):
            future = self.executor.submit(
                self.fetch_url, url, headers, cookies, proxies
            )
            futures_with_index.append((i, future))
        
        # 收集结果，保持顺序
        results: List[Optional[str]] = [None] * len(urls)
        completed = 0
        
        for index, future in futures_with_index:
            try:
                result = future.result(timeout=30)
                results[index] = result
                completed += 1
                
                # 定期输出进度
                if completed % 10 == 0 or completed == len(urls):
                    logger.info(f"[FAST_CRAWLER] 批量获取进度: {completed}/{len(urls)}")
            except Exception as e:
                logger.warning(f"! [FAST_CRAWLER] 批量获取任务失败 (索引{index}): {e}")
                results[index] = None
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息的副本"""
        return self.stats.copy()
    
    def close(self) -> None:
        """关闭爬取器，清理资源"""
        self.executor.shutdown(wait=True)
        
        # 关闭所有会话
        while not self.session_pool.empty():
            session = self.session_pool.get()
            session.close()
