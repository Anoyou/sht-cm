#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量处理器模块
将大量 URL 分批处理，使用 FastCrawler 进行高效爬取
"""

import time
import logging
import concurrent.futures
from typing import List, Dict, Any, Optional, Callable
from .fast_crawler import FastCrawler

logger = logging.getLogger(__name__)


class BatchProcessor:
    """
    批量处理器 - 分批处理大量 URL
    
    将大量 URL 分成小批次，使用 FastCrawler 进行并发爬取，
    然后对每个结果应用处理函数
    """
    
    def __init__(
        self,
        batch_size: int = 10,
        max_workers: int = 5,
        delay_min: float = 0.3,
        delay_max: float = 0.8,
        batch_timeout: int = 300  # 单批次超时时间(秒),默认5分钟
    ):
        """
        初始化批量处理器

        Args:
            batch_size: 每批处理的 URL 数量
            max_workers: FastCrawler 的最大工作线程数
            delay_min: 最小延迟时间（秒）
            delay_max: 最大延迟时间（秒）
            batch_timeout: 单批次最大超时时间（秒）
        """
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.batch_timeout = batch_timeout
        
        # 创建 FastCrawler 实例
        self.crawler = FastCrawler(
            max_workers=max_workers,
            delay_min=delay_min,
            delay_max=delay_max
        )
    
    def process_urls_in_batches(
        self,
        urls: List[str],
        process_func: Callable[[str, str], Any],
        headers: Optional[Dict] = None,
        cookies: Optional[Dict] = None,
        proxies: Optional[Dict] = None
    ) -> List[Any]:
        """
        分批处理 URL 列表
        
        Args:
            urls: URL 列表
            process_func: 处理函数，接收 (url, html) 返回处理结果
            headers: 请求头
            cookies: Cookie 字典
            proxies: 代理配置
            
        Returns:
            处理结果列表，与输入 URL 顺序一致
        """
        logger.info(
            f"[BATCH_PROCESSOR] 开始分批处理 {len(urls)} 个URL，"
            f"批次大小: {self.batch_size}"
        )
        
        all_results = []
        
        # 分批处理
        for i in range(0, len(urls), self.batch_size):
            batch_urls = urls[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            total_batches = (len(urls) + self.batch_size - 1) // self.batch_size

            logger.info(
                f"🕷️ 处理批次 {batch_num}/{total_batches}: "
                f"{len(batch_urls)} 个URL"
            )

            # 批量获取 HTML 内容（带超时控制）
            batch_start_time = time.time()
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(
                        self.crawler.fetch_urls_batch,
                        batch_urls,
                        headers,
                        cookies,
                        proxies
                    )
                    try:
                        html_contents = future.result(timeout=self.batch_timeout)
                    except concurrent.futures.TimeoutError:
                        logger.error(f"⏱️ 批次 {batch_num} 处理超时")
                        html_contents = [None] * len(batch_urls)
            except Exception as e:
                logger.error(f"⚠️ 批次 {batch_num} 处理异常: {e}")
                html_contents = [None] * len(batch_urls)
            
            # 对每个结果应用处理函数
            batch_results = []
            for url, html in zip(batch_urls, html_contents):
                if html:
                    try:
                        result = process_func(url, html)
                        batch_results.append(result)
                    except Exception as e:
                        logger.warning(
                            f"! [BATCH_PROCESSOR] 处理URL失败: {url}, 错误: {e}"
                        )
                        batch_results.append(None)
                else:
                    batch_results.append(None)
            
            all_results.extend(batch_results)
            
            # 批次间短暂延迟
            if batch_num < total_batches:
                time.sleep(0.1)
        
        # 统计成功数量
        success_count = sum(1 for r in all_results if r is not None)
        logger.info(
            f"✓ [BATCH_PROCESSOR] 分批处理完成: "
            f"成功 {success_count}/{len(urls)}"
        )
        
        return all_results
    
    def get_crawler_stats(self) -> Dict[str, Any]:
        """获取底层 FastCrawler 的统计信息"""
        return self.crawler.get_stats()
    
    def close(self) -> None:
        """关闭处理器，清理资源"""
        self.crawler.close()
