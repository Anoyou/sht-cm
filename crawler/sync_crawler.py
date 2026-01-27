#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SHT 同步爬虫 - 主爬虫类

提供完整的爬虫功能：
- 板块信息获取
- TID 列表爬取
- 详情页爬取
- 数据保存

继承自 SHTBase，复用网络请求和防屏蔽机制
"""

import os
import re
import time
import random
import logging
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urljoin
from pyquery import PyQuery as pq
from datetime import datetime, timedelta
import bencodepy
import hashlib
import binascii

from .base import SHTBase
from .parser import (
    extract_and_convert_video_size,
    extract_safeid,
    extract_exact_datetime,
    extract_bracket_content
)
from .batch_processor import BatchProcessor

# 获取日志记录器
logger = logging.getLogger(__name__)


class SHT(SHTBase):
    """SHT 主爬虫类 - 继承自 SHTBase
    
    提供完整的爬虫功能：
    - 板块信息获取（从手机版列表页或桌面版首页）
    - TID 列表爬取
    - 详情页爬取（单个或批量）
    - 数据保存到数据库
    """
    
    def __init__(self):
        """初始化 SHT 爬虫"""
        super().__init__()
        
        # 板块信息缓存
        self._forums_cache = None
        self._forums_cache_time = 0
        self._cache_duration = 300  # 5分钟缓存
        self._cache_expiry = 0  # 缓存过期时间戳
        
        # 错误类型计数器 - 避免日志刷屏和被反爬
        self._error_type_counter = {}
        self._error_threshold = 15  # 相同错误类型的阈值
        self._should_stop_crawling = False  # 停止爬取标志
        
        logger.debug("SHT 爬虫初始化完成")
    
    # ==================== 板块信息获取 ====================
    
    def get_all_forums_info(self) -> Dict[str, Dict]:
        """获取所有板块信息 - 双重策略：优先手机版列表页，失败则尝试桌面版首页
        
        Returns:
            Dict[str, Dict]: 板块信息字典，格式：
                {
                    'fid': {
                        'fid': str,
                        'name': str,
                        'total_topics': int or None,
                        'total_pages': int or None
                    }
                }
        """
        # 检查实例缓存是否有效
        current_time = time.time()
        if self._forums_cache and (current_time - self._forums_cache_time) < self._cache_duration:
            logger.info(f"✓ 使用实例缓存的板块信息（{int(self._cache_duration - (current_time - self._forums_cache_time))}秒后过期）")
            return self._forums_cache
        
        logger.info("[CRAWLER] 开始获取所有板块信息（准确数据，双重策略，带重试机制）")
        
        # 策略A：优先尝试手机版板块列表页（带重试机制：最多3次，间隔5秒）
        forums_info = self._retry_with_delay(
            func=self._get_forums_from_mobile_list,
            func_name="策略A-手机版列表页",
            max_attempts=3,
            delay_seconds=5
        )
        
        # 策略B：如果手机版失败，尝试桌面版首页作为备份（带重试机制）
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
    
    def _get_forums_from_mobile_list(self) -> Dict[str, Dict]:
        """策略A：从手机版板块列表页提取准确数据
        
        Returns:
            Dict[str, Dict]: 板块信息字典
        """
        try:
            # 只获取预设的11个板块
            from constants import VALID_FIDS
            
            logger.info(f"[CRAWLER] [策略A] 尝试从手机版板块列表页获取（仅限{len(VALID_FIDS)}个预设板块）")
            
            url = "https://sehuatang.org/forum.php?forumlist=1&mobile=2"
            html = self.get_original(url)
            
            if not html:
                logger.warning(f"! [策略A] 无法获取板块列表页")
                return {}
            
            # 统一传递 bytes 给 pq
            html_bytes = html if isinstance(html, bytes) else html.encode('utf-8')
            doc = pq(html_bytes)
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
    
    def _get_forums_from_desktop_home(self) -> Dict[str, Dict]:
        """策略B：从桌面版首页提取准确数据（备用方案）
        
        Returns:
            Dict[str, Dict]: 板块信息字典
        """
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
            
            # 统一传递 bytes 给 pq，让 lxml 自行处理编码声明
            html_bytes = html if isinstance(html, bytes) else html.encode('utf-8')
            doc = pq(html_bytes)
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
    
    def _get_default_forums(self) -> Dict[str, Dict]:
        """获取预定义的板块列表
        
        Returns:
            Dict[str, Dict]: 板块信息字典
        """
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
    
    def get_forum_info(self, fid: str, all_forums_cache: Optional[Dict] = None) -> Optional[Dict]:
        """获取单个板块信息：总页数、主题数量等
        
        Args:
            fid: 板块ID
            all_forums_cache: 可选的板块信息缓存，避免重复获取
        
        Returns:
            Dict: 板块信息，包含 fid, name, total_topics, total_pages
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
                    'total_topics': all_forums_cache[fid].get('total_topics'),
                    'total_pages': all_forums_cache[fid].get('total_pages')
                }
                logger.debug(f"✅ 从首页数据获取到准确信息: total_topics={forum_info['total_topics']}")
            else:
                # 尝试从持久化缓存获取
                try:
                    from models import Category
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
                except ImportError:
                    # 如果models模块不可用，使用默认值
                    forum_info = {
                        'fid': fid,
                        'name': f'板块{fid}',
                        'total_topics': None,
                        'total_pages': None
                    }
            
            # 只在需要时获取页数信息（首页没有页数，需要单独获取）
            if forum_info['total_pages'] is None:
                url = f"https://sehuatang.org/forum.php?mod=forumdisplay&fid={fid}&mobile=2"
                logger.debug(f" 获取页数信息: {url}")
                
                html = self.get_original(url)
                if html:
                    # 统一传递 bytes 给 pq，让 lxml 自行处理编码声明
                    html_bytes = html if isinstance(html, bytes) else html.encode('utf-8')
                    doc = pq(html_bytes)
                    pages_found = False
                    
                    # 方法1: 使用具体选择器获取总页数
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
            topics_display = "未知" if forum_info['total_topics'] is None else str(forum_info['total_topics'])
            pages_display = "未知" if forum_info['total_pages'] is None else str(forum_info['total_pages'])
            
            logger.info(f"📋 板块信息 [{forum_info['name']}]: 总计{topics_display}主题, 共{pages_display}页")
            
            return forum_info
        
        except Exception as e:
            logger.error(f"✗ 获取板块信息失败: fid={fid}, 错误: {e}")
            logger.debug(f" 详细错误信息", exc_info=True)
            return None
    
    # ==================== 爬取功能 ====================
    
    def crawler_tid_list(self, url: str) -> List[int]:
        """爬取页面中的tid列表
        
        Args:
            url: 板块页面URL
        
        Returns:
            List[int]: TID列表
        """
        # 添加重试机制，提高成功率
        for retry in range(3):
            try:
                html = self.get_original(url)
                if html:
                    # 统一传递 bytes 给 pq，让 lxml 自行处理编码声明
                    html_bytes = html if isinstance(html, bytes) else html.encode('utf-8')
                    doc = pq(html_bytes)
                    # 使用精确选择器
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
                time.sleep(1)
        
        logger.error(f"连续3次爬取失败: {url}")
        return []
    
    def _fix_mobile_session_and_retry(self, original_url: str) -> Optional[str]:
        """修复会话问题 - 改用桌面版访问策略
        
        Args:
            original_url: 原始URL
        
        Returns:
            str or None: 修复后的HTML内容
        """
        logger.info("🔧 开始修复会话问题...")
        
        try:
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
        """使用正则表达式提取tid - 备用方法
        
        Args:
            html: HTML内容
            url: 页面URL（用于日志）
        
        Returns:
            List[int]: TID列表
        """
        logger.debug("🔧 使用正则表达式备用方法提取tid")
        
        try:
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
    
    def crawler_detail(self, url: str) -> Dict:
        """爬取单个详情页
        
        Args:
            url: 详情页URL
        
        Returns:
            Dict: 详情信息，包含 title, sub_type, publish_date, magnet, preview_images, size
        """
        try:
            html = self.get_original(url)
            if html:
                # 统一传递 bytes 给 pq，让 lxml 自行处理编码声明
                html_bytes = html if isinstance(html, bytes) else html.encode('utf-8')
                doc = pq(html_bytes)
                
                # 使用精确磁力链接提取方式
                all_text = doc('div.blockcode').text()
                magnet_pattern = r'magnet:\?xt=urn:btih:[0-9a-fA-F]+'
                match = re.search(magnet_pattern, all_text)
                
                magnet = None
                torrent_size = None # 提前初始化，为了后面的 size 补全逻辑
                
                if match:
                    magnet = match.group()
                    logger.debug(f"找到磁力链接: {url}")
                
                # 备用torrent处理（用于获取磁力链接或大小）
                if not magnet:
                    torrent = doc("a:contains('.torrent')").eq(0)
                    if torrent:
                        torrent_url = torrent.attr('href')
                        logger.debug(f"尝试解析torrent文件: {url}")
                        res = self.parse_torrent_get_magnet(url, f"https://sehuatang.org/{torrent_url}")
                        if res:
                            magnet, torrent_size = res
                
                if magnet:
                    # 提取其他信息
                    date = extract_exact_datetime(html)
                    size = extract_and_convert_video_size(html)
                    
                    # 如果 HTML 没有大小信息，且还没从种子提取过，尝试下载种子获取
                    if (size is None or size == 0) and torrent_size is None:
                        torrent = doc("a:contains('.torrent')").eq(0)
                        if torrent:
                            torrent_url = torrent.attr('href')
                            logger.info(f"💡 HTML无大小信息，尝试从种子文件提取: {url}")
                            res = self.parse_torrent_get_magnet(url, f"https://sehuatang.org/{torrent_url}")
                            if res:
                                _, torrent_size = res
                    
                    # 优先利用物理种子的大小补全
                    if (size is None or size == 0) and torrent_size:
                        size = torrent_size
                        logger.info(f"💡 成功从种子中补全大小: {size}MB")
                    
                    # 兜底利用附件区明文信息补全
                    if (size is None or size == 0):
                        attachment_text = doc('.attnm, .pattl, .attachlib').text()
                        if attachment_text:
                            alt_size = extract_and_convert_video_size(attachment_text)
                            if alt_size:
                                size = alt_size
                                logger.info(f"💡 [详情页] 成功从附件详情区探测到大小: {size}MB")

                    sub_type = extract_bracket_content(html)
                    
                    # 标题处理
                    title = doc('h2.n5_bbsnrbt').text()
                    pattern = r"^\[.*?\]"
                    title = re.sub(pattern, "", title).strip()
                    
                    # 预览图片提取 (多版本兼容)
                    img_elements = doc('div.message img, td.t_f img, .pcb img, .ignoreattcheck img, .pattl img')
                    img_src_list = []
                    filter_keywords = ['static/image/smiley', 'static/image/common', 'none.gif', 'zoom.png']
                    
                    for img in img_elements.items():
                        src = img.attr('src') or img.attr('file') or img.attr('zoomfile')
                        if src:
                            src = src.strip()
                            if any(k in src for k in filter_keywords): continue
                            if src not in img_src_list:
                                img_src_list.append(src)
                    
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
    
    def crawler_details_batch(self, urls: List[str], use_batch_mode: bool = False) -> List[Dict]:
        """批量爬取详情页 - 可选择批量或单个处理模式
        
        Args:
            urls: 详情页URL列表
            use_batch_mode: 是否使用批量模式（默认False，使用单个处理模式更稳定）
        
        Returns:
            List[Dict]: 详情信息列表
        """
        logger.info(f" 开始{'批量' if use_batch_mode else '单个'}爬取 {len(urls)} 个详情页")
        
        # 每次批量爬取开始时重置状态
        self._consecutive_failures = 0
        self._slow_mode = False
        self._error_type_counter = {}
        self._should_stop_crawling = False
        logger.info(f"[CRAWLER] 初始化爬取状态：正常模式，延迟{self._normal_mode_delay[0]}-{self._normal_mode_delay[1]}秒")
        
        try:
            if use_batch_mode:
                # 批量处理模式
                try:
                    # 获取配置的线程数和延迟
                    from configuration import config_manager
                    max_workers = config_manager.get('CRAWLER_THREAD_COUNT', 10)
                    delay_min = config_manager.get('CRAWLER_SYNC_DELAY_MIN', 0.3)
                    delay_max = config_manager.get('CRAWLER_SYNC_DELAY_MAX', 0.8)
                    
                    # 创建新的处理器实例
                    local_batch_processor = BatchProcessor(
                        batch_size=max_workers,
                        max_workers=max_workers,
                        delay_min=delay_min,
                        delay_max=delay_max
                    )
                    
                    def process_detail_html(url: str, html: str) -> Dict:
                        """处理详情页HTML"""
                        return self._parse_detail_html_stable(url, html)
                    
                    # 批量处理
                    results = local_batch_processor.process_urls_in_batches(
                        urls,
                        process_detail_html,
                        headers=self.headers,
                        cookies=self.cookie,
                        proxies=self.proxies
                    )
                    
                    # 关闭处理器
                    local_batch_processor.close()
                    
                    # 确保结果数量与输入URL数量一致
                    if len(results) != len(urls):
                        logger.warning(f"结果数量不匹配: 输入{len(urls)}个URL，返回{len(results)}个结果")
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
                    
                    return results
                
                except Exception as e:
                    logger.error(f"批量爬取失败: {e}")
                    logger.info("降级到单个处理模式")
                    use_batch_mode = False
            
            if not use_batch_mode:
                # 单个处理模式 - 更稳定可靠
                logger.info("使用单个处理模式（更稳定）")
                results = []
                success_count = 0
                
                for i, url in enumerate(urls):
                    # 检查控制信号（停止和暂停）
                    try:
                        from scheduler.utils import check_stop_and_pause
                        if check_stop_and_pause():
                            logger.info(f"⛔ [CRAWLER] 检测到停止信号，剩余 {len(urls) - i} 个URL未爬取")
                            results.extend([None] * (len(urls) - i))
                            break
                    except Exception as e:
                        logger.debug(f"检查控制信号失败: {e}")
                    
                    # 检查是否应该停止爬取（旧标志，保留兼容性）
                    if self._should_stop_crawling:
                        logger.error(f"⛔ [CRAWLER] 检测到停止标志，剩余 {len(urls) - i} 个URL未爬取")
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
                            mode_info = "慢速模式" if self._slow_mode else "正常模式"
                            logger.info(f"[CRAWLER] 当前爬取模式: {mode_info}，连续失败次数: {self._consecutive_failures}")
                        
                        # 使用自适应延迟
                        if i < len(urls) - 1:
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
        """解析详情页HTML内容 - 稳定解析逻辑
        
        Args:
            url: 页面URL
            html: HTML内容（可以是bytes或str）
        
        Returns:
            Dict: 解析结果
        """
        try:
            # 统一传递 bytes 给 pq，让 lxml 自行处理编码声明
            html_bytes = html if isinstance(html, bytes) else html.encode('utf-8')
            doc = pq(html_bytes)
            # 基本检查
            if not html or len(html) < 100:
                logger.warning(f"页面内容过短或为空: {url}, 长度: {len(html) if html else 0}")
                return {}
            
            # 检查常见错误页面
            if "抱歉，指定的主题不存在或已被删除或正在被审核" in html:
                logger.warning(f"主题不存在或已删除: {url}")
                return {}
            
            if "您无权进行当前操作" in html:
                logger.warning(f"无权访问页面: {url}")
            
            # 检查页面标题确认有效性
            page_title = doc('head>title').text()
            valid_keywords = ["98堂", "门户", "forum", "Discuz"]
            if not any(k in page_title for k in valid_keywords):
                logger.warning(f"⚠️ [ANTIBOT] 页面被拦截(标题: {page_title})")
                return {"error_type": "antibot_detected", "error_msg": f"拦截页: {page_title}"}
            
            # 提取标题 (适配全版本)
            title = doc('h2.n5_bbsnrbt').text() or doc('#thread_subject').text() or doc('h1.ts').text()
            if not title:
                title_elem = doc('title').text()
                if " - 98堂" in title_elem:
                    title = title_elem.split(" - 98堂")[0].strip()

            if not title:
                logger.warning(f"⚠️ [ANTIBOT] 未找到帖子标题: {url}")
                return {"error_type": "antibot_detected", "error_msg": "未找到标题"}
            
            # 清理标题
            title = re.sub(r"^\[.*?\]", "", title).strip()

            # --- 2. 磁力链接/种子挖掘 (全版本适配) ---
            magnet = None
            torrent_size = None # 提前初始化，保证变量安全
            
            # 方案A: 搜索精确代码块
            magnet_pattern = r'magnet:\?xt=urn:btih:[0-9a-fA-F]+'
            all_potential_text = doc('div.blockcode, div.message, td.t_f, .pcb').text()
            match = re.search(magnet_pattern, all_potential_text)
            
            if match:
                magnet = match.group()
                logger.debug(f"找到磁力链接: {url}")
            else:
                # 方案B: 全盘深控附件种子 (P1优先)
                logger.debug(f"正文未见磁链，启动全盘附件扫描...")
                torrent_link = (
                    doc("a:contains('.torrent')").eq(0) or 
                    doc("a[href*='attachment.php'][href*='aid=']").filter(lambda i, e: '.torrent' in pq(e).text().lower()).eq(0) or
                    doc("div.attnm a, div.pattl a, .ignoreattcheck a").filter(lambda i, e: 
                        '.torrent' in pq(e).text().lower() or 'torrent' in (pq(e).attr('href') or '').lower()
                    ).eq(0) or
                    doc("a[href*='.torrent']").eq(0)
                )

                if torrent_link:
                    torrent_url = torrent_link.attr('href')
                    if torrent_url:
                        full_torrent_url = urljoin(url, torrent_url)
                        logger.info(f"🔎 挖掘到种子链接: {full_torrent_url}，正在自动转化...")
                        res = self.parse_torrent_get_magnet(url, full_torrent_url)
                        if res:
                            magnet, torrent_size = res
                
                if not magnet:
                    logger.warning(f"未找到可用数据: {url}")
            
            if magnet:
                # 提取其他信息
                date = extract_exact_datetime(html)
                size = extract_and_convert_video_size(html)
                
                # 如果 HTML 没有大小信息，且还没从种子提取过，主动下载种子获取
                if (size is None or size == 0) and torrent_size is None:
                    # 尝试查找种子附件
                    torrent_link = (
                        doc("a:contains('.torrent')").eq(0) or 
                        doc("a[href*='attachment.php'][href*='aid=']").filter(lambda i, e: '.torrent' in pq(e).text().lower()).eq(0) or
                        doc("div.attnm a, div.pattl a, .ignoreattcheck a").filter(lambda i, e: 
                            '.torrent' in pq(e).text().lower() or 'torrent' in (pq(e).attr('href') or '').lower()
                        ).eq(0)
                    )
                    if torrent_link:
                        torrent_url = torrent_link.attr('href')
                        if torrent_url:
                            full_torrent_url = urljoin(url, torrent_url)
                            logger.info(f"💡 [全域解析] HTML无大小信息，尝试从种子文件提取: {full_torrent_url}")
                            res = self.parse_torrent_get_magnet(url, full_torrent_url)
                            if res:
                                _, torrent_size = res
                
                # 补全逻辑1：物理种子提取 (高可信度)
                if (size is None or size == 0) and torrent_size:
                    size = torrent_size
                    logger.info(f"💡 [全域解析] 成功从种子中补全大小: {size}MB")
                
                # 补全逻辑2：附件详情区明文 (辅助可信度)
                if (size is None or size == 0):
                    attachment_text = doc('.attnm, .pattl, .attachlib').text()
                    if attachment_text:
                        alt_size = extract_and_convert_video_size(attachment_text)
                        if alt_size:
                            size = alt_size
                            logger.info(f"💡 [全域解析] 成功从附件详情区探测到大小: {size}MB")

                sub_type = extract_bracket_content(html)
                
                # 预览图片提取 (全域探测)
                img_elements = doc('div.message img, td.t_f img, .pcb img, .ignoreattcheck img, .pattl img')
                img_src_list = []
                filter_keywords = ['static/image/smiley', 'static/image/common', 'none.gif', 'zoom.png']
                
                for img in img_elements.items():
                    # 探测真实图片地址属性
                    src = img.attr('src') or img.attr('file') or img.attr('zoomfile')
                    if src:
                        src = src.strip()
                        if any(k in src for k in filter_keywords): continue
                        if src not in img_src_list:
                            img_src_list.append(src)
                
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
            if "Unicode strings with encoding declaration" in error_msg:
                error_type = "encoding_declaration_error"
            elif "ValueError" in str(type(e)):
                error_type = "value_error"
            elif "PyQueryError" in str(type(e)) or "etree" in error_msg:
                error_type = "parse_error"
            else:
                error_type = "unknown_error"
            
            should_stop = self._record_error_type(error_type)
            logger.error(f"解析详情页失败 ({error_type}): {url}, 错误: {e}")
            if not should_stop:
                import traceback
                logger.debug(f"详细错误信息: {traceback.format_exc()}")
        
        return {}
    
    # ==================== 数据处理 ====================
    
    def parse_torrent_get_magnet(self, refer: str, torrent_source: str, is_local: bool = False) -> Optional[tuple]:
        """解析torrent文件获取磁力链接和大小
        
        Args:
            refer: 引用页面URL
            torrent_source: torrent文件URL或本地路径
            is_local: 是否为本地文件
        
        Returns:
            tuple: (magnet_link, size_mb) 或 None
        """
        try:
            from curl_cffi import requests
            
            torrent_bin = None
            if is_local:
                with open(torrent_source, "rb") as f:
                    torrent_bin = f.read()
                if len(torrent_bin) == 0:
                    logger.error("错误：本地 torrent 文件为空")
                    return None
            else:
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
                
                if torrent_bin.lstrip().startswith(b'<!DOCTYPE') or torrent_bin.lstrip().startswith(b'<html'):
                    html_text = torrent_bin.decode('utf-8', errors='ignore')
                    if "var safeid" in html_text:
                        logger.info(f"下载种子时检测到年龄验证，解析并重试: {torrent_source}")
                        safeid = extract_safeid(torrent_bin)
                        if safeid:
                            self.cookie['_safe'] = safeid
                            header['Cookie'] = f"_safe={safeid}"
                            resp = requests.get(
                                torrent_source,
                                proxies=self.proxies,
                                headers=header,
                                allow_redirects=True,
                                timeout=10,
                                impersonate="chrome110"
                            )
                            resp.raise_for_status()
                            torrent_bin = resp.content
                    elif "抱歉，您需要登录" in html_text:
                        logger.warning(f"种子下载失败：需要登录 - {torrent_source}")
                        return None

                if len(torrent_bin) < 50:
                    logger.warning(f"警告：下载内容过小（{len(torrent_bin)} 字节），非合法 torrent 文件")
                    return None
            
            # 使用bencodepy解析torrent文件
            try:
                torrent_dict = bencodepy.decode(torrent_bin)
            except Exception as b_err:
                logger.error(f"Bencode解码失败: {b_err}")
                return None
                
            info_dict = None
            # 兼容 bytes 和 str key
            for k in [b"info", "info"]:
                if k in torrent_dict:
                    info_dict = torrent_dict[k]
                    break
            
            if not info_dict:
                logger.error("错误：种子缺少 info 核心字段")
                return None
            
            # --- 提取大小 (物理提取，增强调试) ---
            total_size_bytes = 0
            found_length = False
            
            # 调试：输出 info_dict 的所有键
            info_keys = list(info_dict.keys())
            logger.debug(f"种子 info_dict 包含的键: {info_keys}")
            
            # 单文件种子：直接有 length 字段
            for k in [b"length", "length"]:
                if k in info_dict:
                    total_size_bytes = int(info_dict[k])
                    found_length = True
                    logger.debug(f"✅ 单文件种子，length 字段值: {total_size_bytes} bytes")
                    break
            
            # 多文件种子：files 列表
            if not found_length:
                for k in [b"files", "files"]:
                    if k in info_dict:
                        files_list = info_dict[k]
                        logger.debug(f"✅ 多文件种子，files 列表包含 {len(files_list)} 个文件")
                        for idx, f in enumerate(files_list):
                            for fk in [b"length", "length"]:
                                if fk in f:
                                    file_size = int(f[fk])
                                    total_size_bytes += file_size
                                    if idx < 3:  # 只记录前3个文件，避免日志过多
                                        logger.debug(f"  文件 {idx+1}: {file_size} bytes")
                                    break
                        found_length = True
                        break
            
            # 计算 MB
            if total_size_bytes > 0:
                size_mb = int(total_size_bytes / (1024 * 1024))
                logger.info(f"✅ 物理种子解析完成，总字节: {total_size_bytes}, 大小: {size_mb}MB")
            else:
                size_mb = None
                logger.warning(f"⚠️ 种子解析成功但未提取到大小信息 (total_size_bytes={total_size_bytes})")
            
            # 计算 Hash 并提取名称
            info_bin = bencodepy.encode(info_dict)
            info_hash_hex = hashlib.sha1(info_bin).hexdigest()
            
            torrent_name = "Unknown_Torrent"
            for k in [b"name", "name"]:
                if k in info_dict:
                    torrent_name = info_dict[k]
                    break
            
            if isinstance(torrent_name, bytes):
                torrent_name = torrent_name.decode("utf-8", errors="ignore")
            
            encoded_name = urlencode({"dn": torrent_name})[3:]
            magnet_link = f"magnet:?xt=urn:btih:{info_hash_hex}&dn={encoded_name}"
            
            logger.debug(f"种子名称: {torrent_name[:50]}...")
            return (magnet_link, size_mb)
        
        except Exception as e:
            logger.error(f"解析torrent文件失败：{e}")
            return None
    
    def save_to_db(self, data: Dict, section: str, tid: int, detail_url: str) -> bool:
        """将爬取的数据保存到数据库"""
        return self._save_to_db(data, tid, section, detail_url)
    
    def _save_to_db(self, data: Dict, tid: int, section: str = None, detail_url: str = None) -> bool:
        """保存数据到数据库 (包含自动重试机制)"""
        try:
            from models import db, Resource
            from utils import retry_on_lock
            
            @retry_on_lock(max_retries=3, initial_delay=0.5)
            def _do_save():
                # 验证数据
                try:
                    from health import validator
                    validation_result = validator._validate_single(tid, detail_url, data)
                    if not validation_result['valid']:
                        logger.warning(f"❌ 保存前验证失败: tid={tid}, 原因: {', '.join(validation_result['reasons'])}")
                except Exception as e:
                    logger.warning(f"验证过程出错: {e}")
                    pass
                
                # 检查是否已存在
                existing_resource = Resource.query.filter_by(tid=tid).first()
                if existing_resource:
                    # 如果已存在，更新可能缺失的信息（如 size 或 images）
                    modified = False
                    if not existing_resource.size and data.get('size'):
                        existing_resource.size = data.get('size')
                        modified = True
                    if not existing_resource.preview_images and data.get('preview_images'):
                        existing_resource.preview_images = data.get('preview_images')
                        modified = True
                    
                    if modified:
                        db.session.commit()
                        logger.info(f"✓ 成功更新存量资源信息: tid={tid}")
                    else:
                        logger.debug(f"资源已存在，跳过: tid={tid}")
                    return False
                
                # 数据清理
                title = data.get('title', '').strip()[:500]
                sub_type = data.get('sub_type', '').strip()[:200] if data.get('sub_type') else None
                publish_date = self._normalize_date(data.get('publish_date', ''))
                
                # 创建新资源
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
                
                db.session.add(resource)
                db.session.commit()
                
                # 清理统计缓存
                try:
                    from cache_manager import cache_manager, CacheKeys
                    cache_manager.delete(CacheKeys.STATS)
                    cache_manager.delete(CacheKeys.CATEGORIES)
                except: pass
                
                logger.info(f"✓ 成功保存资源: tid={tid}, title={title[:50]}...")
                return True
            
            return _do_save()
        
        except Exception as e:
            if "database is locked" in str(e).lower(): raise e
            logger.error(f"保存资源到数据库失败: tid={tid}, 错误: {e}")
            try:
                from models import db
                db.session.rollback()
            except: pass
            return False
    
    def _normalize_date(self, date_str: str) -> Optional[str]:
        """标准化日期格式"""
        if not date_str: return None
        if len(date_str) == 10 and date_str.count('-') == 2: return date_str
        return date_str[:20]
    
    def _record_error_type(self, error_type: str) -> bool:
        """记录错误类型并检查是否应该停止爬取"""
        if self._should_stop_crawling: return True
        self._error_type_counter[error_type] = self._error_type_counter.get(error_type, 0) + 1
        count = self._error_type_counter[error_type]
        if count >= self._error_threshold:
            self._should_stop_crawling = True
            logger.error(f"⛔ [CRAWLER] 错误 '{error_type}' 达上限，停止爬取")
            return True
        return False
    
    def _adaptive_delay(self):
        """自适应延迟"""
        delay = random.uniform(self._normal_mode_delay[0], self._normal_mode_delay[1])
        time.sleep(delay)

    def _retry_with_delay(self, func, func_name: str, max_attempts: int = 3, delay_seconds: int = 5):
        """带重试机制的函数调用包装器"""
        result = None
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f" [{func_name}] 第 {attempt}/{max_attempts} 次尝试")
                result = func()
                if result: return result
            except Exception as e:
                logger.error(f"✗ [{func_name}] 出错: {e}")
            if attempt < max_attempts: time.sleep(delay_seconds)
        return {} if isinstance(result, dict) else None

    def _close_session(self):
        """清理会话"""
        pass
