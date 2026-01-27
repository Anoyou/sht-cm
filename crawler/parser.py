#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTML 解析工具模块
提供各种 HTML 内容解析函数，用于从页面中提取特定信息
"""

import re
import logging
from datetime import datetime, timedelta
from typing import Optional
from pyquery import PyQuery as pq

logger = logging.getLogger(__name__)


def extract_and_convert_video_size(html_content: str) -> Optional[int]:
    """从 HTML 内容中提取并转换视频大小（增强版：跨域多点扫描）"""
    if not html_content: 
        logger.info("[SIZE_PARSER] ⚠️ html_content 为空")
        return None
    
    # 统一转换格式
    html_bytes = html_content if isinstance(html_content, bytes) else html_content.encode('utf-8')
    doc = pq(html_bytes)
    
    # 提取所有可能有大小的文本区域：正文 + 代码块 + 标题 + 网页 Title
    text_content = doc('.message, .t_f, .pcb, div.blockcode, h2.n5_bbsnrbt, #thread_subject, title').text()
    
    if not text_content:
        text_content = doc('body').text() or ""
    
    # 调试：输出提取到的文本（截取前500字符）
    # logger.info(f"[SIZE_PARSER] 提取到的文本长度: {len(text_content)}, 前300字符: {text_content[:300]}")
        
    # 定义级联匹配模式（由强指纹到弱指纹）
    patterns = [
        # 1. 强特征匹配：前缀标签
        r"(?:文件大小|Size|容量|影片大小)[\s：:]*(\d+(?:\.\d+)?)\s*(G|M)B?",
        # 2. 强特征匹配：中英文方块括号包裹
        r"[【\[]\s*(\d+(?:\.\d+)?)\s*(G|M)B?\s*[】\]]",
        # 3. 常见格式匹配（要求带B以防误触年份等数字）
        r"(\d+(?:\.\d+)?)\s*(G|M)B",
        # 4. 极端备选：数字后紧跟 G/M（不区分大小写，且后面不再有字母）
        r"(\d+(?:\.\d+)?)\s*(G|M)(?!\w)"
    ]
    
    for idx, pattern in enumerate(patterns, 1):
        match = re.search(pattern, text_content, re.IGNORECASE)
        if match:
            size_num_str, unit = match.groups()
            try:
                size_num = float(size_num_str)
                # 统一换算为 MB
                if unit.upper() == 'G':
                    result = int(size_num * 1024)
                elif unit.upper() == 'M':
                    result = int(size_num)
                else:
                    continue
                logger.info(f"[SIZE_PARSER] ✅ 模式 {idx} 匹配成功: {size_num}{unit} -> {result}MB")
                return result
            except Exception as e:
                logger.info(f"[SIZE_PARSER] 模式 {idx} 匹配但转换失败: {e}")
                continue
    
    logger.info(f"[SIZE_PARSER] ⚠️ 所有模式均未匹配到大小信息")
    return None


def extract_safeid(html_content: str) -> Optional[str]:
    """
    从 HTML 内容中提取 safeid（用于绕过年龄验证）
    """
    # 统一传递 bytes 给 pq
    html_bytes = html_content if isinstance(html_content, bytes) else html_content.encode('utf-8')
    doc = pq(html_bytes)
    
    # 遍历所有 script 标签查找 safeid
    for script_elem in doc('script'):
        script_text = pq(script_elem).text().strip()
        
        if not script_text or 'safeid' not in script_text:
            continue
            
        # 匹配 safeid = "xxx" 或 safeid = 'xxx'
        match = re.search(r"safeid\s*=\s*['\"]([^'\"]+)['\"]", script_text)
        if match:
            return match.group(1)
            
    return None


def extract_exact_datetime(html_content: str) -> Optional[str]:
    """
    从 HTML 内容中提取精确的日期时间
    """
    # 统一传递 bytes 给 pq
    html_bytes = html_content if isinstance(html_content, bytes) else html_content.encode('utf-8')
    doc = pq(html_bytes)
    
    # 提取日期文本
    # 手机版：dt.z.cl
    # 桌面版：.authi em (包含 "发表于 2024-1-1") 或 .pti .authi
    date_text = doc('dt.z.cl').eq(0).text().strip()
    if not date_text:
        date_text = doc('.authi em').eq(0).text().strip()
    if not date_text:
        date_text = doc('.pti .authi').text().strip()
    
    if not date_text:
        return ""
        
    # 清理文本
    processed_text = date_text.replace('&nbsp;', ' ').strip()
    processed_text = re.sub(r'\s+', ' ', processed_text)
    
    # 移除桌面版特有的 "发表于" 前缀
    processed_text = processed_text.replace('发表于 ', '').strip()
    
    today = datetime.now().date()
    
    # 匹配各种相对时间格式
    if re.match(r'^\d+ 小时前', processed_text):
        return today.strftime('%Y-%m-%d')
    elif processed_text.startswith('半小时前'):
        return today.strftime('%Y-%m-%d')
    elif re.match(r'^\d+ 分钟前', processed_text):
        return today.strftime('%Y-%m-%d')
    elif re.match(r'^\d+ 秒前', processed_text):
        return today.strftime('%Y-%m-%d')
    elif processed_text.startswith('昨天 '):
        yesterday = today - timedelta(days=1)
        return yesterday.strftime('%Y-%m-%d')
    elif processed_text.startswith('前天 '):
        day_before_yesterday = today - timedelta(days=2)
        return day_before_yesterday.strftime('%Y-%m-%d')
    elif re.match(r'^\d+ 天前', processed_text):
        days = int(re.search(r'(\d+) 天前', processed_text).group(1))
        target_date = today - timedelta(days=days)
        return target_date.strftime('%Y-%m-%d')
    elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', processed_text):
        # 标准格式，提取日期部分
        pure_date_str = processed_text.split(' ')[0]
        return pure_date_str
    else:
        logger.warning(f"! [PARSER] 无法解析日期格式: {date_text}")
        return None


def extract_bracket_content(html_content: str) -> Optional[str]:
    """
    从 HTML 内容中提取方括号 [] 内的内容
    """
    # 增加对桌面版标题嵌套格式的识别
    html_bytes = html_content if isinstance(html_content, bytes) else html_content.encode('utf-8')
    doc = pq(html_bytes)
    
    # 尝试多种标题容器
    h2_text = doc('h2.n5_bbsnrbt, #thread_subject, h1.ts').text()
    clean_text = h2_text.strip()
    
    # 特案：如果标题本身就是 "不详" (通常见于拦截重定向页)，则返回 None
    if clean_text == "不详": return None

    # 匹配第一个方括号内容
    pattern = r"\[(.*?)\]"
    match = re.search(pattern, clean_text)

    if match:
        return match.group(1)
    else:
        return None
