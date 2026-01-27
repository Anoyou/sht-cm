#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SHT2BM 适配器模块 - 将 SHT 数据层以 SHT2BM 兼容的 API 形式暴露
通过 Flask 蓝图集成到主应用中，提供标准化的 BT 搜索接口
"""

import re
import logging
from flask import Blueprint, request, jsonify
from models import db, Resource
from configuration import Config

# 配置日志
logger = logging.getLogger("SHT_BM_ADAPTER")

# 创建蓝图
# 注意：我们将在 app.py 中使用 url_prefix='/api' 注册它
sht2bm_bp = Blueprint('sht2bm', __name__)

# ==================== 辅助函数 (保留原逻辑以确保兼容) ====================

def extract_size_from_text(text):
    """从文本中提取大小并转换为MB"""
    if not text:
        return 0.0
    pattern = r'(\d+(?:\.\d+)?)\s*(GB|MB|KB|GIB|MIB|KIB)'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        try:
            value = float(match.group(1))
            unit = match.group(2).upper()
            if 'G' in unit:
                return value * 1024
            elif 'M' in unit:
                return value
            elif 'K' in unit:
                return value / 1024
        except:
            pass
    return 0.0

def determine_category_flags(title, sub_type, section):
    """根据标题和分类信息确定分类标志"""
    title_upper = str(title).upper()

    # chinese: 是否有中文字幕
    is_chinese = False
    if "中字" in title or "中英" in title:
        is_chinese = True
    if re.search(r'[-_]C\b', title_upper):
        is_chinese = True

    # uc: 是否为UC资源（无码/破解/流出/FC2/国产无码等）
    is_uc = False
    uc_keywords = ["无码", "破解", "流出", "FC2"]
    if any(keyword in title for keyword in uc_keywords):
        is_uc = True
    if "FC2" in title_upper:
        is_uc = True
    
    if sub_type and "无码" in sub_type:
        is_uc = True
    if section and ("无码" in section or "流出" in section):
        is_uc = True

    # uhd: 是否为4K超高清
    is_uhd = False
    if "4K" in title_upper or "8K" in title_upper:
        is_uhd = True

    return is_chinese, is_uc, is_uhd

def clean_title(title, sub_type):
    """清理和优化标题"""
    if not title:
        return "No Title"

    title = str(title).strip()
    title = re.sub(r'\s+', ' ', title)

    if title.lower() in ["none", "null"]:
        title = ""

    if not title and sub_type:
        title = sub_type

    if not title:
        title = "No Title"

    return title

def convert_to_torrent_model(resource: Resource):
    """将 Resource 模型对象转换为 Torrent Model"""
    
    # 确定id (使用 tid 作为唯一标识)
    torrent_id = resource.tid or resource.id

    # 确定site标识
    site = "SHT"
    if resource.detail_url and ("sehuatang" in resource.detail_url.lower()):
        site = "Sehuatang"

    # 确定size_mb
    size_mb = float(resource.size or 0)

    # 如果size为0，尝试从标题提取
    if size_mb == 0.0:
        size_mb = extract_size_from_text(resource.title)

    # seeders: 默认设置为999（表示资源可用）
    seeders = 999

    # title: 清理和优化标题
    title = clean_title(resource.title, resource.sub_type)

    # 确定分类标志
    chinese, uc, uhd = determine_category_flags(title, resource.sub_type, resource.section)

    return {
        "id": torrent_id,
        "site": site,
        "size_mb": round(size_mb, 2),
        "seeders": seeders,
        "title": title,
        "chinese": chinese,
        "uc": uc,
        "uhd": uhd,
        "free": True,
        "download_url": resource.magnet,
        "_raw_magnet": resource.magnet
    }

# ==================== 蓝图路由 ====================

@sht2bm_bp.route('/bt/health', methods=['GET'])
def bt_health():
    """SHT2BM API 健康检查端点"""
    return jsonify({
        'status': 'ok',
        'service': 'SHT2BM',
        'version': Config.VERSION,
        'integrated': True
    })


@sht2bm_bp.route('/bt', methods=['GET', 'POST'])
def bt_api():
    """BT资源搜索API - 集成版"""
    
    # 获取关键词
    keyword = request.args.get('keyword') or request.args.get('q')
    if not keyword and request.is_json:
        data = request.json or {}
        keyword = data.get('keyword')

    if not keyword:
        return jsonify({"data": []})

    logger.info(f"SHT2BM 搜索请求: [{keyword}]")

    # 使用 Resource 模型进行搜索
    # 限制返回 50 条结果以保持兼容性
    pagination = Resource.search_resources(keyword, page=1, per_page=50)
    resources = pagination.items

    # 转换为 Torrent Model 并去重
    results = []
    seen_magnets = set()
    
    for res in resources:
        if not res.magnet or res.magnet in seen_magnets:
            continue
            
        seen_magnets.add(res.magnet)
        results.append(convert_to_torrent_model(res))

    logger.debug(f"SHT2BM 搜索完成，返回 {len(results)} 条结果")
    return jsonify({"data": results})
