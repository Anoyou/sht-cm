#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫通知模块 - 负责发送Telegram通知和任务报告
"""

import logging
import datetime as _dt
import json
import os
import re
import importlib.util
from configuration import Config

logger = logging.getLogger(__name__)

DEFAULT_TEMPLATES = {
    "parse_mode": "MarkdownV2",
    "crawl_report": {
        "title": "{status_emoji} *{task_type_text}完成！*",
        "separator": "━━━━━━━━━━━━",
        "status_line": "📊 *完成状态*：{completion_status}",
        "exception_line": "⚠️ *异常原因*：{exception_reason}",
        "duration_line": "⏱️ *共耗时*：{duration}",
        "config_line": "📝 *本次爬取配置*：{crawl_config_desc}",
        "summary_line": "📈 *爬取合计*：新增{total_saved}个，跳过重复{total_skipped}个，失败并加入重试的有{total_failed}个",
        "section_header": "*具体板块*：",
        "section_line": "• {section_name}：爬取{pages_crawled}页 新增{saved}个，跳过重复{skipped}个，失败并加入重试的有{failed}个",
        "empty_section": "（无数据变动）"
    },
    "messages": {
        "initial_report": "🚀 *开始{task_type}，本次爬取配置：*\n板块：{all_boards}\n时间：{time_range}\n页数：{page_mode} \\- {page_desc}\n模式：{mode}\n\n━━━━━━━━━━━━\n📂 当前进行中的板块：{section_name}\n📄 板块 {section_name} 的实际任务页数：{actual_page_range}\n⏳ 候选中的板块：{pending_boards}",
        "board_switch": "✅ *{prev_board_name} 板块已完成，开始爬取候选板块*\n\n本次爬取配置：\n板块：{all_boards}\n时间：{time_range}\n页数：{page_mode} \\- {page_desc}\n模式：{mode}\n\n━━━━━━━━━━━━\n📂 当前进行中的板块：{section_name}\n📄 板块 {section_name} 的实际任务页数：{actual_page_range}\n✅ 已完成的板块：{completed_boards}\n⏳ 候选中的板块：{pending_boards}",
        "heartbeat": "💓 *爬取任务稳定进行中*\n━━━━━━━━━━━━\n⏱️ 已运行：{elapsed_minutes}分钟\n📂 当前板块：{section_name}\n📄 当前页码：{page_display} (板块进度: {section_progress_percent}%)\n📊 任务进度：{task_progress_display}\n📊 总体进度：{total_progress_percent}% ({processed_pages}/{estimated_total_pages}页)\n✅ 已保存：{total_saved}个\n⏭️ 已跳过：{total_skipped}个\n❌ 已失败：{total_failed}个\n⏰ 时间：{timestamp}",
        "crawler_thread_exception": "⚠️ *爬虫线程异常，任务可能中断*\n━━━━━━━━━━━━\n🔴 异常类型：{error_type}\n📝 错误信息：{error_message}\n📂 板块：{section_name}\n📄 页码：第{page_idx}页\n🔧 爬虫模式：{crawler_mode}\n⏰ 时间：{timestamp}\n\n🔧 系统已自动降级处理，可能会影响爬取质量",
        "section_error_alert": "⚠️ *板块爬取错误较多*\n━━━━━━━━━━━━\n📂 板块：{section_name}\n❌ 失败页数：{failure_count}\n📝 最新错误：{error_message}\n💡 任务继续进行中，但请关注",
        "error_limit_stop": "🛑 *错误累积过多，任务已自动停止*\n━━━━━━━━━━━━\n❌ 总失败数：{total_failed}个（超过阈值{threshold}个）\n📂 当前板块：{section_name}\n📄 当前页码：第{page_idx}页\n✅ 已保存：{total_saved}个\n⏭️ 已跳过：{total_skipped}个\n⏰ 时间：{timestamp}\n\n📝 建议：\n1. 检查网络连接和代理配置\n2. 检查目标网站是否有变化\n3. 稍后重试爬取任务",
        "task_stopped": "🛑 *任务已被停止*\n━━━━━━━━━━━━\n📂 最后进度：{section_name} 第{page_idx}页\n✅ 已保存：{total_saved}个\n⏭️ 已跳过：{total_skipped}个\n❌ 已失败：{total_failed}个\n⏰ 停止时间：{timestamp}\n\n✋ 任务已被强制停止",
        "crawler_thread_error": "❌ *爬虫任务异常终止*\n━━━━━━━━━━━━\n⚠️ 错误类型：{error_type}\n📝 错误信息：{error_message}\n⏰ 终止时间：{timestamp}\n💡 建议：{suggestion}",
        "crawler_error_stop": "🛑 *爬虫因错误过多已停止*\n━━━━━━━━━━━━\n❌ 错误类型：{error_type}\n🔢 错误次数：{count}\n📝 详情：{details}\n💡 建议：{suggestion}",
        "state_change": "🔔 *爬虫状态变更*\n━━━━━━━━━━━━\n{old_state} → {new_state}\n⏰ 时间：{timestamp}\n{reason}",
        "final_progress": "🧾 *终止前最后进度*\n━━━━━━━━━━━━\n🧭 终止原因：{completion_status}\n📂 当前板块：{section_name}\n📄 当前页码：{page_idx}\n📊 任务进度：{task_progress_display}\n📊 总体进度：{total_progress_percent}% ({processed_pages}/{estimated_total_pages}页)\n✅ 已保存：{total_saved}个\n⏭️ 已跳过：{total_skipped}个\n❌ 已失败：{total_failed}个\n⏰ 时间：{timestamp}"
    }
}


class _SafeDict(dict):
    def __missing__(self, key):
        return ""


def _normalize_template_string(template: str) -> str:
    if not isinstance(template, str):
        return ""
    return re.sub(r"\[['\"]([a-zA-Z0-9_]+)['\"]\]", r"[\1]", template)


def _format_template(template: str, context: dict, parse_mode: str = None) -> str:
    """格式化模板并替换占位符

    Args:
        template: 模板字符串
        context: 上下文字典
        parse_mode: MarkdownV2/Markdown/HTML，如果是 MarkdownV2 则需要转义占位符值

    Returns:
        格式化后的字符串
    """
    if not template:
        return ""

    # 如果是 MarkdownV2 模式，需要对上下文中的值进行转义
    if parse_mode == 'MarkdownV2' and context:
        escaped_context = {}
        for key, value in context.items():
            if isinstance(value, str):
                # 只转义变量值，不转义模板中已有的 Markdown 格式
                escaped_context[key] = escape_markdown_v2(value)
            else:
                escaped_context[key] = value
        context = escaped_context

    normalized = _normalize_template_string(template)
    return normalized.format_map(_SafeDict(context or {}))


def _load_py_templates(path: str) -> dict:
    try:
        spec = importlib.util.spec_from_file_location("telegram_templates", path)
        if not spec or not spec.loader:
            return {}
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        templates = getattr(module, "TEMPLATES", None) or getattr(module, "templates", None)
        return templates if isinstance(templates, dict) else {}
    except Exception as e:
        logger.warning(f"⚠️ 读取Telegram模板失败（Python）: {e}")
        return {}


def _build_default_templates_py() -> str:
    return """#!/usr/bin/env python3
# -*- coding: utf-8 -*-
\"\"\"
Telegram 通知模板（可直接编辑，保存后实时生效）

⚠️ 重要提示：
- 本文件定义了项目所有 Bot 通知消息的格式
- 修改后无需重启，实时生效
- 占位符必须用花括号包裹，例如 {task_type}
\"\"\"

# =============================================================================
# 占位符说明
# =============================================================================

# 【基础任务配置相关】
# {task_type}              # 任务类型文本（如"爬取任务"）
# {all_boards}             # 当前配置的爬取板块名称列表（多个用逗号分隔）
# {time_range}             # 当前配置的爬取时间范围（如"近3天"）
# {page_mode}              # 页数模式名称（如"固定页数"、"全部页面"）
# {page_desc}              # 页数描述（如"3页"）
# {mode}                   # 爬虫模式描述（如"异步并发(10并发)"）
# {crawl_config_desc}      # 爬取配置的完整描述（包含模式、板块、页数、时间）

# 【板块进度相关】
# {section_name}           # 当前正在爬取的板块名称
# {actual_page_range}      # 当前板块实际任务页码范围（如"1-3"）
# {pending_boards}         # 候选中的板块列表（待爬取）
# {completed_boards}        # 已完成爬取的板块列表
# {prev_board_name}        # 上一个完成的板块名称
# {section_progress_percent}  # 当前板块完成进度百分比
# {page_display}           # 当前页码显示（含板块总页数，如"2/10"）
# {pages_crawled}          # 板块已爬取的页数

# 【任务进度相关】
# {elapsed_minutes}        # 任务已运行分钟数
# {task_progress_display}  # 当前任务进度文本描述
# {total_progress_percent} # 整体任务完成进度百分比
# {processed_pages}        # 已处理的总页数
# {estimated_total_pages}  # 预计总页数

# 【统计结果相关】
# {total_saved}            # 总共新增保存的数量
# {total_skipped}          # 总共跳过重复的数量
# {total_failed}           # 总共失败的数量
# {saved}                  # 当前板块新增保存数量（仅 crawl_report.section_line）
# {skipped}                # 当前板块跳过重复数量（仅 crawl_report.section_line）
# {failed}                 # 当前板块失败数量（仅 crawl_report.section_line）

# 【错误和异常相关】
# {error_type}             # 错误类型（如"ConnectionError"）
# {error_message}          # 错误信息详情
# {crawler_mode}           # 爬虫模式类型（async/thread/sync）
# {failure_count}          # 失败计数（某板块连续失败页数）
# {threshold}              # 阈值（如失败阈值）
# {exception_reason}       # 异常原因描述
# {page_idx}               # 当前页码索引（仅用于 crawler_thread_exception/error_limit_stop/task_stopped）

# 【错误处理相关】（特定模板使用）
# {suggestion}             # 处理建议（仅 crawler_thread_error/crawler_error_stop）
# {count}                  # 错误次数（仅 crawler_error_stop）
# {details}                # 错误详情（仅 crawler_error_stop）

# 【状态变更相关】（特定模板使用）
# {old_state}              # 变更前的状态（仅 state_change）
# {new_state}              # 变更后的状态（仅 state_change）
# {reason}                 # 变更原因（仅 state_change）

# 【任务完成相关】
# {completion_status}      # 完成状态描述（如"爬取完成"、"手动终止"、"异常终止"）
# {duration}               # 总耗时（格式化后，如"2分30秒"）
# {status_emoji}           # 完成状态对应的 Emoji（✅/⏹️/❌）
# {task_type_text}         # 任务类型文本（报告专用，如"爬取任务"）
# {timestamp}              # 时间字符串（YYYY-MM-DD HH:MM:SS 格式）

# =============================================================================
# 注意事项
# =============================================================================
# 1. 分隔线统一使用 "━━━━━━━━━━━━"（12个横杠）
# 2. 占位符必须用花括号包裹，且不要在占位符中加引号
# 3. 支持嵌套变量写法：{initial_report_template[all_boards]} 会自动兼容替换

TEMPLATES = {
    # 消息解析模式：MarkdownV2 | Markdown | HTML
    "parse_mode": "MarkdownV2",

    # =============================================================================
    # 爬取结束报告模板（任务完成后发送的详细报告）
    # =============================================================================
    "crawl_report": {
        # 报告标题（带完成状态 Emoji）
        "title": "{status_emoji} *{task_type_text}完成！*",

        # 分隔线（统一使用12个横杠）
        "separator": "━━━━━━━━━━━━",

        # 完成状态行
        "status_line": "📊 *完成状态*：{completion_status}",

        # 异常原因行（仅在异常终止时显示）
        "exception_line": "⚠️ *异常原因*：{exception_reason}",

        # 总耗时行
        "duration_line": "⏱️ *共耗时*：{duration}",

        # 本次爬取配置描述（包含模式、板块、页数、时间）
        "config_line": "📝 *本次爬取配置*：{crawl_config_desc}",

        # 爬取合计统计（全局统计）
        "summary_line": "📈 *爬取合计*：新增{total_saved}个，跳过重复{total_skipped}个，失败并加入重试的有{total_failed}个",

        # 板块明细标题
        "section_header": "*具体板块*：",

        # 板块明细行（每个板块一行，包含爬取页数和各项统计）
        "section_line": "• {section_name}：爬取{pages_crawled}页 新增{saved}个，跳过重复{skipped}个，失败并加入重试的有{failed}个",

        # 无数据变动时的提示
        "empty_section": "（无数据变动）"
    },

    # =============================================================================
    # 实时消息模板（爬取过程中的各类通知）
    # =============================================================================
    "messages": {
        # ------------------------------------------------------------------------
        # 【开始爬取】任务启动时发送，显示爬取配置和初始状态
        # ------------------------------------------------------------------------
        "initial_report": "🚀 *开始{task_type}，本次爬取配置：*\n板块：{all_boards}\n时间：{time_range}\n页数：{page_mode} \\- {page_desc}\n模式：{mode}\n\n━━━━━━━━━━━━\n📂 当前进行中的板块：{section_name}\n📄 板块 {section_name} 的实际任务页数：{actual_page_range}\n⏳ 候选中的板块：{pending_boards}",

        # ------------------------------------------------------------------------
        # 【板块切换】完成一个板块后，切换到下一个板块时发送
        # ------------------------------------------------------------------------
        "board_switch": "✅ *{prev_board_name} 板块已完成，开始爬取候选板块*\n\n本次爬取配置：\n板块：{all_boards}\n时间：{time_range}\n页数：{page_mode} \\- {page_desc}\n模式：{mode}\n\n━━━━━━━━━━━━\n📂 当前进行中的板块：{section_name}\n📄 板块 {section_name} 的实际任务页数：{actual_page_range}\n✅ 已完成的板块：{completed_boards}\n⏳ 候选中的板块：{pending_boards}",

        # ------------------------------------------------------------------------
        # 【心跳通知】定期发送，展示任务运行状态和进度
        # ------------------------------------------------------------------------
        "heartbeat": "💓 *爬取任务稳定进行中*\n━━━━━━━━━━━━\n⏱️ 已运行：{elapsed_minutes}分钟\n📂 当前板块：{section_name}\n📄 当前页码：{page_display} (板块进度: {section_progress_percent}%)\n📊 任务进度：{task_progress_display}\n📊 总体进度：{total_progress_percent}% ({processed_pages}/{estimated_total_pages}页)\n✅ 已保存：{total_saved}个\n⏭️ 已跳过：{total_skipped}个\n❌ 已失败：{total_failed}个\n⏰ 时间：{timestamp}",

        # ------------------------------------------------------------------------
        # 【爬虫线程异常】线程执行异常时发送（如超时、崩溃等）
        # 使用占位符：error_type, error_message, section_name, page_idx, crawler_mode, timestamp
        # ------------------------------------------------------------------------
        "crawler_thread_exception": "⚠️ *爬虫线程异常，任务可能中断*\n━━━━━━━━━━━━\n🔴 异常类型：{error_type}\n📝 错误信息：{error_message}\n📂 板块：{section_name}\n📄 页码：第{page_idx}页\n🔧 爬虫模式：{crawler_mode}\n⏰ 时间：{timestamp}\n\n🔧 系统已自动降级处理，可能会影响爬取质量",

        # ------------------------------------------------------------------------
        # 【板块错误警报】某个板块失败页数过多时发送（但任务继续）
        # 使用占位符：section_name, failure_count, error_message
        # ------------------------------------------------------------------------
        "section_error_alert": "⚠️ *板块爬取错误较多*\n━━━━━━━━━━━━\n📂 板块：{section_name}\n❌ 失败页数：{failure_count}\n📝 最新错误：{error_message}\n💡 任务继续进行中，但请关注",

        # ------------------------------------------------------------------------
        # 【错误超限停止】总失败次数超过阈值时发送，任务自动停止
        # 使用占位符：total_failed, threshold, section_name, page_idx, total_saved, total_skipped, timestamp
        # ------------------------------------------------------------------------
        "error_limit_stop": "🛑 *错误累积过多，任务已自动停止*\n━━━━━━━━━━━━\n❌ 总失败数：{total_failed}个（超过阈值{threshold}个）\n📂 当前板块：{section_name}\n📄 当前页码：第{page_idx}页\n✅ 已保存：{total_saved}个\n⏭️ 已跳过：{total_skipped}个\n⏰ 时间：{timestamp}\n\n📝 建议：\n1. 检查网络连接和代理配置\n2. 检查目标网站是否有变化\n3. 稍后重试爬取任务",

        # ------------------------------------------------------------------------
        # 【任务停止】用户手动停止任务时发送
        # 使用占位符：section_name, page_idx, total_saved, total_skipped, total_failed, timestamp
        # ------------------------------------------------------------------------
        "task_stopped": "🛑 *任务已被停止*\n━━━━━━━━━━━━\n📂 最后进度：{section_name} 第{page_idx}页\n✅ 已保存：{total_saved}个\n⏭️ 已跳过：{total_skipped}个\n❌ 已失败：{total_failed}个\n⏰ 停止时间：{timestamp}\n\n✋ 任务已被强制停止",

        # ------------------------------------------------------------------------
        # 【线程错误】任务线程执行失败时发送（由 API 触发）
        # 使用占位符：error_type, error_message, timestamp, suggestion
        # ------------------------------------------------------------------------
        "crawler_thread_error": "❌ *爬虫任务异常终止*\n━━━━━━━━━━━━\n⚠️ 错误类型：{error_type}\n📝 错误信息：{error_message}\n⏰ 终止时间：{timestamp}\n💡 建议：{suggestion}",

        # ------------------------------------------------------------------------
        # 【错误过多停止】爬虫因连续错误过多而停止时发送
        # 使用占位符：error_type, count, details, suggestion
        # ------------------------------------------------------------------------
        "crawler_error_stop": "🛑 *爬虫因错误过多已停止*\n━━━━━━━━━━━━\n❌ 错误类型：{error_type}\n🔢 错误次数：{count}\n📝 详情：{details}\n💡 建议：{suggestion}",

        # ------------------------------------------------------------------------
        # 【状态变更】爬虫状态发生变化时发送（如 IDLE → RUNNING）
        # 使用占位符：old_state, new_state, timestamp, reason
        # ------------------------------------------------------------------------
        "state_change": "🔔 *爬虫状态变更*\n━━━━━━━━━━━━\n{old_state} → {new_state}\n⏰ 时间：{timestamp}\n{reason}",
    }
}
"""


def _deep_merge(base: dict, incoming: dict) -> dict:
    for k, v in incoming.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            base[k] = _deep_merge(base.get(k, {}), v)
        else:
            base[k] = v
    return base


def _resolve_templates_path() -> str:
    path = Config.get_path('telegram_templates')
    if path:
        return path
    config_dir = Config.get_path('config_dir')
    if config_dir:
        return os.path.join(config_dir, 'telegram_templates.py')
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(base_dir, '..', 'data', 'config', 'telegram_templates.py'))


def _ensure_templates_file(path: str):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            if path.endswith('.py'):
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(_build_default_templates_py())
            else:
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(DEFAULT_TEMPLATES, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"⚠️ 创建Telegram模板文件失败: {e}")


def load_telegram_templates() -> dict:
    path = _resolve_templates_path()
    _ensure_templates_file(path)
    try:
        if os.path.exists(path):
            if path.endswith('.py'):
                user_tpl = _load_py_templates(path)
            else:
                with open(path, 'r', encoding='utf-8') as f:
                    user_tpl = json.load(f)

            if not user_tpl and path.endswith('.py'):
                json_path = path.replace('.py', '.json')
                if os.path.exists(json_path):
                    with open(json_path, 'r', encoding='utf-8') as f:
                        user_tpl = json.load(f)

            merged = _deep_merge(json.loads(json.dumps(DEFAULT_TEMPLATES)), user_tpl or {})
            return merged
    except Exception as e:
        logger.warning(f"⚠️ 读取Telegram模板失败，使用默认模板: {e}")
    return json.loads(json.dumps(DEFAULT_TEMPLATES))


def render_message_template(template_key: str, context: dict) -> tuple[str, str]:
    """渲染消息模板

    Args:
        template_key: 模板键名（如 'initial_report'）
        context: 上下文字典，包含占位符的值

    Returns:
        tuple: (格式化后的消息, 解析模式)
    """
    templates = load_telegram_templates()
    message_tpl = templates.get('messages', {}).get(template_key)
    parse_mode = templates.get('parse_mode') or 'Markdown'
    if not message_tpl:
        return "", parse_mode
    return _format_template(message_tpl, context, parse_mode), parse_mode


def escape_markdown_v2(text: str) -> str:
    """转义 Telegram MarkdownV2 特殊字符"""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{char}' if char in escape_chars else char for char in str(text))


def build_crawl_report_message(summary: dict) -> tuple[str, str]:
    templates = load_telegram_templates()
    crawl_tpl = templates.get('crawl_report', {})
    parse_mode = templates.get('parse_mode') or 'Markdown'

    task_type_text = summary.get('task_type_text', '任务')
    completion_status = summary.get('completion_status', '爬取完成')
    exception_reason = summary.get('exception_reason')
    duration = summary.get('duration', {})
    results = summary.get('results', {})
    per_section = summary.get('per_section_results', {})
    crawl_conditions = summary.get('crawl_conditions', {})
    section_page_breakdown = summary.get('section_page_breakdown', {})
    engine_set = summary.get('engine_set', {})

    status_emoji = {
        '爬取完成': '✅',
        '手动终止': '⏹️',
        '异常终止': '❌'
    }.get(completion_status, '❓')

    formatted_duration = duration.get('formatted', '未知')

    # 生成详细的配置描述
    config_desc_parts = []
    target_sections = crawl_conditions.get('target_sections', [])
    
    # 1. 爬虫模式（异步/多线程/单线程）
    crawler_mode = engine_set.get('mode', 'async')
    mode_map = {
        'async': '异步并发',
        'thread': '多线程',
        'sync': '同步单线程'
    }
    mode_desc = mode_map.get(crawler_mode, crawler_mode)
    concurrency = engine_set.get('concurrency', 0)
    if crawler_mode == 'async' and concurrency:
        mode_desc += f" ({concurrency}并发)"
    elif crawler_mode == 'thread' and concurrency:
        mode_desc += f" ({concurrency}线程)"
    config_desc_parts.append(f"模式：{mode_desc}")
    
    # 2. 板块信息
    if target_sections and len(target_sections) > 0:
        from constants import SECTION_MAP
        section_names = []
        for fid in target_sections:
            # 同时支持整数和字符串类型的 fid
            fid_str = str(fid)
            fid_int = int(fid) if fid.isdigit() else None
            
            # 尝试用 fid_str 和 fid_int 匹配
            if fid_str in SECTION_MAP:
                section_names.append(SECTION_MAP[fid_str])
            elif fid_int is not None and fid_int in SECTION_MAP:
                section_names.append(SECTION_MAP[fid_int])
        
        if section_names:
            if len(section_names) <= 3:
                # 板块不多时，显示所有名称
                config_desc_parts.append(f"板块：{', '.join(section_names)}")
            else:
                # 板块太多时，显示前3个+数量
                config_desc_parts.append(f"板块：{', '.join(section_names[:3])} 等{len(section_names)}个")
    else:
        # 没有指定板块，说明是全部板块
        config_desc_parts.append("板块：全部")
    
    # 3. 页数配置
    page_mode = crawl_conditions.get('page_mode')
    max_pages = crawl_conditions.get('max_pages_per_section') or crawl_conditions.get('max_pages')
    page_range = crawl_conditions.get('page_range')
    
    if page_range and len(page_range) == 2:
        # 范围模式
        config_desc_parts.append(f"页数：第{page_range[0]}-{page_range[1]}页")
    elif page_mode == 'full':
        # 全部页面模式
        config_desc_parts.append(f"页数：全部页面")
    elif max_pages:
        # 固定页数模式
        config_desc_parts.append(f"页数：{max_pages}页")
    
    # 4. 时间范围配置
    date_filter = crawl_conditions.get('date_filter', {})
    date_mode = date_filter.get('mode')
    date_value = date_filter.get('value')
    dateline = date_filter.get('dateline')
    
    if date_mode and date_mode != 'all':
        time_desc_map = {
            'day': '单日',
            '1day': '单日',
            '2day': '近2天',
            '3day': '近3天',
            'week': '近1周',
            '1week': '近1周',
            'month': '近1月',
            '1month': '近1月',
            '3month': '近3月',
            '6month': '近半年',
            'year': '近1年',
            '1year': '近1年'
        }
        time_desc = time_desc_map.get(date_mode, date_mode)
        
        if date_value:
            config_desc_parts.append(f"时间：{date_value} ({time_desc})")
        elif time_desc:
            config_desc_parts.append(f"时间：{time_desc}")
        else:
            config_desc_parts.append(f"时间：{date_mode}")
    elif dateline:
        seconds = int(dateline)
        if seconds == 86400:
            config_desc_parts.append("时间：近1天")
        elif seconds == 604800:
            config_desc_parts.append("时间：近1周")
        elif seconds == 2592000:
            config_desc_parts.append("时间：近1月")
        elif seconds == 31536000:
            config_desc_parts.append("时间：近1年")
        else:
            days = seconds // 86400
            config_desc_parts.append(f"时间：近{days}天")
    
    # 组合配置描述
    crawl_config_desc = " | ".join(config_desc_parts) if config_desc_parts else "未知配置"

    context_base = {
        'status_emoji': status_emoji,
        'task_type_text': task_type_text,
        'completion_status': completion_status,
        'exception_reason': exception_reason,
        'duration': formatted_duration,
        'crawl_config_desc': crawl_config_desc,
        'total_saved': results.get('total_saved', 0),
        'total_skipped': results.get('total_skipped', 0),
        'total_failed': results.get('total_failed', 0)
    }

    msg = [
        _format_template(crawl_tpl.get('title', '{status_emoji} *{task_type_text}完成！*'), context_base, parse_mode),
        _format_template(crawl_tpl.get('separator', '━━━━━━━━━━━━'), context_base, parse_mode),
        _format_template(crawl_tpl.get('status_line', '📊 *完成状态*：{completion_status}'), context_base, parse_mode)
    ]

    if exception_reason:
        msg.append(_format_template(crawl_tpl.get('exception_line', '⚠️ *异常原因*：{exception_reason}'), context_base, parse_mode))

    msg.append(_format_template(crawl_tpl.get('duration_line', '⏱️ *共耗时*：{duration}'), context_base, parse_mode))

    msg.append(_format_template(crawl_tpl.get('config_line', '📝 *本次爬取配置*：{crawl_config_desc}'), context_base, parse_mode))

    msg.append(_format_template(crawl_tpl.get('summary_line', '📈 *爬取合计*：新增{total_saved}个，跳过重复{total_skipped}个，失败并加入重试的有{total_failed}个'), context_base, parse_mode))

    msg.append("")
    msg.append(_format_template(crawl_tpl.get('section_header', '*具体板块*：'), {}, parse_mode))

    has_detail = False
    for section_name in per_section.keys():
        section_data = per_section[section_name]
        saved = section_data.get('saved', 0)
        skipped = section_data.get('skipped', 0)
        failed = section_data.get('failed', 0)
        pages_crawled = 0
        if section_name in section_page_breakdown:
            pages_crawled = section_page_breakdown[section_name].get('total_pages', 0)

        if saved > 0 or skipped > 0 or failed > 0 or pages_crawled > 0:
            msg.append(_format_template(crawl_tpl.get('section_line', '• {section_name}：爬取{pages_crawled}页 新增{saved}个，跳过重复{skipped}个，失败并加入重试的有{failed}个'), {
                **context_base,
                'section_name': section_name,
                'pages_crawled': pages_crawled,
                'saved': saved,
                'skipped': skipped,
                'failed': failed
            }, parse_mode))
            has_detail = True

    if not has_detail:
        msg.append(_format_template(crawl_tpl.get('empty_section', '（无数据变动）'), {}, parse_mode))

    return "\n".join(msg), parse_mode


# v1.4.2 [修复] 使用全局线程池发送通知，防止网络波动阻塞爬虫主线程
# v1.5.4 [修复] 使用守护线程池，防止程序退出时过早关闭
import concurrent.futures
import threading

def _create_notif_pool():
    """创建通知线程池（延迟初始化）"""
    return concurrent.futures.ThreadPoolExecutor(
        max_workers=3, 
        thread_name_prefix="notif_"
    )

_notif_pool = _create_notif_pool()
_notif_pool_lock = threading.Lock()

def _send_telegram_message(text: str, parse_mode: str = None) -> bool:
    """发送Telegram消息 (非阻塞后台模式)"""
    token = Config.TG_BOT_TOKEN
    chat_id = Config.TG_NOTIFY_CHAT_ID

    if not token or not chat_id:
        return False

    def _sync_send():
        try:
            import requests
            from utils.retry_utils import retry_request, RETRY_CONFIG

            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {'chat_id': chat_id, 'text': text}
            if parse_mode: payload['parse_mode'] = parse_mode

            proxies = None
            if Config.PROXY:
                proxies = {'http': Config.PROXY, 'https': Config.PROXY}

            config = RETRY_CONFIG['telegram']
            # 这里保持原有的带重试逻辑，但在后台线程运行
            response = retry_request(
                requests.post,
                url=url,
                json=payload,
                proxies=proxies,
                raise_on_fail=False,
                **config
            )

            if response and response.status_code == 200:
                logger.info("✓ [TELEGRAM] 通知推送成功 (异步)")
                return True
            else:
                l_code = response.status_code if response else 'N/A'
                logger.warning(f"! [TELEGRAM] 异步推送失败 HTTP {l_code}")
                return False
        except Exception as e:
            logger.warning(f"❌ [TELEGRAM] 异步后台发送异常: {e}")
            return False

    # 🚀 立即提交任务到线程池并返回 True (表示已接受发送任务)
    try:
        global _notif_pool
        with _notif_pool_lock:
            # v1.5.4: 如果线程池已关闭，重新创建
            if _notif_pool._shutdown:
                logger.debug("🔄 通知线程池已关闭，重新创建")
                _notif_pool = _create_notif_pool()
            
            _notif_pool.submit(_sync_send)
        return True
    except Exception as e:
        logger.debug(f"⚠️ 无法提交通知任务: {e}")
        return False


def _send_crawl_report(summary: dict, force_send=False):
    """发送爬虫任务完成报告（优化版本）

    Args:
        summary: 爬取摘要数据
        force_send: 是否强制发送（用于异常情况下的备用通知）
    """
    try:
        # 先尝试发送带格式的消息
        text, parse_mode = build_crawl_report_message(summary)

        # 检查消息长度
        if len(text) > 4096:
            logger.warning(f"⚠️ 通知消息过长 ({len(text)} 字节)，将被截断")
            text = text[:4090] + "...\n\n[消息已截断]"

        # 先尝试 Markdown 格式
        success = _send_telegram_message(text, parse_mode=parse_mode)

        # 如果 Markdown 格式失败，降级到纯文本
        if not success:
            logger.warning("⚠️ Markdown 格式发送失败，降级到纯文本格式")
            # 移除所有 Markdown 格式符号，但保留 emoji
            plain_text = text
            plain_text = plain_text.replace('*', '')  # 移除粗体
            plain_text = plain_text.replace('_', '')  # 移除斜体（这是导致问题的字符！）
            plain_text = plain_text.replace('`', '')  # 移除代码
            plain_text = plain_text.replace('━━', '==')  # 替换分隔线
            plain_text = plain_text.replace('┃', '|')  # 替换竖线
            success = _send_telegram_message(plain_text, parse_mode=None)

            if not success:
                logger.error("❌ 纯文本格式也发送失败，通知推送失败")

        # 记录发送结果
        if success:
            logger.info(f"✅ [通知] 任务完成报告已发送 (保存:{summary.get('results', {}).get('total_saved', 0)}, 跳过:{summary.get('results', {}).get('total_skipped', 0)})")
        else:
            logger.error(f"❌ [通知] 任务完成报告发送失败")

        return success

    except Exception as e:
        logger.error(f"❌ Telegram通知推送失败: {e}")
        # 尝试发送简化的纯文本通知
        try:
            simple_msg = f"爬取任务完成\n新增: {summary.get('results', {}).get('total_saved', 0)}个\n跳过: {summary.get('results', {}).get('total_skipped', 0)}个\n耗时: {summary.get('duration', {}).get('formatted', '未知')}"
            success = _send_telegram_message(simple_msg, parse_mode=None)
            logger.info(f"✅ [通知] 已发送简化通知: {success}")
            return success
        except Exception as e2:
            logger.error(f"❌ 简化通知也发送失败: {e2}")
            return False
