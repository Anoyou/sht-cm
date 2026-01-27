#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram 机器人服务模块 - 基于 python-telegram-bot 实现
支持资源查询、分类浏览、健康检查及抓取摘要推送等交互功能
"""

import logging
import asyncio
import traceback
import threading
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters
)
from telegram.request import HTTPXRequest
from configuration import Config
from scheduler.notifier import build_crawl_report_message
from models import db, Resource, Category
from utils import get_flask_app  # 使用统一的工具函数
from services.resource_service import UnifiedService
import os
import time
import random
from sqlalchemy import func, text
import requests
import datetime as dt

# 添加全局停止标志
bot_stop_event = threading.Event()

def stop_bot():
    """停止机器人的函数"""
    global bot_stop_event
    bot_stop_event.set()
    logger.info("收到停止信号，机器人将停止")

def is_bot_stopped():
    """检查机器人是否应该停止"""
    global bot_stop_event
    return bot_stop_event.is_set()

# 获取日志记录器 - 不再重复配置basicConfig
logger = logging.getLogger(__name__)


# ==================== BotNotifier 类 ====================
# 合并自 bot_notifier.py - 独立的通知发送功能

# ==================== BotNotifier 类 ====================
# 合并自 bot_notifier.py - 独立的通知发送功能

class BotNotifier:
    """机器人通知类 - 统一委托给 scheduler.notifier 处理"""

    @staticmethod
    def send_message(text: str, parse_mode: str = None) -> bool:
        """发送通用文本消息 (异步非阻塞)"""
        # 直接复用 scheduler.notifier 的线程池实现
        try:
            from scheduler.notifier import _send_telegram_message
            return _send_telegram_message(text, parse_mode=parse_mode)
        except Exception as e:
            logger.error(f"Telegram通知委托失败: {e}")
            return False

    @staticmethod
    def send_crawl_report(summary: dict):
        """发送爬虫任务完成报告"""
        # 直接复用 scheduler.notifier 的完整报告逻辑（含截断和降级）
        try:
            from scheduler.notifier import _send_crawl_report
            return _send_crawl_report(summary)
        except Exception as e:
            logger.error(f"Telegram报告委托失败: {e}")
            return False

# ==================== Bot 命令处理函数 ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /start 命令"""
    welcome_text = (
        "欢迎使用SHT资源查询机器人！\n\n"
        "我可以帮您：\n"
        "- 查询各类资源\n"
        "- 按分类筛选内容\n"
        "- 搜索特定资源\n\n"
        "使用 /help 查看帮助\n"
        "使用 /categories 查看所有分类"
    )
    await update.message.reply_text(welcome_text)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /help 命令"""
    help_text = (
        "使用说明：\n\n"
        "/start - 开始使用机器人\n"
        "/help - 显示帮助信息\n"
        "/categories - 显示所有分类\n"
        "/health - 检查系统健康状态\n"
        "/crawl - 执行资源抓取\n"
        "/fid - 查询板块信息"
        "/latest - 显示最新资源\n"
        "/search <关键词> - 搜索资源\n"
        "/summary - 显示最近一次抓取摘要\n\n"
        "点击分类按钮可以直接浏览对应分类的资源"
    )
    await update.message.reply_text(help_text)

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """检查系统健康状态"""
    from health import monitor
    from utils import get_flask_app
    
    try:
        flask_app = get_flask_app()
        with flask_app.app_context():
            # 获取统一健康摘要
            summary = monitor.get_summary()
            metrics = summary.get('metrics', {})
            db_info = metrics.get('db', {})
            sys_info = metrics.get('system', {})
            val_info = metrics.get('validation', {})
            
            # 状态表情
            status_emoji = "✅" if summary['status'] == 'healthy' else ("⚠️" if summary['status'] == 'warning' else "🚨")
            
            msg = [
                f"{status_emoji} *系统健康摘要*",
                f"━━━━━━━━━━━━━━",
                f"*状态*: {summary['status'].upper()} (得分: {summary['score']})",
                f"*数据库*: {db_info.get('resources', 0)} 资源 / {db_info.get('categories', 0)} 分类",
                f"*验证率*: {val_info.get('success_rate', 0)}% (最近24h)",
                f"*CPU/内存*: {sys_info.get('cpu', 0)}% / {sys_info.get('memory', {}).get('percent', 0)}%",
                f"*运行时间*: {sys_info.get('uptime_sec', 0) // 3600}小时"
            ]
            
            if summary['issues']:
                msg.append(f"\n*待处理问题*:\n• " + "\n• ".join(summary['issues']))
                
            await update.message.reply_text("\n".join(msg), parse_mode='Markdown')
            
    except Exception as e:
        logger.error(f"机器人健康检查失败: {e}")
        await update.message.reply_text(f"获取健康状态失败: {str(e)}")


async def fid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查看板块详情指令"""
    from crawler import SHT  # 从新的 crawler 模块导入
    
    await update.message.reply_text("🔍 正在获取板块实时数据，请稍候...")
    
    try:
        sht = SHT()
        # 获取所有激活分类
        flask_app = get_flask_app()
        with flask_app.app_context():
            categories = Category.get_all_active()
            
            if not categories:
                await update.message.reply_text("暂无激活的板块信息")
                return

            msg = ["📊 *板块实时深度概览*", "━━━━━━━━━━━━━━"]
            
            for cat in categories:
                # 获取远程实时数据（含总页数、主题数）
                remote_info = sht.get_forum_info(cat.fid)
                
                # 本地存量数据
                local_count = cat.resource_count
                
                msg.append(f"📁 *{cat.name}* (FID: {cat.fid})")
                msg.append(f"  • 远程主题: {remote_info.get('total_topics', '未知')}")
                msg.append(f"  • 远程页数: {remote_info.get('total_pages', '未知')}")
                msg.append(f"  • 本地存量: {local_count}")
                msg.append("")
                
            await update.message.reply_text("\n".join(msg), parse_mode='Markdown')
            
    except Exception as e:
        logger.error(f"FID 查询失败: {e}")
        await update.message.reply_text(f"❌ 获取板块信息失败: {str(e)}")


async def categories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示所有分类"""
    flask_app = get_flask_app()
    with flask_app.app_context():
        # 使用优化后的模型方法
        categories = Category.get_all_categories()
    
    if not categories:
        await update.message.reply_text("暂无分类信息")
        return
    
    keyboard = []
    for i, cat in enumerate(categories):
        if i % 2 == 0:
            # 每两个按钮一行
            if i + 1 < len(categories):
                keyboard.append([
                    InlineKeyboardButton(
                        cat.name, 
                        callback_data=f"cat_{cat.fid}"
                    ),
                    InlineKeyboardButton(
                        categories[i+1].name, 
                        callback_data=f"cat_{categories[i+1].fid}"
                    )
                ])
            else:
                keyboard.append([
                    InlineKeyboardButton(
                        cat.name, 
                        callback_data=f"cat_{cat.fid}"
                    )
                ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("请选择一个分类：", reply_markup=reply_markup)


async def latest_resources(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示最新资源"""
    flask_app = get_flask_app()
    with flask_app.app_context():
        # 使用统一服务层获取资源
        result = UnifiedService.resource_service.get_resources_with_filters(
            page=1,
            per_page=10,
            order_by='created_at'
        )
        resources = result.get('resources', [])

    if not resources:
        await update.message.reply_text("暂无资源数据")
        return

    message = "最新资源：\n\n"
    for resource in resources:
        message += (
            f"标题: {resource.get('title', '未知')}\n"
            f"分类: {resource.get('section', '未知')}\n"
            f"日期: {resource.get('publish_date', '未知')}\n"
            f"详情: {resource.get('detail_url', '未知')}\n\n"
        )

    await update.message.reply_text(message)


async def search_resources(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """搜索资源"""
    query = " ".join(context.args) if context.args else ""

    if not query:
        await update.message.reply_text("请提供搜索关键词，例如：/search 关键词")
        return

    flask_app = get_flask_app()
    with flask_app.app_context():
        # 使用统一服务层搜索资源
        result = UnifiedService.resource_service.get_resources_with_filters(
            page=1,
            per_page=10,
            search=query,
            order_by='created_at'
        )
        resources = result.get('resources', [])

    if not resources:
        await update.message.reply_text(f"未找到包含关键词 '{query}' 的资源")
        return

    message = f"搜索结果 (关键词: {query})：\n\n"
    for resource in resources:
        message += (
            f"标题: {resource.get('title', '未知')}\n"
            f"分类: {resource.get('section', '未知')}\n"
            f"日期: {resource.get('publish_date', '未知')}\n"
            f"详情: {resource.get('detail_url', '未知')}\n\n"
        )

    await update.message.reply_text(message)


async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示最近一次抓取摘要"""
    try:
        import json, os, time
        from configuration import Config
        path = Config.get_path('summary_json')
        if not path or not os.path.exists(path):
            await update.message.reply_text("暂无抓取摘要")
            return
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        lines = [
            f"🎉 {data.get('task_type_text', '任务')}完成",
            f"耗时：{data.get('duration', {}).get('formatted', '未知')}",
            f"资源：新增 {data.get('results', {}).get('total_saved', 0)}，跳过 {data.get('results', {}).get('total_skipped', 0)}，失败 {data.get('results', {}).get('total_failed', 0)}"
        ]
        per = data.get('per_section_results', {})
        for name, cnt in per.items():
            saved = cnt.get('saved', 0)
            skipped = cnt.get('skipped', 0)
            failed = cnt.get('failed', 0)
            if saved or skipped or failed:
                lines.append(f"• {name}: +{saved} / -{skipped} / !{failed}")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"读取摘要失败: {e}")


def main():
    """启动机器人"""
    global bot_stop_event
    
    config = Config()
    
    if not config.TG_BOT_TOKEN:
        logger.error("未设置TG_BOT_TOKEN")
        return

    # 重置停止标志
    bot_stop_event.clear()

    # 简单的重试循环
    while not is_bot_stopped():
        try:
            logger.info("正在初始化机器人应用...")
            
            # 检查是否被停止
            if is_bot_stopped():
                logger.info("检测到停止信号，退出初始化")
                break
            
            # 配置代理和自定义请求设置（应对不稳定的代理连接）
            proxy_url = config.PROXY
            
            # 使用自定义 HTTPXRequest 以增加超时时间，减少 Server disconnected 错误
            t_request = HTTPXRequest(
                connect_timeout=20.0,
                read_timeout=30.0,
                proxy_url=proxy_url if proxy_url else None
            )
            
            builder = Application.builder().token(config.TG_BOT_TOKEN).request(t_request)
            
            if proxy_url:
                logger.info(f"使用代理: {proxy_url}")
                # 提示：get_updates_proxy 会在 HTTPXRequest 内部处理
            
            application = builder.build()
            
            logger.info("注册处理器...")
            # 注册处理器
            application.add_handler(CommandHandler("start", start))
            application.add_handler(CommandHandler("help", help_command))
            application.add_handler(CommandHandler("health", health))
            application.add_handler(CommandHandler("categories", categories))
            application.add_handler(CommandHandler("latest", latest_resources))
            application.add_handler(CommandHandler("search", search_resources))
            application.add_handler(CommandHandler("summary", summary))
            application.add_handler(CommandHandler("fid", fid_command))
            application.add_handler(CallbackQueryHandler(button_click))
            application.add_handler(CommandHandler("browse", browse))
            
            logger.info("机器人启动中...")
            
            # 在子线程中运行时，需要使用不同的启动方式
            try:
                # 检查当前线程是否为主线程
                if threading.current_thread() is not threading.main_thread():
                    logger.info("在子线程中运行，使用异步方式启动")
                    
                    # 为当前线程创建新的事件循环
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    # 使用异步方式启动，避免信号处理器问题
                    async def run_bot():
                        try:
                            await application.initialize()
                            await application.start()
                            
                             # 开始轮询
                            await application.updater.start_polling(
                                drop_pending_updates=True
                            )
                            
                            # 保持运行直到收到停止信号
                            while not is_bot_stopped():
                                await asyncio.sleep(1)
                                
                        except Exception as e:
                            logger.error(f"异步运行机器人时出错: {e}")
                            raise
                        finally:
                            # 清理资源
                            try:
                                await application.updater.stop()
                                await application.stop()
                                await application.shutdown()
                            except Exception as e:
                                logger.error(f"清理机器人资源时出错: {e}")
                    
                    # 运行异步函数
                    logger.info("开始异步运行机器人...")
                    loop.run_until_complete(run_bot())
                    
                else:
                    # 主线程中可以正常使用run_polling
                    logger.info("在主线程中运行，使用标准polling...")
                    application.run_polling(drop_pending_updates=True)
                
            except Exception as inner_e:
                logger.error(f"启动机器人时出错: {inner_e}")
                if is_bot_stopped():
                    break
                raise inner_e
            
            # 如果正常退出（例如收到 SIGINT），则跳出循环
            logger.info("机器人已停止")
            break
            
        except Exception as e:
            if is_bot_stopped():
                logger.info("检测到停止信号，退出重试循环")
                break
            logger.error(f"机器人运行出错: {e}")
            logger.error(f"错误详细信息:\n{traceback.format_exc()}")
            logger.info("等待 10 秒后重启...")
            
            # 在等待期间也检查停止信号
            for i in range(10):
                if is_bot_stopped():
                    logger.info("在等待期间收到停止信号")
                    return
                time.sleep(1)

async def browse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """交互式分类浏览 (入口)"""
    await show_browse_menu(update, context)

async def show_browse_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, edit_mode=False):
    """显示或更新浏览菜单"""
    user_data = context.user_data
    
    # 1. 获取当前设置 (默认值)
    current_date = user_data.get('browse_date', 'all')
    current_per = user_data.get('browse_per', 10)
    
    # 2. 构建状态显示文本
    date_map = {
        'all': '全部', '1d': '近1天', '3d': '近3天', 
        '1w': '近1周', '1m': '近1月'
    }
    status_text = (
        f"📂 *资源分类浏览*\n"
        f"━━━━━━━━━━━━━━\n"
        f"📅 时间范围: {date_map.get(current_date, current_date)}\n"
        f"📄 每页显示: {current_per}条\n\n"
        f"👇 *请选择分类开始浏览*:"
    )
    
    # 3. 获取分类列表
    flask_app = get_flask_app()
    with flask_app.app_context():
        cats = Category.get_all_categories()
    
    if not cats:
        text = "暂无分类数据"
        if edit_mode:
            await update.callback_query.edit_message_text(text)
        else:
            await update.message.reply_text(text)
        return

    # 4. 构建按钮键盘
    keyboard = []
    
    # 分类按钮 (每行3个)
    for i in range(0, len(cats), 3):
        row = []
        for j in range(i, min(i+3, len(cats))):
            # 将分类操作直接指向 browse_cat，不再需要携带无关参数，参数从 user_data 读取
            row.append(InlineKeyboardButton(
                cats[j].name, 
                callback_data=f"browse_cat|{cats[j].name}"
            ))
        keyboard.append(row)
    
    # 功能分隔线
    keyboard.append([InlineKeyboardButton("⚙️ 筛选设置 ⚙️", callback_data="noop")])
    
    # 日期筛选行
    def _d_btn(label, val):
        prefix = "✅ " if current_date == val else ""
        return InlineKeyboardButton(f"{prefix}{label}", callback_data=f"browse_set_date|{val}")
        
    date_row = [
        _d_btn("1天", "1d"), _d_btn("3天", "3d"), 
        _d_btn("1周", "1w"), _d_btn("1月", "1m"),
        _d_btn("全部", "all")
    ]
    keyboard.append(date_row)
    
    # 每页数量行
    def _p_btn(val):
        prefix = "✅ " if current_per == val else ""
        return InlineKeyboardButton(f"{prefix}{val}条/页", callback_data=f"browse_set_per|{val}")
        
    per_row = [
        _p_btn(5), _p_btn(10), _p_btn(20)
    ]
    keyboard.append(per_row)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # 5. 发送或更新消息
    if edit_mode:
        # 避免内容无变化时报错
        try:
            await update.callback_query.edit_message_text(
                text=status_text, 
                reply_markup=reply_markup, 
                parse_mode='Markdown'
            )
        except Exception:
            pass # 内容未变忽略
    else:
        await update.message.reply_text(status_text, reply_markup=reply_markup, parse_mode='Markdown')

async def show_category_resources(update_obj, context, category_name, page=1):
    """显示指定分类的资源列表 (使用 UnifiedService 直连数据库)"""
    user_data = context.user_data
    per_page = user_data.get('browse_per', 10)
    date_mode = user_data.get('browse_date', 'all')
    
    # 计算日期筛选
    date_start = None
    if date_mode != 'all':
        now = dt.date.today()
        if date_mode == '1d':
            date_start = (now - dt.timedelta(days=1)).isoformat()
        elif date_mode == '3d':
            date_start = (now - dt.timedelta(days=3)).isoformat()
        elif date_mode == '1w':
            date_start = (now - dt.timedelta(days=7)).isoformat()
        elif date_mode == '1m':
            date_start = (now - dt.timedelta(days=30)).isoformat()
    
    try:
        flask_app = get_flask_app()
        with flask_app.app_context():
            # 直接调用服务层，不再走 HTTP
            result = UnifiedService.resource_service.get_resources_with_filters(
                page=page,
                per_page=per_page,
                category=category_name,
                date_start=date_start,
                order_by='created_at' # 默认按时间倒序
            )
            
        items = result.get('resources', [])
        total_pages = result.get('pages', 1)
        current_page = result.get('current_page', 1)
        total_items = result.get('total', 0)
        
        if not items:
            await update_obj.edit_message_text(
                f"📂 分类: *{category_name}*\n⚠️ 当前筛选条件下暂无资源",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 返回菜单", callback_data="browse_menu")]])
            )
            return

        # 构建资源列表消息
        msg = f"📂 *{category_name}* (共 {total_items} 个)\n"
        msg += f"📄 第 {current_page}/{total_pages} 页 | 📅 {date_mode} | 👁️ {per_page}条\n"
        msg += "━━━━━━━━━━━━━━\n\n"
        
        for r in items:
            title = r.get('title', '无标题').replace('[', '(').replace(']', ')')
            size_str = f"📦 {r['size']}" if r.get('size') else ""
            date_str = f"🕒 {r['publish_date']}" if r.get('publish_date') else ""
            meta_line = f"{date_str} {size_str}".strip()
            
            # 详情链接
            link = r.get('detail_url', '#')
            
            msg += f"🎬 [{title}]({link})\n"
            if meta_line:
                msg += f"_{meta_line}_\n\n"
        
        # 构建翻页按钮
        nav_buttons = []
        if current_page > 1:
            nav_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"browse_page|{category_name}|{current_page-1}"))
        
        nav_buttons.append(InlineKeyboardButton("🔙 菜单", callback_data="browse_menu"))
        
        if current_page < total_pages:
            nav_buttons.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"browse_page|{category_name}|{current_page+1}"))
            
        await update_obj.edit_message_text(
            msg, 
            parse_mode='Markdown', 
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup([nav_buttons])
        )
        
    except Exception as e:
        logger.error(f"浏览资源失败: {e}")
        await update_obj.edit_message_text(f"❌ 获取数据失败: {str(e)}")

async def button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理按钮点击回调 (v1.5.5 重构版)"""
    query = update.callback_query
    await query.answer() # 立即响应避免进度条转圈
    data = query.data or ""
    
    # 1. 浏览主菜单返回
    if data == "browse_menu":
        await show_browse_menu(update, context, edit_mode=True)
        return
        
    # 2. 修改日期设置
    if data.startswith("browse_set_date|"):
        val = data.split("|")[1]
        context.user_data['browse_date'] = val
        # 刷新菜单显示选中状态
        await show_browse_menu(update, context, edit_mode=True)
        return
        
    # 3. 修改每页数量 设置
    if data.startswith("browse_set_per|"):
        val = int(data.split("|")[1])
        context.user_data['browse_per'] = val
        # 刷新菜单显示选中状态
        await show_browse_menu(update, context, edit_mode=True)
        return
        
    # 4. 选择分类 (进入资源列表)
    if data.startswith("browse_cat|"):
        cat_name = data.split("|")[1]
        # 重置页码为1
        await show_category_resources(query, context, cat_name, page=1)
        return
        
    # 5. 翻页操作
    if data.startswith("browse_page|"):
        parts = data.split("|")
        cat_name = parts[1]
        page = int(parts[2])
        await show_category_resources(query, context, cat_name, page=page)
        return
    
    # --- 兼容旧版及其他直接命令 ---
    
    if data == "noop":
        return
        
    if data.startswith("cat_"):
        # 旧版/categories命令的分类点击
        cat_fid = data[4:]
        flask_app = get_flask_app()
        with flask_app.app_context():
            category = UnifiedService.category_service.get_category_by_fid(cat_fid)
            if not category:
                await query.edit_message_text("未找到指定分类")
                return
            resources = UnifiedService.resource_service.get_latest_resources_by_category(
                category_name=category.name, limit=10
            )
        
        if not resources:
            await query.edit_message_text(f"分类 '{category.name}' 下暂无资源")
            return
            
        msg = f"📁 分类: *{category.name}* (最新10条)\n\n"
        for r in resources:
            msg += f"🎬 {r.title}\n📅 {r.publish_date}\n🔗 {r.detail_url}\n\n"
            
        await query.edit_message_text(msg, parse_mode='Markdown', disable_web_page_preview=True)
        return

if __name__ == '__main__':
    main()