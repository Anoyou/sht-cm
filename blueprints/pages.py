#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
页面路由 Blueprint - 处理所有页面渲染和静态文件
"""

from flask import Blueprint, render_template, jsonify, current_app
from datetime import datetime
from configuration import Config

pages_bp = Blueprint('pages', __name__)

# ==================== 主页面路由 ====================

@pages_bp.route('/')
def index():
    """主页"""
    return render_template('index_new.html')


@pages_bp.route('/config')
def config_page():
    """配置页面"""
    return render_template('config_new.html')


@pages_bp.route('/config/test')
def config_test_page():
    """配置测试页面 - 已弃用，重定向到配置页"""
    from flask import redirect, url_for
    return redirect(url_for('pages.config_page'))


@pages_bp.route('/categories')
def categories_page():
    """分类页面"""
    return render_template('categories_new.html')


@pages_bp.route('/crawler')
def crawler_page():
    """爬虫页面"""
    return render_template('crawler_new.html')


@pages_bp.route('/logs')
def logs_page():
    """日志页面"""
    return render_template('logs_new.html')


@pages_bp.route('/services')
def services_page():
    """服务页面"""
    return render_template('services_new.html')


# ==================== PWA 相关路由 ====================

@pages_bp.route('/manifest.json')
def manifest():
    """PWA manifest文件"""
    return current_app.send_static_file('manifest.json')


@pages_bp.route('/sw.js')
def service_worker():
    """Service Worker文件"""
    return current_app.send_static_file('sw.js')


# ==================== 测试和调试路由 ====================

@pages_bp.route('/test-refactor')
def test_refactor():
    """测试路由 - 用于验证代码更新"""
    return jsonify({
        'status': 'success',
        'message': '重构代码已生效！',
        'timestamp': datetime.now().isoformat(),
        'version': Config.VERSION,
        'templates': {
            'index': 'index_new.html',
            'config': 'config_new.html',
            'categories': 'categories_new.html',
            'logs': 'logs_new.html',
            'crawler': 'crawler_new.html'
        }
    })


@pages_bp.route('/reload-app', methods=['POST'])
def reload_app():
    """强制重载应用（仅开发环境）"""
    try:
        import sys

        # 清除模块缓存
        modules_to_reload = []
        for module_name in list(sys.modules.keys()):
            if module_name.startswith(('app', 'config', 'models')):
                modules_to_reload.append(module_name)

        for module_name in modules_to_reload:
            if module_name in sys.modules:
                del sys.modules[module_name]

        return jsonify({
            'status': 'success',
            'message': '应用模块缓存已清除，请重启服务以完全生效',
            'reloaded_modules': modules_to_reload,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500
