// 统一的JavaScript入口文件
// 此文件负责加载所有核心模块

(function() {
    // 定义模块配置
    const MODULES = {
        // 核心模块 - 所有页面立即加载
        core: [
            'ios-optimizations.js',  // iOS/iPhone 优化
            'theme.js',              // 主题管理
            'sidebar.js',            // 侧边栏管理
            'page-title.js',         // 页面标题滚动（立即加载）
            'version.js',            // 版本信息（header 中需要）
            'toast.js'               // Toast 通知（所有页面系统通知需要）
        ],
        // 按需加载模块 - 用户交互时加载
        lazy: [
            'health.js'              // 健康检查（点击健康按钮时加载）
        ]
    };

    // 已加载模块缓存
    const loadedModules = new Set();

    // 动态加载模块
    function loadModule(moduleName) {
        if (loadedModules.has(moduleName)) {
            return Promise.resolve();
        }

        return new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = `/static/js/${moduleName}`;
            script.onload = () => {
                loadedModules.add(moduleName);
                console.log(`✅ 模块加载成功: ${moduleName}`);
                resolve();
            };
            script.onerror = () => {
                console.error(`❌ 模块加载失败: ${moduleName}`);
                reject(new Error(`Failed to load module: ${moduleName}`));
            };
            document.head.appendChild(script);
        });
    }

    // 批量加载模块
    function loadModules(moduleNames) {
        return Promise.all(moduleNames.map(name => loadModule(name)));
    }

    // 初始化核心模块
    function initCoreModules() {
        console.log('🚀 初始化核心模块...');
        loadModules(MODULES.core)
            .then(() => {
                console.log('✨ 所有核心模块加载完成');
            })
            .catch(error => {
                console.error('❌ 核心模块加载失败:', error);
            });
    }

    // 懒加载模块的入口函数
    function lazyLoad(moduleName) {
        if (MODULES.lazy.includes(moduleName) && !loadedModules.has(moduleName)) {
            return loadModule(moduleName);
        }
        return Promise.resolve();
    }

    // 导出到全局，供 HTML 中的 onclick 等事件使用
    window.ModuleLoader = {
        loadModule,
        loadModules,
        lazyLoad,
        loadedModules
    };

    // ========== 懒加载代理函数 ==========

    // 健康检查功能代理
    // 当用户点击健康检查按钮时，如果是第一次点击，先加载 health.js，然后调用真正的 toggleHealth
    window.toggleHealth = function() {
        if (loadedModules.has('health.js')) {
            // 如果已加载，health.js 应该已经覆盖了 toggleHealth 或提供了 healthManager
            if (window.healthManager && typeof window.healthManager.toggleHealth === 'function') {
                window.healthManager.toggleHealth();
            } else {
                console.warn('health.js 已加载但 healthManager 或 toggleHealth 未就绪');
            }
        } else {
            console.log('🔍 用户请求健康检查，开始懒加载 health.js...');
            // 显示加载中提示（可选，这里复用 refreshBtn 的动画如果存在，或者只是等待）
            // 简单的加载反馈
            const btn = document.querySelector('button[onclick="toggleHealth()"]');
            if (btn) btn.style.opacity = '0.5';

            loadModule('health.js').then(() => {
                if (btn) btn.style.opacity = '1';
                // 加载完成后，health.js 会重新定义 toggleHealth，但我们需要手动调用一次以响应本次点击
                // 注意：health.js 执行时会覆盖 window.toggleHealth
                if (window.healthManager && typeof window.healthManager.toggleHealth === 'function') {
                    window.healthManager.toggleHealth();
                } else {
                    console.error('health.js 加载成功但未正确暴露接口');
                }
            }).catch(err => {
                if (btn) btn.style.opacity = '1';
                console.error('懒加载 health.js 失败:', err);
                if (window.showToast) window.showToast('模块加载失败，请重试', 'error');
            });
        }
    };

    // 页面加载时初始化核心模块
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initCoreModules);
    } else {
        initCoreModules();
    }
})();

// 更新日志抽屉功能
window.toggleChangelog = function() {
    const drawer = document.getElementById('changelogDrawer');
    if (!drawer) return;

    const isVisible = drawer.classList.contains('show');

    if (isVisible) {
        drawer.classList.remove('show');
    } else {
        drawer.classList.add('show');
    }
};
