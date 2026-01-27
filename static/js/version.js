// 版本信息模块 (懒加载)

(function() {
    // 版本信息加载
    async function loadVersion() {
        try {
            console.log('[版本] 开始加载版本信息...');
            const r = await fetch('/api/version');
            console.log('[版本] API响应状态:', r.status);

            if (!r.ok) {
                throw new Error(`HTTP ${r.status}: ${r.statusText}`);
            }

            const v = await r.json();
            console.log('[版本] API响应数据:', v);

            // 处理两种可能的响应格式
            const version = v.version || (v.data && v.data.version) || '-';
            console.log('[版本] 提取的版本号:', version);

            const versionEl = document.getElementById('versionText');
            if (versionEl) {
                versionEl.textContent = 'v' + version;
                console.log('[版本] 版本号已更新到界面');
            } else {
                console.warn('[版本] 找不到版本显示元素');
            }
        } catch (error) {
            console.error('[版本] 加载版本失败:', error);

            // 显示错误状态
            const versionEl = document.getElementById('versionText');
            if (versionEl) {
                versionEl.textContent = '加载失败';
            }
        }
    }

    // 导出函数供其他模块使用
    window.versionManager = {
        loadVersion
    };

    // 页面加载时初始化（立即加载，不等待 DOMContentLoaded）
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', loadVersion);
    } else {
        loadVersion();
    }
})();
