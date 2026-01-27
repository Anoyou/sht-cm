// 健康检查模块 (懒加载)

(function() {
    function toggleHealth() {
        const drawer = document.getElementById('healthDrawer');
        if (!drawer) return;

        const isVisible = drawer.classList.contains('show');

        if (isVisible) {
            drawer.classList.remove('show');
        } else {
            drawer.classList.add('show');
            loadHealthData();
        }
    }

    function refreshHealth() {
        const refreshBtn = document.querySelector('#healthDrawer .icon-btn');
        if (refreshBtn) {
            refreshBtn.classList.add('animate-spin');
            setTimeout(() => {
                refreshBtn.classList.remove('animate-spin');
            }, 600);
        }
        loadHealthData(true);
    }

    // 统一的健康检查数据加载函数（最完整版本）
    function loadHealthData(forceRefresh = false) {
        const contentDiv = document.getElementById('healthContent');
        if (!contentDiv) return;

        contentDiv.innerHTML = '<div>检测中...</div>';

        const url = forceRefresh ? '/api/health?force=true' : '/api/health';

        fetch(url).then(r => r.json()).then(h => {
            const getStatusText = (status) => {
                switch (status) {
                    case 'healthy': return '正常';
                    case 'warning': return '警告';
                    case 'critical': return '严重';
                    default: return '未知';
                }
            };

            const getStatusColor = (status) => {
                switch (status) {
                    case 'healthy': return 'var(--emerald-500)';
                    case 'warning': return 'var(--amber-500)';
                    case 'critical': return 'var(--rose-500)';
                    default: return 'var(--muted-foreground)';
                }
            };

            // SHT2BM 状态显示
            let sht2bmHtml = '';
            if (h.sht2bm) {
                const sht2bmStatus = h.sht2bm.status;
                const sht2bmColor = sht2bmStatus === 'ok' ? 'var(--emerald-500)' :
                    sht2bmStatus === 'disabled' ? 'var(--muted-foreground)' : 'var(--rose-500)';
                const sht2bmText = sht2bmStatus === 'ok' ? '正常' :
                    sht2bmStatus === 'disabled' ? '未启用' : '异常';

                sht2bmHtml = `<div style="display: flex; justify-content: space-between; align-items: center;">
                    <span>SHT2BM API: <span style="color: ${sht2bmColor};">${sht2bmText}</span></span>`;

                if (h.sht2bm.enabled) {
                    // 修复：根据实际状态显示按钮文字
                    if (sht2bmStatus === 'ok') {
                        sht2bmHtml += `<button onclick="controlSht2bm('stop')" class="btn btn-xs btn-destructive">停止</button>`;
                    } else {
                        sht2bmHtml += `<button onclick="controlSht2bm('start')" class="btn btn-xs btn-success">启动</button>`;
                    }
                }
                sht2bmHtml += '</div>';

                if (h.sht2bm.enabled && h.sht2bm.details && h.sht2bm.details.port) {
                    sht2bmHtml += `<div style="font-size: 11px; color: var(--muted-foreground); margin-left: 16px;">端口: ${h.sht2bm.details.port}</div>`;
                }
            }

            // Telegram Bot HTML
            const telegramEnabled = h.config?.telegram_enabled;
            const telegramHtml = `<div style="display: flex; justify-content: space-between; align-items: center;">
                <span>Telegram Bot: ${telegramEnabled ? '已启用' : '未启用'}</span>
                ${telegramEnabled ? '<button onclick="testTelegramBot()" class="btn btn-xs btn-primary">测试</button>' : ''}
            </div>`;

            contentDiv.innerHTML = `
                <div>状态: <span style="color: ${getStatusColor(h.status)};">${getStatusText(h.status)}</span></div>
                <div>数据库: <span style="color: ${h.database?.status === 'ok' ? 'var(--emerald-500)' : 'var(--rose-500)'};">${h.database?.status === 'ok' ? '正常' : '异常'}</span></div>
                <div>分类数: ${h.database?.categories || 0}</div>
                <div>资源数: ${h.database?.resources || 0}</div>
                ${sht2bmHtml}
                <div>版本: ${h.version || '未知'}</div>
                ${telegramHtml}
                <div>代理: ${h.config?.proxy_enabled ? '已启用' : '未启用'}</div>
                <div>缓存: ${h.cache?.cache_type || '未知'}</div>
            `;
        }).catch(() => {
            contentDiv.innerHTML = '<div style="color: var(--rose-500);">健康检查失败</div>';
        });
    }

    // SHT2BM控制函数
    async function controlSht2bm(action) {
        try {
            const response = await fetch(`/api/tasks/sht2bm_service/${action}`, {
                method: 'POST'
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const result = await response.json();

            if (result.status === 'success') {
                showToast(`SHT2BM服务${action === 'start' ? '启动' : '停止'}成功`, 'success');
                // 延迟刷新健康状态
                setTimeout(() => {
                    loadHealthData(true);
                }, 2000);
            } else {
                showToast(`操作失败: ${result.message}`, 'error');
            }

        } catch (error) {
            console.error('SHT2BM控制失败:', error);
            showToast(`操作失败: ${error.message}`, 'error');
        }
    }

    // Telegram Bot测试函数
    async function testTelegramBot() {
        try {
            showToast('正在发送测试消息...', 'info');

            const response = await fetch('/api/config/test-telegram', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({})  // 使用服务器配置的token和chat_id
            });

            const result = await response.json();

            if (result.status === 'success') {
                showToast('测试消息发送成功！请检查 Telegram', 'success', 5000);
            } else {
                showToast('测试失败: ' + (result.message || '未知错误'), 'error', 5000);
            }
        } catch (error) {
            console.error('测试 Telegram Bot 失败:', error);
            showToast('测试失败: ' + error.message, 'error', 5000);
        }
    }

    // 导出函数供其他模块使用
    window.healthManager = {
        toggleHealth,
        refreshHealth,
        loadHealthData,
        controlSht2bm,
        testTelegramBot
    };

    // 全局绑定（为了兼容 HTML 中的 onclick 属性）
    window.toggleHealth = toggleHealth;
    window.refreshHealth = refreshHealth;
    window.controlSht2bm = controlSht2bm;
    window.testTelegramBot = testTelegramBot;
})();
