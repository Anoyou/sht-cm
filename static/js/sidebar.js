// 侧边栏管理模块

(function() {
    let sidebarToggleTimeout = null;

    function toggleSidebar() {
        // 防抖处理，避免快速点击导致的问题
        if (sidebarToggleTimeout) {
            clearTimeout(sidebarToggleTimeout);
        }

        sidebarToggleTimeout = setTimeout(() => {
            const sidebar = document.getElementById('sidebar');
            if (!sidebar) return;

            const isMobile = window.innerWidth <= 1024;

            if (isMobile) {
                const isExpanded = sidebar.getAttribute('data-state') === 'expanded';

                // 添加动画类
                sidebar.classList.add('transitioning');

                sidebar.setAttribute('data-state', isExpanded ? 'collapsed' : 'expanded');

                if (!isExpanded) {
                    // 移动端展开时，暂时禁止激活状态更新
                    window.sidebarActivationAllowed = false;
                    createSidebarOverlay();
                } else {
                    removeSidebarOverlay();
                }

                // 动画完成后移除过渡类，时间与CSS保持一致
                setTimeout(() => {
                    sidebar.classList.remove('transitioning');
                    // 移动端展开后重新允许激活状态更新
                    if (!isExpanded) {
                        window.sidebarActivationAllowed = true;
                    }
                }, 250); // 与CSS动画时间保持一致

            } else {
                const isCollapsed = sidebar.getAttribute('data-state') === 'collapsed';

                // 添加动画类
                sidebar.classList.add('transitioning');

                sidebar.setAttribute('data-state', isCollapsed ? 'expanded' : 'collapsed');

                // 桌面端状态改变时重新设置激活状态，时间与CSS保持一致
                setTimeout(() => {
                    setActiveSidebar();
                    sidebar.classList.remove('transitioning');
                }, 250); // 与CSS动画时间保持一致

                try {
                    localStorage.setItem('sidebar_collapsed', isCollapsed ? '0' : '1');
                } catch { }
            }

            sidebarToggleTimeout = null;
        }, 50); // 50ms防抖延迟
    }

    function createSidebarOverlay() {
        let overlay = document.getElementById('sidebar-overlay');
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.id = 'sidebar-overlay';
            overlay.className = 'sidebar-overlay';

            overlay.addEventListener('click', () => {
                const sidebar = document.getElementById('sidebar');
                sidebar.setAttribute('data-state', 'collapsed');
                removeSidebarOverlay();
            });

            document.body.appendChild(overlay);

            setTimeout(() => {
                overlay.classList.add('show');
            }, 10);
        }
    }

    function removeSidebarOverlay() {
        const overlay = document.getElementById('sidebar-overlay');
        if (overlay) {
            overlay.classList.remove('show');
            setTimeout(() => {
                if (overlay.parentNode) {
                    overlay.parentNode.removeChild(overlay);
                }
            }, 300);
        }
    }

    function applyResponsiveSidebar() {
        const sidebar = document.getElementById('sidebar');
        if (!sidebar) return;

        const isMobile = window.innerWidth <= 1024;

        // 移动端总是收起
        if (isMobile) {
            sidebar.setAttribute('data-state', 'collapsed');
            return;
        }

        // 桌面端：只在窗口尺寸变化导致需要强制时才改变
        // 如果已经有手动设置的状态，保持不变
        const currentState = sidebar.getAttribute('data-state');
        const saved = localStorage.getItem('sidebar_collapsed');

        // 如果有保存的偏好，且当前状态与偏好不符，则更新
        if (saved !== null) {
            const desiredState = saved === '1' ? 'collapsed' : 'expanded';
            if (currentState !== desiredState) {
                sidebar.setAttribute('data-state', desiredState);
            }
        }
    }

    function initSidebar() {
        try {
            const sidebar = document.getElementById('sidebar');
            if (!sidebar) return;

            console.log('[Sidebar] initSidebar() 被调用');

            // 优先检查已保存的偏好
            const saved = localStorage.getItem('sidebar_collapsed');

            // 如果已保存过，使用保存的状态
            if (saved !== null) {
                sidebar.setAttribute('data-state', saved === '1' ? 'collapsed' : 'expanded');
            } else {
                // 第一次访问，默认展开，但不要标记为"已保存"
                sidebar.setAttribute('data-state', 'expanded');
            }
        } catch { }
    }

    function setActiveSidebar() {
        const path = location.pathname || '/';
        const isMobile = window.innerWidth <= 1024;

        console.log(`[Sidebar] 开始设置选中状态 - 当前路径: ${path}, 桌面端: ${!isMobile}`);

        document.querySelectorAll('.sidebar-menu-item a').forEach(a => {
            const href = a.getAttribute('href') || '';

            // 更精确的路径匹配逻辑
            let isActive = false;

            if (path === '/' && href === '/') {
                // 只有在真正访问首页时才激活首页
                isActive = true;
            } else if (path !== '/' && href !== '/') {
                // 非首页的精确匹配
                if (path === href) {
                    isActive = true;
                }
                // 处理子路径匹配（如 /categories 匹配 /categories/xxx）
                else if (href.length > 1 && path.startsWith(href + '/')) {
                    isActive = true;
                }
            }

            a.setAttribute('data-active', isActive ? 'true' : 'false');

            // 调试日志 - 显示所有菜单项的状态
            console.log(`  菜单项: ${href}, 选中: ${isActive}, 匹配: path=${path} vs href=${href}`);

            // 额外的调试信息
            if (isActive) {
                console.log(`✅ [Sidebar] 激活菜单项: ${href} (当前路径: ${path})`);
                console.log(`  设置属性: data-active="true"`);
            }
        });

        console.log(`[Sidebar] 设置完成，找到的选中元素:`, document.querySelector('[data-active="true"]'));
    }

    // PWA模式下的特殊处理
    function handlePWAMode() {
        const isPWA = window.matchMedia('(display-mode: standalone)').matches;
        if (isPWA) {
            document.documentElement.classList.add('pwa-mode');

            // PWA模式下延迟设置侧边栏状态，确保路径正确识别
            setTimeout(() => {
                setActiveSidebar();
            }, 200);
        }
    }

    function bindSidebarAutoCollapse() {
        document.querySelectorAll('.sidebar-menu-item a').forEach(a => {
            a.addEventListener('click', (e) => {
                const isMobile = window.innerWidth <= 1024;
                if (isMobile) {
                    // 添加小延迟，确保点击事件完成后再收起侧边栏
                    setTimeout(() => {
                        const sidebar = document.getElementById('sidebar');
                        if (sidebar) {
                            sidebar.setAttribute('data-state', 'collapsed');
                            removeSidebarOverlay();
                        }
                    }, 100);
                }
            });
        });
    }

    // 导出函数供其他模块使用
    window.sidebarManager = {
        toggleSidebar,
        createSidebarOverlay,
        removeSidebarOverlay,
        applyResponsiveSidebar,
        initSidebar,
        setActiveSidebar,
        handlePWAMode,
        bindSidebarAutoCollapse
    };

    // 导出到全局（为了兼容 HTML 中的 onclick 属性）
    window.toggleSidebar = toggleSidebar;

    function initializeSidebarModule() {
        // 延迟初始化，确保DOM完全加载
        setTimeout(() => {
            initSidebar();
            applyResponsiveSidebar();
            setActiveSidebar();

            requestAnimationFrame(() => {
                requestAnimationFrame(() => {
                    document.documentElement.classList.remove('sidebar-no-transition');
                    document.documentElement.classList.remove('sidebar-pref-collapsed');
                });
            });
        }, 50);

        // 监听窗口大小变化
        window.addEventListener('resize', () => {
            // 防抖处理
            clearTimeout(window.sidebarResizeTimeout);
            window.sidebarResizeTimeout = setTimeout(() => {
                applyResponsiveSidebar();
            }, 150);
        });

        bindSidebarAutoCollapse();

        // PWA模式处理
        handlePWAMode();
    }

    // 页面加载时初始化（兼容模块延迟加载）
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initializeSidebarModule);
    } else {
        initializeSidebarModule();
    }

    // 监听页面导航变化（用于单页应用或动态内容加载）
    let lastPath = location.pathname;
    let isCheckingPathChange = false;
    let lastCheckTime = 0;

    function checkPathChange() {
        const now = Date.now();
        // 防止频繁检查（至少间隔200ms）
        if (now - lastCheckTime < 200) {
            return;
        }

        // 防止重复检查
        if (isCheckingPathChange) {
            return;
        }

        if (location.pathname !== lastPath) {
            isCheckingPathChange = true;
            lastPath = location.pathname;
            lastCheckTime = now;

            console.log(`[Sidebar] 路径变化: 检测到导航，新路径: ${location.pathname}`);

            // 延迟设置，确保新页面DOM加载完成
            setTimeout(() => {
                setActiveSidebar();
                isCheckingPathChange = false;
            }, 100);
        }
    }
})();
