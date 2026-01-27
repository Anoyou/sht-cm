// 主题管理模块 - 支持 Light/Dark/Auto 三种模式 + 下拉菜单 UI

(function () {
    // 状态定义
    const THEME_AUTO = 'auto';
    const THEME_LIGHT = 'light';
    const THEME_DARK = 'dark';

    // 获取当前存储的主题偏好
    function getStoredTheme() {
        return localStorage.getItem('theme') || THEME_AUTO;
    }

    // 获取当前生效的主题（深色还是浅色）
    function getEffectiveTheme() {
        const stored = getStoredTheme();
        if (stored === THEME_AUTO) {
            return window.matchMedia('(prefers-color-scheme: dark)').matches ? THEME_DARK : THEME_LIGHT;
        }
        return stored;
    }

    // 打开/关闭主题菜单
    function toggleThemeMenu(event) {
        if (event) event.stopPropagation();
        const menu = document.getElementById('themeMenu');
        if (menu) {
            const isActive = menu.classList.contains('active');
            // 关闭其他所有可能打开的菜单
            document.querySelectorAll('.theme-menu.active').forEach(m => m.classList.remove('active'));

            if (!isActive) {
                menu.classList.add('active');
            }
        }
    }

    // 关闭主题菜单
    function closeThemeMenu() {
        const menu = document.getElementById('themeMenu');
        if (menu) menu.classList.remove('active');
    }

    // 更新菜单项的激活状态
    function updateMenuState(currentMode) {
        document.querySelectorAll('.theme-menu-item').forEach(item => {
            if (item.getAttribute('data-theme') === currentMode) {
                item.classList.add('active');
            } else {
                item.classList.remove('active');
            }
        });
    }

    // 更新 trigger 按钮的图标
    function updateTriggerIcon(mode) {
        const sun = document.getElementById('iconSun');
        const moon = document.getElementById('iconMoon');
        const system = document.getElementById('iconSystem');

        if (!sun || !moon || !system) return;

        sun.style.display = 'none';
        moon.style.display = 'none';
        system.style.display = 'none';

        if (mode === THEME_AUTO) {
            system.style.display = 'inline';
        } else if (mode === THEME_DARK) {
            moon.style.display = 'inline';
        } else {
            sun.style.display = 'inline';
        }
    }

    // 设置特定主题
    function setTheme(mode) {
        localStorage.setItem('theme', mode);
        applyTheme(mode);
        closeThemeMenu();

        // 显示提示
        if (window.showToast) {
            const labels = {
                [THEME_AUTO]: '跟随系统',
                [THEME_LIGHT]: '浅色模式',
                [THEME_DARK]: '深色模式'
            };
            window.showToast(`已切换至: ${labels[mode]}`);
        }
    }

    // 应用主题样式 (核心逻辑)
    function applyTheme(mode) {
        const html = document.documentElement;

        html.classList.remove('dark', 'light');

        if (mode === THEME_AUTO) {
            // 自动模式
        } else if (mode === THEME_DARK) {
            html.classList.add('dark');
        } else {
            html.classList.add('light');
        }

        updateTriggerIcon(mode);
        updateMenuState(mode);
        updateSafeAreaColors();

        // 关键修复：强制触发重绘，模拟"打开侧边栏"的效果
        forceLayoutUpdate();
    }

    // 强力重绘函数 - 解决 iOS 状态栏卡顿 (核弹级终极重绘)
    function forceLayoutUpdate() {
        const html = document.documentElement;

        // 策略A: 切换 Root 元素的内边距
        const oldPad = html.style.paddingBottom;
        html.style.paddingBottom = '1px';

        // 策略B: 模拟侧边栏遮罩层的 DOM 操作
        const dummyOverlay = document.createElement('div');
        dummyOverlay.style.position = 'fixed';
        dummyOverlay.style.top = '0';
        dummyOverlay.style.left = '0';
        dummyOverlay.style.width = '100vw';
        dummyOverlay.style.height = '100vh';
        dummyOverlay.style.zIndex = '9999';
        dummyOverlay.style.backgroundColor = 'rgba(0,0,0,0.01)';
        dummyOverlay.style.pointerEvents = 'none';
        dummyOverlay.style.transform = 'translateZ(0)';
        document.body.appendChild(dummyOverlay);

        // 策略C: PWA 状态栏遮罩层显隐
        const statusBarOverlay = document.getElementById('statusBarOverlay');
        if (statusBarOverlay) statusBarOverlay.style.display = 'none';

        void html.offsetHeight;
        void dummyOverlay.offsetHeight;

        requestAnimationFrame(() => {
            html.style.paddingBottom = oldPad;

            if (document.body.contains(dummyOverlay)) {
                setTimeout(() => { if (dummyOverlay.parentNode) document.body.removeChild(dummyOverlay); }, 50);
            }

            if (statusBarOverlay) statusBarOverlay.style.display = 'block';

            const scrollY = window.scrollY;
            if (document.documentElement.scrollHeight > window.innerHeight) {
                window.scrollTo(0, scrollY + 1);
                setTimeout(() => { window.scrollTo(0, scrollY); }, 16);
            } else {
                window.dispatchEvent(new Event('resize'));
            }

            if (window.iosOptimizations && typeof window.iosOptimizations.forceSafariRepaint === 'function') {
                window.iosOptimizations.forceSafariRepaint();
            }
        });
    }

    function updateSafeAreaColors() {
        const effectiveTheme = getEffectiveTheme();
        const isDark = effectiveTheme === THEME_DARK;

        const darkColor = '#000000';
        const lightColor = '#f2f2f7';
        const targetColor = isDark ? darkColor : lightColor;

        const html = document.documentElement;
        const body = document.body;

        // CSS 变量
        html.style.setProperty('--safe-area-bg', targetColor);
        body.style.setProperty('--safe-area-bg', targetColor);

        // 覆盖层 - 不再手动设置背景色，由 CSS 的玻璃效果接管
        const safeAreaBottomOverlay = document.getElementById('safeAreaBottomOverlay');
        if (safeAreaBottomOverlay) safeAreaBottomOverlay.style.backgroundColor = targetColor;

        // Meta Tags Management
        const existingMetas = document.querySelectorAll('meta[name="theme-color"]');
        existingMetas.forEach(meta => meta.remove());

        const mode = getStoredTheme();

        // 针对 Safari Web 的优化：
        // 虽然无法实现 PWA 的 'black-translucent' (全透)，
        // 但我们可以将 theme-color 设置为 Header 的底色 (而不是 Body 底色)，
        // 这样状态栏就会与顶栏融为一体，消除"断层感"。

        // 浅色模式 Header Glass 底色: rgba(255, 255, 255, 0.45) 叠加在 #F2F2F7 上 -> 接近纯白
        const headerColorLight = '#ffffff';

        // 深色模式 Header Glass 底色: rgba(30, 30, 30, 0.6) 叠加在 #000000 上 -> 接近 #1e1e1e
        const headerColorDark = '#1e1e1e';

        if (mode === THEME_AUTO) {
            // 自动模式：双标签
            const metaLight = document.createElement('meta');
            metaLight.name = 'theme-color';
            metaLight.content = headerColorLight;
            metaLight.media = '(prefers-color-scheme: light)';
            document.head.appendChild(metaLight);

            const metaDark = document.createElement('meta');
            metaDark.name = 'theme-color';
            metaDark.content = headerColorDark;
            metaDark.media = '(prefers-color-scheme: dark)';
            document.head.appendChild(metaDark);
        } else {
            // 手动模式：单标签
            const meta = document.createElement('meta');
            meta.name = 'theme-color';
            meta.content = isDark ? headerColorDark : headerColorLight;
            document.head.appendChild(meta);
        }
    }

    // 切换主题 (旧接口兼容，现在默认打开菜单，或者如果需要也可循环)
    // 根据用户要求，现在是"抽出抽屉那般"，所以旧的 toggleTheme 应该打开菜单
    function toggleTheme() {
        toggleThemeMenu();
    }

    // 初始化
    function initTheme() {
        const mode = getStoredTheme();
        applyTheme(mode);

        // 监听系统主题变化
        window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
            if (getStoredTheme() === THEME_AUTO) {
                updateSafeAreaColors();
                forceLayoutUpdate();
            }
        });

        // 点击外部关闭菜单
        document.addEventListener('click', (e) => {
            const menuWrapper = document.querySelector('.theme-dropdown-wrapper');
            if (menuWrapper && !menuWrapper.contains(e.target)) {
                closeThemeMenu();
            }
        });
    }

    // 导出
    window.themeManager = {
        toggleTheme, // 兼容旧接口
        toggleThemeMenu,
        setTheme,
        initTheme
    };

    // 全局暴露
    window.toggleTheme = toggleTheme; // 绑定到按钮点击
    window.toggleThemeMenu = toggleThemeMenu;
    window.setTheme = setTheme;

    // 启动
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initTheme);
    } else {
        initTheme();
    }

    // 尽早执行
    try { initTheme(); } catch (e) { }

})();
