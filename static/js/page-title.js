// 页面标题滚动监听模块

(function() {
    // 页面标题滚动监听 - 当滚动超过 page-title 时，在顶栏显示标题
    function initPageTitleScroll() {
        const pageTitle = document.querySelector('.page-title');
        const headerTitle = document.getElementById('headerTitle');
        const headerTitleText = document.getElementById('headerTitleText');

        if (!pageTitle || !headerTitle || !headerTitleText) {
            console.log('页面标题元素未找到');
            return;
        }

        console.log('初始化页面标题滚动监听');

        // 等待所有样式加载完成后再初始化
        function delayedInit() {
            // 获取 header 的实际高度
            const header = document.querySelector('.header');
            if (!header) return;

            const headerHeight = header.offsetHeight;
            console.log('Header 高度:', headerHeight);

            function updateHeaderTitle() {
                const pageTitleRect = pageTitle.getBoundingClientRect();

                // 如果 page-title 的顶部低于 header 的底部（即被 header 遮挡），显示标题
                if (pageTitleRect.top < headerHeight && pageTitleRect.top > 0) {
                    headerTitle.style.display = 'flex';
                    headerTitleText.textContent = pageTitle.textContent.trim();
                    console.log('显示标题:', pageTitle.textContent.trim());
                } else if (pageTitleRect.top <= 0) {
                    // 完全滚动过去，保持显示
                    headerTitle.style.display = 'flex';
                    headerTitleText.textContent = pageTitle.textContent.trim();
                    console.log('保持显示标题:', pageTitle.textContent.trim());
                } else {
                    // page-title 可见，隐藏顶栏中的标题
                    headerTitle.style.display = 'none';
                    console.log('隐藏标题');
                }
            }

            // 初始检查
            setTimeout(updateHeaderTitle, 100);

            // 监听滚动事件（使用 requestAnimationFrame 优化性能）
            let ticking = false;
            window.addEventListener('scroll', () => {
                if (!ticking) {
                    window.requestAnimationFrame(() => {
                        updateHeaderTitle();
                        ticking = false;
                    });
                    ticking = true;
                }
            }, { passive: true });

            // 监听窗口大小变化
            window.addEventListener('resize', () => {
                if (!ticking) {
                    window.requestAnimationFrame(() => {
                        updateHeaderTitle();
                        ticking = false;
                    });
                    ticking = true;
                }
            });
        }

        // 延迟执行，确保 DOM 和样式都加载完成
        setTimeout(delayedInit, 200);
    }

    // 导出函数供其他模块使用
    window.pageTitleManager = {
        initPageTitleScroll
    };

    // 页面加载时初始化
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initPageTitleScroll);
    } else {
        initPageTitleScroll();
    }
})();
