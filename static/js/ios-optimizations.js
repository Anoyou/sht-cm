// iOS/iPhone 专用检测和优化模块

(function () {
    function initiPhoneOptimizations() {
        // 检测iPhone设备
        const isIPhone = /iPhone/.test(navigator.userAgent);
        const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) && !window.MSStream;
        const isSafari = /Safari/.test(navigator.userAgent) && !/Chrome/.test(navigator.userAgent);

        if (isIPhone) {
            // 添加iPhone专用类名
            document.documentElement.classList.add('iphone');

            // 检测具体iPhone型号并添加对应类名
            const screenWidth = screen.width;
            const screenHeight = screen.height;
            const pixelRatio = window.devicePixelRatio;

            // iPhone型号检测
            if (screenWidth === 375 && screenHeight === 667 && pixelRatio === 2) {
                document.documentElement.classList.add('iphone-6-7-8');
            } else if (screenWidth === 414 && screenHeight === 736 && pixelRatio === 3) {
                document.documentElement.classList.add('iphone-6-7-8-plus');
            } else if (screenWidth === 375 && screenHeight === 812 && pixelRatio === 3) {
                document.documentElement.classList.add('iphone-x-xs-11-pro');
            } else if (screenWidth === 414 && screenHeight === 896) {
                document.documentElement.classList.add('iphone-xr-11-xs-max-11-pro-max');
            } else if (screenWidth === 390 && screenHeight === 844 && pixelRatio === 3) {
                document.documentElement.classList.add('iphone-12-13-14');
            } else if (screenWidth === 428 && screenHeight === 926 && pixelRatio === 3) {
                document.documentElement.classList.add('iphone-12-13-14-pro-max');
            } else if (screenWidth === 393 && screenHeight === 852 && pixelRatio === 3) {
                document.documentElement.classList.add('iphone-14-pro');
            } else if (screenWidth === 430 && screenHeight === 932 && pixelRatio === 3) {
                document.documentElement.classList.add('iphone-14-pro-max');
            }

            // 强制应用移动端样式
            document.documentElement.classList.add('force-mobile-layout');

            // 针对小屏幕iPhone的额外优化
            if (screenWidth <= 375) {
                document.documentElement.classList.add('iphone-small-screen');
            }

            // iPhone专用的触摸优化
            document.addEventListener('touchstart', function () { }, { passive: true });
            document.addEventListener('touchmove', function () { }, { passive: true });

            // 防止双击缩放
            let lastTouchEnd = 0;
            document.addEventListener('touchend', function (event) {
                const now = (new Date()).getTime();
                if (now - lastTouchEnd <= 300) {
                    event.preventDefault();
                }
                lastTouchEnd = now;
            }, false);
        }

        if (isIOS) {
            // 添加iOS类名用于特殊样式
            document.documentElement.classList.add('ios');

            if (isSafari) {
                document.documentElement.classList.add('ios-safari');

                // 检测Safari底部标签栏
                const hasBottomTabs = window.innerHeight < screen.height - 100;
                if (hasBottomTabs) {
                    document.documentElement.classList.add('ios-safari-tabs');
                }
            }

            // 监听方向变化
            window.addEventListener('orientationchange', function () {
                // 延迟处理，等待方向变化完成
                setTimeout(function () {
                    // 强制重新计算安全区域
                    document.documentElement.style.setProperty('--viewport-height', window.innerHeight + 'px');

                    // 重新检测底部标签栏
                    if (isSafari) {
                        const hasBottomTabs = window.innerHeight < screen.height - 100;
                        document.documentElement.classList.toggle('ios-safari-tabs', hasBottomTabs);
                    }

                    // 触发重绘以确保安全区域正确应用
                    document.body.style.display = 'none';
                    document.body.offsetHeight; // 触发重排
                    document.body.style.display = '';
                }, 200);
            });

            // 初始设置视口高度
            document.documentElement.style.setProperty('--viewport-height', window.innerHeight + 'px');

            // 处理iOS Safari地址栏隐藏/显示
            let lastHeight = window.innerHeight;
            let resizeTimer;

            window.addEventListener('resize', function () {
                clearTimeout(resizeTimer);
                resizeTimer = setTimeout(() => {
                    const currentHeight = window.innerHeight;
                    if (Math.abs(currentHeight - lastHeight) > 50) {
                        document.documentElement.style.setProperty('--viewport-height', currentHeight + 'px');

                        // 重新检测底部标签栏
                        if (isSafari) {
                            const hasBottomTabs = currentHeight < screen.height - 100;
                            document.documentElement.classList.toggle('ios-safari-tabs', hasBottomTabs);
                        }

                        lastHeight = currentHeight;

                        // Web模式下的额外处理
                        if (!window.matchMedia('(display-mode: standalone)').matches) {
                            // 确保内容区域正确适配
                            const content = document.getElementById('content');
                            if (content) {
                                content.style.minHeight = currentHeight + 'px';
                            }
                        }
                    }
                }, 150);
            });
        }
    }

    // Safari强制重绘函数 - 【Iframe唤醒版 + 样式重算】
    // 原理：插入 iframe 会强制浏览器创建新的渲染层，销毁时会强制重绘父级上下文
    function forceSafariRepaint() {
        const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) && !window.MSStream;
        if (!isIOS) return;

        // 1. 强制样式重算
        // 读取一个计算样式属性会强制浏览器重新计算当前样式树
        const computedStyle = window.getComputedStyle(document.body);
        const dummy = computedStyle.backgroundColor; // 触发读取

        // 2. 创建一个隐藏的 iframe
        const iframe = document.createElement('iframe');

        // 设置样式：不仅隐藏，而且要脱离文档流，避免页面抖动
        iframe.style.cssText = `
            position: absolute;
            top: 0;
            left: 0;
            width: 1px;
            height: 1px;
            visibility: hidden;
            pointer-events: none;
            z-index: -1000;
            opacity: 0;
            border: none;
        `;

        // 必须加到 body 中，加到 head 中无效
        document.body.appendChild(iframe);

        // 短暂延迟后立即移除，这个"插入-销毁"的过程就是触发点
        setTimeout(() => {
            if (iframe.parentNode) {
                document.body.removeChild(iframe);
            }
        }, 10); // 10ms 足以让浏览器感知到 DOM 变化
    }

    // 导出函数供其他模块使用
    window.iosOptimizations = {
        initiPhoneOptimizations,
        forceSafariRepaint
    };

    // 页面加载时初始化
    document.addEventListener('DOMContentLoaded', initiPhoneOptimizations);
})();
