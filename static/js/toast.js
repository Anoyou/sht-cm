// Toast 通知系统模块 (懒加载)

(function () {
    // Toast通知系统
    function showToast(message, type = 'info', duration = 3000) {
        const container = document.getElementById('toastContainer') || (() => {
            const div = document.createElement('div');
            div.id = 'toastContainer';
            div.className = 'toast-container';
            document.body.appendChild(div);
            return div;
        })();

        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;

        container.appendChild(toast);

        // 自动移除Toast
        const removeToast = () => {
            if (toast.parentNode) {
                toast.classList.add('removing');
                setTimeout(() => {
                    if (toast.parentNode) {
                        toast.parentNode.removeChild(toast);
                    }
                }, 300);
            }
        };

        setTimeout(removeToast, duration);

        // 点击移除Toast
        toast.addEventListener('click', removeToast);
    }

    // 显示确认Toast（用于日志页面）
    function showConfirmToast(message, onConfirm, onCancel) {
        const toast = document.createElement('div');
        toast.className = 'toast-confirm';

        toast.innerHTML = `
            <div class="toast-content">
                <div class="toast-message">${message}</div>
                <div class="toast-actions">
                    <button class="btn btn-sm btn-secondary" onclick="handleCancel()">取消</button>
                    <button class="btn btn-sm btn-destructive" onclick="handleConfirm()">确认</button>
                </div>
            </div>
        `;

        // 绑定事件处理函数到全局
        window.handleConfirm = () => {
            document.body.removeChild(toast);
            if (onConfirm) onConfirm();
            // 清理全局函数
            delete window.handleConfirm;
            delete window.handleCancel;
        };

        window.handleCancel = () => {
            document.body.removeChild(toast);
            if (onCancel) onCancel();
            // 清理全局函数
            delete window.handleConfirm;
            delete window.handleCancel;
        };

        document.body.appendChild(toast);

        // 5秒后自动取消
        setTimeout(() => {
            if (document.body.contains(toast)) {
                window.handleCancel();
            }
        }, 5000);
    }

    // 显示带多个选项的 Toast
    function showChoiceToast(message, choices) {
        return new Promise((resolve) => {
            const toast = document.createElement('div');
            toast.className = 'toast-confirm';
            toast.style.zIndex = '9999';

            let buttonsHtml = choices.map((choice, index) =>
                `<button class="btn btn-sm ${choice.color || 'btn-secondary'}" onclick="handleChoice(${index})">${choice.text}</button>`
            ).join('');

            toast.innerHTML = `
                <div class="toast-content">
                    <div class="toast-message">${message}</div>
                    <div class="toast-actions" style="flex-wrap: wrap; gap: 5px; justify-content: center;">
                        ${buttonsHtml}
                        <button class="btn btn-sm btn-outline" onclick="handleChoice(-1)">取消</button>
                    </div>
                </div>
            `;

            window.handleChoice = (index) => {
                document.body.removeChild(toast);
                delete window.handleChoice;
                if (index === -1) {
                    resolve(null);
                } else {
                    resolve(choices[index].value);
                }
            };

            document.body.appendChild(toast);
        });
    }

    // 导出函数供其他模块使用
    window.toastManager = {
        showToast,
        showConfirmToast,
        showChoiceToast
    };

    // 全局绑定（为了兼容 HTML 中的 onclick 属性）
    window.showToast = showToast;
    window.showConfirmToast = showConfirmToast;
    window.showChoiceToast = showChoiceToast;
})();
