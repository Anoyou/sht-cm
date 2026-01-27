# PWA图标文件说明

## 文件列表

### 应用图标
- `icon-72x72.png` - 72x72像素应用图标
- `icon-96x96.png` - 96x96像素应用图标  
- `icon-128x128.png` - 128x128像素应用图标
- `icon-144x144.png` - 144x144像素应用图标
- `icon-152x152.png` - 152x152像素应用图标
- `icon-192x192.png` - 192x192像素应用图标
- `icon-384x384.png` - 384x384像素应用图标
- `icon-512x512.png` - 512x512像素应用图标

### Apple设备图标
- `apple-touch-icon.png` - Apple Touch图标 (180x180)

### Apple启动画面
- `apple-splash-2048-2732.png` - iPad Pro 12.9" 启动画面
- `apple-splash-1668-2224.png` - iPad Pro 11" 启动画面
- `apple-splash-1536-2048.png` - iPad 启动画面
- `apple-splash-1125-2436.png` - iPhone X/11 Pro 启动画面
- `apple-splash-1242-2208.png` - iPhone Plus 启动画面
- `apple-splash-750-1334.png` - iPhone 6/7/8 启动画面
- `apple-splash-640-1136.png` - iPhone 5 启动画面

### 快捷方式图标
- `shortcut-crawler.png` - 爬虫管理快捷方式图标
- `shortcut-resources.png` - 资源浏览快捷方式图标
- `shortcut-config.png` - 系统配置快捷方式图标
- `shortcut-services.png` - 服务监控快捷方式图标

## 生成工具

### 当前状态
目前所有图标都是占位符文件（1x1透明PNG）。

### 生成真实图标的方法

#### 方法1: 使用浏览器生成
1. 在浏览器中打开 `generate_icons.html`
2. 右键点击各个尺寸的图标
3. 选择"图片另存为"
4. 保存为对应的文件名

#### 方法2: 使用Python脚本（需要PIL库）
```bash
pip install Pillow
python3 create_icons.py
```

#### 方法3: 使用设计工具
- 使用 `icon.svg` 作为基础
- 导出为不同尺寸的PNG文件
- 确保图标在小尺寸下仍然清晰可见

## 设计规范

### 颜色方案
- 主色：#3b82f6 (蓝色)
- 渐变：#3b82f6 → #1d4ed8
- 文字：白色
- 副标题：rgba(255, 255, 255, 0.8)

### 设计元素
- 主标题：SHT
- 副标题：资源管理
- 装饰性圆圈元素
- 圆角矩形背景

### 尺寸要求
- 所有图标必须是正方形
- 支持透明背景
- 在小尺寸下保持可读性
- 遵循各平台的设计规范

## 注意事项

1. **文件大小**: 尽量控制图标文件大小，避免影响PWA加载速度
2. **兼容性**: 确保图标在各种设备和浏览器中正常显示
3. **一致性**: 所有图标应保持统一的设计风格
4. **可访问性**: 确保图标在高对比度模式下仍然可见

## 更新图标

如果需要更新图标：
1. 替换对应尺寸的PNG文件
2. 确保文件名保持不变
3. 清除浏览器缓存测试新图标
4. 更新Service Worker版本号以强制更新缓存