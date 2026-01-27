#!/usr/bin/env python3
"""
PWA 图标完整生成脚本
生成所有应用图标、Apple 启动页和快捷方式图标

使用方法:
    pip install Pillow
    python generate_all_icons.py

自定义:
    修改下方的 DESIGN_CONFIG 配置
"""

import os
import sys

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    print("请先安装 Pillow: pip install Pillow")
    sys.exit(1)

# ==================== 设计配置 ====================

DESIGN_CONFIG = {
    # 颜色配置
    'primary_color': (59, 130, 246),      # #3b82f6 蓝色
    'secondary_color': (29, 78, 216),     # #1d4ed8 深蓝
    'text_color': (255, 255, 255),        # 白色
    'subtitle_color': (255, 255, 255, 200),  # 半透明白色

    # 文字配置
    'main_text': 'SHT',
    'subtitle': '资源管理',

    # 圆角比例（相对于图标尺寸）
    'corner_radius_ratio': 0.15,
}

# 图标尺寸配置
ICON_SIZES = [72, 96, 128, 144, 152, 192, 384, 512]

# Apple 启动页配置
SPLASH_SCREENS = [
    ('apple-splash-2048-2732.png', 2048, 2732),   # iPad Pro 12.9"
    ('apple-splash-1668-2224.png', 1668, 2224),   # iPad Pro 11"
    ('apple-splash-1536-2048.png', 1536, 2048),   # iPad
    ('apple-splash-1125-2436.png', 1125, 2436),   # iPhone X/11/12
    ('apple-splash-1242-2208.png', 1242, 2208),   # iPhone Plus
    ('apple-splash-750-1334.png', 750, 1334),     # iPhone 6/7/8
    ('apple-splash-640-1136.png', 640, 1136),     # iPhone SE
]

# 快捷方式图标配置
SHORTCUT_ICONS = [
    ('shortcut-crawler.png', '爬'),
    ('shortcut-resources.png', '源'),
    ('shortcut-config.png', '配'),
    ('shortcut-services.png', '服'),
]


def create_gradient_background(size, color1, color2, corner_radius=0):
    """创建渐变背景"""
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # 创建渐变
    for y in range(size):
        ratio = y / size
        r = int(color1[0] * (1 - ratio) + color2[0] * ratio)
        g = int(color1[1] * (1 - ratio) + color2[1] * ratio)
        b = int(color1[2] * (1 - ratio) + color2[2] * ratio)
        draw.line([(0, y), (size, y)], fill=(r, g, b, 255))

    # 创建圆角蒙版
    if corner_radius > 0:
        mask = Image.new('L', (size, size), 0)
        mask_draw = ImageDraw.Draw(mask)
        mask_draw.rounded_rectangle(
            [(0, 0), (size, size)],
            radius=corner_radius,
            fill=255
        )
        img.putalpha(mask)

    return img


def get_font(size, bold=False):
    """获取字体"""
    font_paths = [
        # macOS
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        # Linux
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        # Windows
        "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
    ]

    for path in font_paths:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except:
                continue

    return ImageFont.load_default()


def create_app_icon(size):
    """创建应用图标"""
    config = DESIGN_CONFIG
    corner_radius = int(size * config['corner_radius_ratio'])

    # 创建渐变背景
    img = create_gradient_background(
        size,
        config['primary_color'],
        config['secondary_color'],
        corner_radius
    )
    draw = ImageDraw.Draw(img)

    # 绘制主文字
    main_font_size = int(size * 0.35)
    main_font = get_font(main_font_size, bold=True)

    text = config['main_text']
    bbox = draw.textbbox((0, 0), text, font=main_font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]

    x = (size - text_width) // 2
    y = int(size * 0.25)

    draw.text((x, y), text, fill=config['text_color'], font=main_font)

    # 绘制副标题
    sub_font_size = int(size * 0.12)
    sub_font = get_font(sub_font_size)

    subtitle = config['subtitle']
    bbox = draw.textbbox((0, 0), subtitle, font=sub_font)
    text_width = bbox[2] - bbox[0]

    x = (size - text_width) // 2
    y = int(size * 0.6)

    draw.text((x, y), subtitle, fill=config['subtitle_color'], font=sub_font)

    # 绘制装饰圆圈
    center_y = int(size * 0.78)
    for radius, alpha in [(int(size*0.1), 50), (int(size*0.07), 80), (int(size*0.04), 150)]:
        circle_color = (255, 255, 255, alpha)
        draw.ellipse(
            [(size//2 - radius, center_y - radius),
             (size//2 + radius, center_y + radius)],
            outline=circle_color,
            width=max(1, size // 100)
        )

    return img


def create_splash_screen(width, height):
    """创建启动页"""
    config = DESIGN_CONFIG

    # 创建纯色背景
    img = Image.new('RGBA', (width, height), config['primary_color'] + (255,))
    draw = ImageDraw.Draw(img)

    # 在中央放置图标
    icon_size = min(width, height) // 4
    icon = create_app_icon(icon_size)

    x = (width - icon_size) // 2
    y = (height - icon_size) // 2 - height // 10

    img.paste(icon, (x, y), icon)

    # 添加应用名称
    font_size = min(width, height) // 15
    font = get_font(font_size)

    app_name = "SHT资源管理"
    bbox = draw.textbbox((0, 0), app_name, font=font)
    text_width = bbox[2] - bbox[0]

    x = (width - text_width) // 2
    y = (height + icon_size) // 2

    draw.text((x, y), app_name, fill=config['text_color'], font=font)

    return img


def create_shortcut_icon(size, char):
    """创建快捷方式图标"""
    config = DESIGN_CONFIG
    corner_radius = int(size * config['corner_radius_ratio'])

    # 创建渐变背景
    img = create_gradient_background(
        size,
        config['primary_color'],
        config['secondary_color'],
        corner_radius
    )
    draw = ImageDraw.Draw(img)

    # 绘制单个字符
    font_size = int(size * 0.5)
    font = get_font(font_size, bold=True)

    bbox = draw.textbbox((0, 0), char, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]

    x = (size - text_width) // 2
    y = (size - text_height) // 2 - size // 10

    draw.text((x, y), char, fill=config['text_color'], font=font)

    return img


def main():
    print("=" * 50)
    print("PWA 图标生成工具")
    print("=" * 50)
    print()

    # 获取脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # 1. 生成应用图标
    print("📱 生成应用图标...")
    for size in ICON_SIZES:
        icon = create_app_icon(size)
        filename = f"icon-{size}x{size}.png"
        icon.save(filename, 'PNG')
        print(f"   ✓ {filename}")

    # 2. 生成 Apple Touch 图标
    print("\n🍎 生成 Apple Touch 图标...")
    apple_icon = create_app_icon(180)
    apple_icon.save("apple-touch-icon.png", 'PNG')
    print("   ✓ apple-touch-icon.png")

    # 3. 生成启动页
    print("\n🚀 生成启动页...")
    for filename, width, height in SPLASH_SCREENS:
        splash = create_splash_screen(width, height)
        splash.save(filename, 'PNG')
        print(f"   ✓ {filename}")

    # 4. 生成快捷方式图标
    print("\n⚡ 生成快捷方式图标...")
    for filename, char in SHORTCUT_ICONS:
        shortcut = create_shortcut_icon(96, char)
        shortcut.save(filename, 'PNG')
        print(f"   ✓ {filename}")

    print("\n" + "=" * 50)
    print("✅ 所有图标生成完成！")
    print("=" * 50)
    print("\n提示：")
    print("1. 重新构建 Docker 镜像以包含新图标")
    print("2. 或直接复制到容器: docker cp static/icons/ sht-cm:/app/static/")
    print("3. 清除浏览器缓存后重新添加到主屏幕")


if __name__ == '__main__':
    main()
