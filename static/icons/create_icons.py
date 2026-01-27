#!/usr/bin/env python3
"""
简单的PWA图标生成脚本
使用PIL库生成不同尺寸的图标
"""

try:
    from PIL import Image, ImageDraw, ImageFont
    import os
    
    def create_icon(size):
        # 创建图像
        img = Image.new('RGBA', (size, size), (59, 130, 246, 255))  # 蓝色背景
        draw = ImageDraw.Draw(img)
        
        # 绘制圆角矩形（简化版）
        # 这里简化处理，直接使用矩形
        
        # 绘制文字
        try:
            # 尝试使用系统字体
            font_size = max(size // 4, 12)
            font = ImageFont.truetype("/System/Library/Fonts/Arial.ttf", font_size)
        except:
            try:
                font = ImageFont.truetype("arial.ttf", font_size)
            except:
                font = ImageFont.load_default()
        
        # 绘制SHT文字
        text = "SHT"
        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        x = (size - text_width) // 2
        y = (size - text_height) // 2 - size // 8
        
        draw.text((x, y), text, fill=(255, 255, 255, 255), font=font)
        
        # 绘制副标题
        try:
            small_font_size = max(size // 12, 8)
            small_font = ImageFont.truetype("/System/Library/Fonts/Arial.ttf", small_font_size)
        except:
            small_font = font
        
        subtitle = "资源管理"
        bbox = draw.textbbox((0, 0), subtitle, font=small_font)
        text_width = bbox[2] - bbox[0]
        
        x = (size - text_width) // 2
        y = (size - text_height) // 2 + size // 6
        
        draw.text((x, y), subtitle, fill=(255, 255, 255, 200), font=small_font)
        
        return img
    
    # 生成不同尺寸的图标
    sizes = [72, 96, 128, 144, 152, 192, 384, 512]
    
    for size in sizes:
        icon = create_icon(size)
        filename = f"icon-{size}x{size}.png"
        icon.save(filename)
        print(f"生成图标: {filename}")
    
    # 生成Apple Touch图标
    apple_icon = create_icon(180)
    apple_icon.save("apple-touch-icon.png")
    print("生成Apple Touch图标: apple-touch-icon.png")
    
    print("所有图标生成完成！")

except ImportError:
    print("PIL库未安装，请运行: pip install Pillow")
    print("或者使用generate_icons.html在浏览器中生成图标")