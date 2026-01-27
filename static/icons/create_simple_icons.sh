#!/bin/bash

# 创建简单的占位符图标文件
# 这些是最小的有效PNG文件

# 1x1像素的透明PNG（base64编码）
TRANSPARENT_PNG="iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVQIHWNgAAIAAAUAAY27m/MAAAAASUVORK5CYII="

# 创建不同尺寸的图标文件
sizes=(72 96 128 144 152 192 384 512)

for size in "${sizes[@]}"; do
    filename="icon-${size}x${size}.png"
    echo "$TRANSPARENT_PNG" | base64 -d > "$filename"
    echo "创建占位符图标: $filename"
done

# 创建Apple Touch图标
echo "$TRANSPARENT_PNG" | base64 -d > "apple-touch-icon.png"
echo "创建Apple Touch图标: apple-touch-icon.png"

echo "所有占位符图标创建完成！"
echo "请使用generate_icons.html在浏览器中生成实际图标"