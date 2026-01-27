#!/bin/bash
set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检测是否为生产环境
IS_PRODUCTION=${FLASK_ENV:-production}

echo -e "${GREEN}=== SHT 容器启动脚本 ===${NC}"

# 获取 PUID 和 PGID，默认为 0（root）
PUID=${PUID:-0}
PGID=${PGID:-0}

# 生产环境默认关闭 debug
if [ "$IS_PRODUCTION" = "production" ]; then
    FLASK_DEBUG=${FLASK_DEBUG:-false}
else
    # 开发环境，可以开启 debug
    FLASK_DEBUG=${FLASK_DEBUG:-true}
fi

# SECRET_KEY 安全检查和自动生成
CONFIG_FILE="/app/data/config/config.json"
SECRET_KEY_GENERATED=false

# 检查环境变量中的 SECRET_KEY
if [ -z "$SECRET_KEY" ] || [ "$SECRET_KEY" = "sht-default-secret-key" ]; then
    # 尝试从配置文件读取
    if [ -f "$CONFIG_FILE" ]; then
        SECRET_KEY=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE', encoding='utf-8')).get('app', {}).get('SECRET_KEY', ''))" 2>/dev/null || echo "")
    fi
    
    # 如果配置文件中也没有，自动生成
    if [ -z "$SECRET_KEY" ] || [ "$SECRET_KEY" = "sht-default-secret-key" ]; then
        echo -e "${YELLOW}SECRET_KEY 未配置，自动生成中...${NC}"
        
        # 生成新的 SECRET_KEY
        SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
        
        # 保存到配置文件
        python3 << EOF
import json
import os

# 确保目录存在
os.makedirs(os.path.dirname('$CONFIG_FILE'), exist_ok=True)

# 读取现有配置
config = {}
if os.path.exists('$CONFIG_FILE'):
    with open('$CONFIG_FILE', 'r', encoding='utf-8') as f:
        config = json.load(f)

# 确保 app 段存在
if 'app' not in config:
    config['app'] = {}

# 更新 SECRET_KEY
config['app']['SECRET_KEY'] = '$SECRET_KEY'

# 保存配置
with open('$CONFIG_FILE', 'w', encoding='utf-8') as f:
    json.dump(config, f, ensure_ascii=False, indent=2)

print(f"✓ 已自动生成并保存 SECRET_KEY 到 {os.path.relpath('$CONFIG_FILE', '/app')}")
EOF
        echo -e "${GREEN}✓ 已自动生成并保存 SECRET_KEY 到 $CONFIG_FILE${NC}"
        SECRET_KEY_GENERATED=true
    fi
fi

# 导出 SECRET_KEY 到环境变量
export SECRET_KEY

echo -e "${YELLOW}配置信息：${NC}"
echo " 环境: $IS_PRODUCTION"
echo " Debug模式: $FLASK_DEBUG"
echo " PUID: $PUID"
echo " PGID: $PGID"

echo -e "${GREEN}=== 启动应用 ===${NC}"

# 执行传入的启动命令 (从 CMD 或 docker run 参数获取)
exec "$@"