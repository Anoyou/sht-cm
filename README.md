# SHT 资源爬取管理平台 ｜ SHT Crawler & Management

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/Flask-2.x-green.svg)](https://flask.palletsprojects.com/)
[![Docker](https://img.shields.io/badge/Docker-Supported-blue.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

一个功能完善的资源聚合与爬虫管理平台，支持多线程/异步爬虫、Telegram Bot 通知、Web UI 管理等功能。

## ✨ 功能特性

### 🕷️ 爬虫功能
- **三种爬虫模式**：异步模式 (推荐)、多线程模式、串行模式
- **智能调度**：支持并发控制、随机延迟、错误重试
- **增量爬取**：自动识别已爬取内容，避免重复
- **定时任务**：支持每日自动爬取

### 🌐 Web 管理界面 特别是移动端和支持 PWA 模式，可类似 App 般使用～强烈建议体验！
- **资源浏览**：支持分类筛选、搜索、分页
- **爬虫控制**：可视化爬虫启停、进度监控
- **系统配置**：所有配置项可通过 Web 界面修改
- **日志查看**：实时查看应用日志和爬虫日志
- **数据统计**：资源统计、爬虫统计、系统状态

### 🤖 Telegram Bot
- **实时通知**：爬虫开始/结束/异常通知
- **自定义模板**：支持自定义通知消息模板
- **远程控制**：通过 Bot 远程查看状态

### 💾 数据管理
- **SQLite 数据库**：轻量级，无需额外服务
- **自动维护**：数据库优化、重复清理、备份
- **数据导出**：支持导出数据

## 🚀 快速开始

### 方式一：Docker 部署（推荐）

#### 最简部署
```bash
# 1. 克隆项目
git clone https://github.com/Anoyou/sht-cm.git
cd sht-cm

# 2. 使用精简版 compose 启动
docker-compose -f docker-compose.min.yml up -d

# 3. 访问 Web 界面配置
open http://localhost:5000/config
```

#### 完整配置部署
```bash
# 使用完整版 compose，通过环境变量预配置
cp docker-compose.full.yml docker-compose.yml

# 编辑配置（可选）
export TG_BOT_TOKEN="your_token"
export PROXY="http://proxy:port"

# 启动
docker-compose up -d
```

### 方式二：本地运行

```bash
# 1. 创建虚拟环境
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 2. 安装依赖
pip install -r requirements.txt

# 3. 启动应用
python start.py web --port 5000

# 4. 访问
open http://localhost:5000
```

## ⚙️ 配置说明

### 环境变量（Docker）

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `SECRET_KEY` | Flask 密钥 | 自动生成 |
| `TG_BOT_TOKEN` | Telegram Bot Token | - |
| `TG_NOTIFY_CHAT_ID` | Telegram 通知 Chat ID | - |
| `PROXY` | HTTP 代理地址 | - |
| `LOG_LEVEL` | 日志级别 | INFO |
| `TZ` | 时区 | Asia/Shanghai |

### Web 界面配置

首次启动后访问 `http://localhost:5000/config`：
- 系统设置：代理、日志级别、时区
- Telegram：Bot Token、通知 Chat ID
- 爬虫配置：模式选择、并发数、延迟
- 功能开关：安全模式、定时爬取

## 🏗️ 项目结构

```
sht-cm/
├── app.py                  # Flask 应用入口
├── start.py                # 统一启动脚本
├── configuration.py        # 统一配置管理器
├── models.py               # SQLAlchemy 数据模型
├── bot.py                  # Telegram Bot
├── task_manager.py         # 任务管理器
├── sht_crawler.py          # 爬虫核心
├── sht2bm_adapter.py       # SHT2BM API 适配器
├── cache_manager.py        # 缓存管理器
├── health.py               # 健康检查
├── maintenance_tools.py    # 数据库维护工具
├── constants.py            # 全局常量
├── requirements.txt        # Python 依赖列表
├── docker-compose.yml      # Docker 配置（精简版）
├── docker-compose.full.yml # Docker 配置（完整版）
├── Dockerfile              # Docker 镜像构建
├── docker-entrypoint.sh    # Docker 入口脚本
├── README.md               # 项目文档
├── .env                    # 环境变量（可选）
├── blueprints/             # Flask 蓝图（API 路由）
├── crawler/                # 爬虫实现
├── crawler_control/        # 爬虫控制器
├── scheduler/              # 定时任务调度
├── services/               # 业务服务层
├── static/                 # 静态资源（CSS/JS/图片）
├── templates/              # HTML 模板
├── utils/                  # 工具函数
└── data/                   # 数据目录（数据库、日志、配置）
```

## 🛠️ 技术栈

- **后端**: Python 3.9+, Flask, SQLAlchemy
- **前端**: HTML5, CSS3, JavaScript
- **数据库**: SQLite
- **爬虫**: aiohttp, requests, BeautifulSoup
- **部署**: Docker, Docker Compose
- **通知**: python-telegram-bot

## 📋 系统要求

- **CPU**: 1 核及以上
- **内存**: 512MB 及以上
- **磁盘**: 1GB 可用空间（数据存储）
- **网络**: 可访问目标网站

## 🔒 安全说明

1. **SECRET_KEY**: 用于加密 Session，首次启动自动生成
2. **安全模式**: 开启后资源图片会模糊显示
3. **代理支持**: 支持 HTTP 代理，保护爬虫 IP
4. **访问控制**: 建议通过反向代理添加基础认证

## 🐳 Docker 部署进阶

### 使用持久化卷
```yaml
volumes:
  - ./data:/app/data  # 数据持久化
```

### 指定时区
```yaml
environment:
  - TZ=Asia/Shanghai
```

### 查看日志
```bash
docker-compose logs -f web
```

## 🔌 SHT2BM 集成

本项目内置 SHT2BM API 接口，可作为 BM 的数据源使用。

### API链接为：

```
http://[本项目IP]:5000/api/bt?keyword
```


### ⚠️ 安全提示

- **未测试公网环境**：该接口目前仅在局域网环境测试
- **不建议直接暴露公网**：如需公网访问，建议配合反向代理和认证使用
- **本地网络优先**：推荐在同一内网环境使用，避免安全风险

### 配置 BM

```
略，不适合在此讨论。
```

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

MIT License

---

**⚠️ 免责声明**: 本项目仅供学习研究使用，请勿用于非法用途。使用本项目产生的任何后果由使用者自行承担。
