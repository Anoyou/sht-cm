# 使用Python 3.11作为基础镜像
FROM python:3.11-slim

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PUID=0 \
    PGID=0

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc g++ curl libssl-dev pkg-config \
    procps htop gosu \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt /app/

# 安装Python依赖
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . /app/

# 创建数据目录并设置权限
RUN mkdir -p /app/data/logs && \
    chmod -R 777 /app/data && \
    chmod +x /app/start.py

# 设置Python路径
ENV PYTHONPATH=/app

# 暴露端口
EXPOSE 5000

# 复制启动脚本
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/api/health || exit 1

# 使用启动脚本
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

# 默认启动命令
CMD ["python", "/app/start.py", "web", "--workers", "4"]