# GitHub Actions 自动打包配置

## 功能

- **自动构建**: 每次推送到 `main` 分支时自动构建镜像
- **多架构支持**: 同时构建 `linux/amd64` 和 `linux/arm64` 架构
- **自动推送**: 自动推送到 Docker Hub
- **版本标签**: 支持根据 Git tag 自动生成版本标签

## 配置步骤

### 1. 配置 GitHub Secrets

在 GitHub 仓库页面：
1. 点击 **Settings** → **Secrets and variables** → **Actions**
2. 点击 **New repository secret**
3. 添加以下两个 secrets：

| Name | Value |
|------|-------|
| `DOCKER_USERNAME` | 你的 Docker Hub 用户名 |
| `DOCKER_PASSWORD` | 你的 Docker Hub 密码或 Access Token |

### 2. 创建 Access Token（推荐）

Docker Hub 密码建议使用 Access Token：
1. 登录 [Docker Hub](https://hub.docker.com)
2. 点击头像 → **Account Settings** → **Security** → **New Access Token**
3. 生成后复制到 GitHub Secrets 的 `DOCKER_PASSWORD`

### 3. 确保仓库可见性

确保 Docker Hub 仓库是公开的，或者已登录有权限推送。

## 触发条件

自动构建会在以下情况触发：

- 推送到 `main` 分支
- 创建版本标签（如 `v1.5.5`）
- Pull Request 到 `main` 分支（仅构建不推送）

## 生成的镜像标签

| 触发条件 | 生成的标签 |
|---------|-----------|
| 推送到 main | `anoyou/sht-cm:main`, `anoyou/sht-cm:latest` |
| 创建 tag v1.5.5 | `anoyou/sht-cm:1.5.5`, `anoyou/sht-cm:1.5`, `anoyou/sht-cm:1` |
| Pull Request | `anoyou/sht-cm:pr-1` |

## 手动触发

也可以在 GitHub 页面手动触发：
1. 进入 **Actions** 标签
2. 选择 **Build and Push Docker Image**
3. 点击 **Run workflow**
