#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
在虚拟环境中运行 SHT 项目的启动脚本
"""

import os
import sys
import subprocess
import venv
from pathlib import Path

def main():
    """主函数"""
    project_dir = Path(__file__).parent
    venv_dir = project_dir / "venv"

    print("=" * 50)
    print("SHT 资源聚合系统 - 虚拟环境启动脚本")
    print("=" * 50)
    print()

    # 检查 venv 是否存在
    if not venv_dir.exists():
        print("📦 创建虚拟环境...")
        venv.create(venv_dir, with_pip=True)
        print("✅ 虚拟环境创建完成")
        print()
    else:
        print("✅ 虚拟环境已存在")
        print()

    # 获取 venv 中的 Python 路径
    if sys.platform == "win32":
        python_exe = venv_dir / "Scripts" / "python.exe"
        pip_exe = venv_dir / "Scripts" / "pip.exe"
    else:
        python_exe = venv_dir / "bin" / "python"
        pip_exe = venv_dir / "bin" / "pip"

    # 检查 requirements.txt
    requirements_file = project_dir / "requirements.txt"
    if not requirements_file.exists():
        print("❌ 错误: requirements.txt 不存在")
        sys.exit(1)

    # 升级 pip
    print("📦 升级 pip...")
    subprocess.run([str(python_exe), "-m", "pip", "install", "--upgrade", "pip"], check=True)
    print("✅ pip 升级完成")
    print()

    # 安装依赖
    print("📦 安装项目依赖...")
    subprocess.run([str(python_exe), "-m", "pip", "install", "-r", str(requirements_file)], check=True)
    print("✅ 依赖安装完成")
    print()

    # 检查 app.py
    app_file = project_dir / "app.py"
    if not app_file.exists():
        print("❌ 错误: app.py 不存在")
        sys.exit(1)

    # 启动应用
    print("=" * 50)
    print("🚀 启动 Flask 应用...")
    print("=" * 50)
    print()
    print("📍 项目目录:", project_dir)
    print("📍 应用入口:", app_file)
    print()
    print("💡 提示:")
    print("   - 访问地址: http://0.0.0.0:5001")
    print("   - 按 Ctrl+C 停止服务器")
    print()
    print("-" * 50)
    print()

    # 运行应用
    os.chdir(project_dir)
    sys.exit(subprocess.run([str(python_exe), str(app_file)]).returncode)

if __name__ == "__main__":
    main()
