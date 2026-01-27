# 调度器模块 - 爬虫任务调度和管理
# v1.5.2 - 清理循环依赖：请从子模块显式导入

# 向后兼容说明：
# 由于本项目存在较多跨模块引用，不再在 __init__.py 中进行统一导出，
# 以防止 "partially initialized module" 错误。
# 
# 建议写法：
# from scheduler.core import run_crawling_task
# from scheduler.utils import stop_crawling_task
# from scheduler.state import sync_crawl_state