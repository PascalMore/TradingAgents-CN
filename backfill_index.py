#!/usr/bin/env python3
"""
补指数历史数据脚本 - 补回A股指数历史日K缺失数据
"""
import asyncio
import sys
import os
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 设置环境变量
os.environ.setdefault('PYTHONPATH', str(project_root))

# 加载 .env 文件
from dotenv import load_dotenv
load_dotenv(project_root / ".env")

# 初始化数据库
from app.core.database import init_database, close_database
from app.worker.tushare_sync_service import get_tushare_sync_service


async def backfill_index_data():
    """补回缺失的指数历史数据"""
    print("=" * 60)
    print("A股指数历史数据补数据脚本")
    print("=" * 60)
    
    # 初始化数据库
    print("初始化数据库连接...")
    await init_database()
    print("✅ 数据库初始化完成")
    
    # 初始化同步服务
    sync_service = await get_tushare_sync_service()
    
    print()
    print("限流策略:")
    print("  - API调用间隔: 0.5秒")
    print("  - 预计总时间: 较长，请耐心等待")
    print()
    print("=" * 60)
    
    # 设置较慢的限流
    sync_service.rate_limit_delay = 0.5
    
    # 同步指数历史数据
    print("开始补指数数据（使用增量同步模式）...")
    print()
    
    try:
        result = await sync_service.sync_historical_index_data(
            incremental=True
        )
        print()
        print("=" * 60)
        print("✅ 指数历史数据同步完成!")
        print(f"处理指数数: {result.get('total_processed', 'N/A')}")
        print(f"成功: {result.get('success_count', 'N/A')}")
        print(f"失败: {result.get('error_count', 'N/A')}")
        print(f"新增记录: {result.get('total_records', 'N/A')}")
    except Exception as e:
        print(f"❌ 同步出错: {e}")
        import traceback
        traceback.print_exc()
    
    print("=" * 60)
    
    # 关闭数据库连接
    await close_database()
    print("数据库连接已关闭")


if __name__ == "__main__":
    asyncio.run(backfill_index_data())
