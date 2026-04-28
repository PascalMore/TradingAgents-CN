#!/usr/bin/env python3
"""
补数据脚本 - 补回A股历史日K缺失数据
使用安全的限流策略，避免触发Tushare API限制
"""
import asyncio
import sys
import os
from datetime import date, timedelta
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


async def backfill_missing_data():
    """补回缺失的历史数据"""
    print("=" * 60)
    print("A股历史数据补数据脚本")
    print("=" * 60)
    
    # 初始化数据库
    print("初始化数据库连接...")
    await init_database()
    print("✅ 数据库初始化完成")
    
    # 初始化同步服务
    sync_service = await get_tushare_sync_service()
    
    # 计算缺失的交易日
    # 数据库最新: 2026-02-13, 需要补: 2026-02-14 至今
    last_db_date = date(2026, 2, 13)
    today = date.today()
    
    # 计算缺失天数（排除周末）
    missing_days = 0
    current = last_db_date + timedelta(days=1)
    trading_days = []
    while current <= today:
        if current.weekday() < 5:  # Mon-Fri
            trading_days.append(current)
            missing_days += 1
        current += timedelta(days=1)
    
    print(f"数据库最新日期: {last_db_date}")
    print(f"今天日期: {today}")
    print(f"缺失交易日: {missing_days} 天")
    if trading_days:
        print(f"需要补的交易日: {trading_days[0]} ~ {trading_days[-1]}")
    print()
    print("限流策略:")
    print("  - API调用间隔: 0.5秒")
    print("  - 预计总时间: 较长，请耐心等待")
    print()
    print("=" * 60)
    
    # 设置较慢的限流
    sync_service.rate_limit_delay = 0.5  # 0.5秒间隔
    
    # 同步缺失的历史数据
    print("开始补数据（使用增量同步模式）...")
    print()
    
    try:
        result = await sync_service.sync_historical_data(
            incremental=True
        )
        print()
        print("=" * 60)
        print("✅ 历史数据同步完成!")
        print(f"处理股票数: {result.get('total_processed', 'N/A')}")
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
    asyncio.run(backfill_missing_data())
