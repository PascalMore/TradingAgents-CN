#!/usr/bin/env python3
"""
Tushare获取指数数据服务测试
验证Tushare获取指数相关数据的测试，如指数基本信息、指数行情等。
"""

import os
import sys
import pandas as pd
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

# 添加项目根目录到Python路径04
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

async def main():
    """主测试函数"""
    from app.core.database import init_database, get_mongo_db
    from app.worker.tushare_sync_service import get_tushare_sync_service

    try:

        # 1. 初始化数据库
        print("🔄 初始化数据库连接...")
        await init_database()
        print("✅ 数据库连接成功")
        print()

        # 2. 获取同步服务
        print("🔄 初始化同步服务...")
        sync_service = await get_tushare_sync_service()
        print("✅ 同步服务初始化完成")
        print()

        # 3.测试指数基本信息同步
        res = await sync_service.sync_index_basic_info()
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)



if __name__ == "__main__":
    asyncio.run(main())
