#!/usr/bin/env python3
"""
Tushare获取指数相关信息测试
验证Tushare获取指数相关数据的测试，如指数基本信息、指数行情等。
"""

import os
import sys
import pandas as pd
import asyncio
from datetime import datetime, timedelta


def test_tushare_index_info_sync(provider):
    index_list = provider.get_index_list_sync(market="SZSE")
    #index_list.to_excel("123.xlsx")
    if not index_list.empty:
        print(f"✅ 获取指数列表成功: {len(index_list)}条")
        print(f"📊 示例指数: {index_list.head(3)[['ts_code', 'name']].to_string(index=False)}")
    else:
        print("❌ 获取指数列表失败")

async def test_tushare_index_info(provider):
    index_list = await provider.get_index_list(market="CSI")
    if len(index_list) > 0:
        print(f"✅ 获取指数列表成功: {len(index_list)}条")
        print(f"📊 示例指数: {pd.DataFrame(index_list).head(3).to_string(index=False)}")
    else:
        print("❌ 获取指数列表失败")

async def main():
    """主测试函数"""
    from tradingagents.dataflows.providers.china.tushare import TushareProvider
    #初始化tushare
    provider = TushareProvider()
    #连接tushare
    success = await provider.connect()
    if not success:
        raise RuntimeError("❌ Tushare连接失败，无法启动同步服务")
    
    test_tushare_index_info_sync(provider)
    await test_tushare_index_info(provider)

if __name__ == "__main__":
    asyncio.run(main())
