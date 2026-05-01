#!/usr/bin/env python3
"""
针对 income <= 5 的540只股票，执行财务数据历史补齐。
验证每批处理后 income_count 是否增加。
"""
import os
import sys
import time
import logging
import asyncio
from collections import defaultdict

# Setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['MONGODB_HOST'] = '172.25.240.1'
os.environ['MONGODB_PORT'] = '27017'
os.environ['MONGODB_USERNAME'] = 'myq'
os.environ['MONGODB_PASSWORD'] = '6812345'

from pymongo import MongoClient
from tradingagents.dataflows.providers.china.tushare import TushareProvider

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(name)-55s | %(levelname)-8s | %(message)s')
logger = logging.getLogger(__name__)

BATCH_SIZE = 54  # 540 / 10 = 54

def get_low_income_stocks(coll):
    """获取 income <= 5 的股票，按 income_count 升序排列"""
    pipeline = [
        {"$match": {"data_source": "tushare"}},
        {"$project": {
            "symbol": 1,
            "income_count": {"$cond": [
                {"$isArray": "$raw_data.income_statement"},
                {"$size": "$raw_data.income_statement"},
                0
            ]},
        }},
        {"$match": {"income_count": {"$lte": 5}}},
        {"$sort": {"income_count": 1, "symbol": 1}}
    ]
    return [(r['symbol'], r['income_count']) for r in coll.aggregate(pipeline)]

def sync_and_validate(coll, tushare, symbols, batch_num):
    """同步一批股票并验证"""
    success = 0
    failed = 0
    improved = 0
    unchanged = 0
    before_after = []

    for i, (sym, before_ic) in enumerate(symbols):
        try:
            # 获取全量历史
            data = asyncio.run(tushare.get_financial_data(
                symbol=sym,
                limit=500,
                start_date='19900101'
            ))
            
            if not data or not data.get('income_statement'):
                failed += 1
                logger.warning(f"  {sym}: 无数据")
                continue

            # 保存
            from app.services.financial_data_service import FinancialDataService
            service = FinancialDataService(coll)
            asyncio.run(service.save_financial_data(sym, data, 'tushare'))
            
            # 验证
            doc = coll.find_one({'symbol': sym, 'data_source': 'tushare'})
            after_ic = len(doc.get('raw_data', {}).get('income_statement', [])) if isinstance(doc.get('raw_data'), dict) else 0
            
            before_after.append((sym, before_ic, after_ic))
            if after_ic > before_ic:
                improved += 1
            else:
                unchanged += 1
            success += 1
            
            if (i + 1) % 10 == 0:
                logger.info(f"  进度: {i+1}/{len(symbols)} ({success}✅ {failed}❌)")
                
        except Exception as e:
            failed += 1
            logger.error(f"  {sym}: 失败 - {e}")
    
    return success, failed, improved, unchanged, before_after

def main():
    client = MongoClient('mongodb://myq:6812345@172.25.240.1:27017/')
    db = client['tradingagents']
    coll = db['stock_financial_data']
    
    tushare = TushareProvider()
    asyncio.run(tushare.connect())  # 初始化连接
    
    stocks = get_low_income_stocks(coll)
    logger.info(f"income <= 5 的股票总数: {len(stocks)}")
    
    total_batches = (len(stocks) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_num in range(1, total_batches + 1):
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = start_idx + BATCH_SIZE
        batch_stocks = stocks[start_idx:end_idx]
        
        logger.info(f"\n========== 批次 {batch_num}/{total_batches} ==========")
        logger.info(f"  股票: {batch_stocks[0][0]} ~ {batch_stocks[-1][0]}")
        logger.info(f"  数量: {len(batch_stocks)} 只")
        
        t0 = time.time()
        success, failed, improved, unchanged, before_after = sync_and_validate(
            coll, tushare, batch_stocks, batch_num
        )
        elapsed = time.time() - t0
        
        logger.info(f"\n批次 {batch_num} 完成: {success}✅ {failed}❌ ({elapsed:.1f}秒)")
        logger.info(f"  改善: {improved} | 无变化: {unchanged}")
        
        # 显示改善的股票
        if improved > 0:
            improved_list = [(s, b, a) for s, b, a in before_after if a > b]
            logger.info(f"\n改善的股票 ({len(improved_list)}只):")
            for sym, before, after in improved_list[:10]:
                logger.info(f"  {sym}: {before}条 -> {after}条")
            if len(improved_list) > 10:
                logger.info(f"  ... 还有 {len(improved_list)-10} 只")
        
        # 显示仍为2-3条的股票
        still_low = [(s, b, a) for s, b, a in before_after if a <= 5]
        logger.info(f"\n仍为 <=5条 的股票 ({len(still_low)}只):")
        for sym, before, after in still_low[:5]:
            logger.info(f"  {sym}: {before}条 -> {after}条 (Tushare数据限制)")
        if len(still_low) > 5:
            logger.info(f"  ... 还有 {len(still_low)-5} 只")
    
    logger.info(f"\n========== 全部批次完成 ==========")

if __name__ == '__main__':
    main()
