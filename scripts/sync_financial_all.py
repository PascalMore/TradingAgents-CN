#!/usr/bin/env python3
"""
全量财务数据同步：所有5522只股票。
复用 sync_financial_low_income.py 的 TushareProvider + connect() 模式。
分50批，每批验证通过后继续。
"""
import os
import sys
import time
import logging
import asyncio
from datetime import datetime, timezone
import asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['MONGODB_HOST'] = '172.25.240.1'
os.environ['MONGODB_PORT'] = '27017'
os.environ['MONGODB_USERNAME'] = 'myq'
os.environ['MONGODB_PASSWORD'] = '6812345'

from pymongo import MongoClient
from tradingagents.dataflows.providers.china.tushare import TushareProvider

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

TOTAL_BATCHES = 20  # 总批次数（仅用于日志显示）
BATCH_SIZE = 277   # 每批股票数量（5522/20≈277）
CURRENT_BATCH = 10   # 当前批次，完成后改为7，依次类推

def get_all_tushare_stocks(coll):
    """获取所有tushare股票代码，按代码升序"""
    return sorted([r['symbol'] for r in coll.find({'data_source': 'tushare'}, {'symbol': 1})])

def validate_batch(coll, batch_symbols):
    """核查批次数据：重复、period不一致、未更新"""
    stats = {'total': len(batch_symbols), 'updated': 0, 'not_updated': 0,
             'duplicates': 0, 'rp_mismatch': 0, 'income_low': 0}
    issues = []
    today = datetime.now(timezone.utc).date()
    
    for sym in batch_symbols:
        docs = list(coll.find({'symbol': sym, 'data_source': 'tushare'}))
        stats['duplicates'] += max(0, len(docs) - 1)
        if not docs:
            issues.append(f"  {sym}: 无记录")
            continue
        
        doc = docs[0]
        outer_rp = doc.get('report_period', '')
        updated_at = doc.get('updated_at')
        raw = doc.get('raw_data', {})
        inc = raw.get('income_statement', []) if isinstance(raw, dict) else []
        
        if updated_at:
            try:
                upd_date = updated_at.replace(tzinfo=timezone.utc).date()
                if upd_date >= today:
                    stats['updated'] += 1
                else:
                    stats['not_updated'] += 1
            except Exception:
                stats['not_updated'] += 1
        
        if inc:
            raw_latest = max(x.get('end_date') for x in inc if x.get('end_date'))
            if raw_latest and raw_latest != outer_rp:
                stats['rp_mismatch'] += 1
                issues.append(f"  {sym}: outer={outer_rp}, raw={raw_latest}")
        
        if len(inc) <= 3:
            stats['income_low'] += 1
    
    return stats, issues

def main():
    client = MongoClient('mongodb://myq:6812345@172.25.240.1:27017/')
    db = client['tradingagents']
    coll = db['stock_financial_data']
    
    symbols = get_all_tushare_stocks(coll)
    total = len(symbols)
    logger.info(f"全量同步: {total}只，分{TOTAL_BATCHES}批，每批{BATCH_SIZE}只")
    
    # 初始化 TushareProvider（复用 low_income 脚本的修复）
    tushare = TushareProvider()
    asyncio.run(tushare.connect())
    logger.info(f"TushareProvider 连接: {tushare.is_available()}\n")
    
    total_updated = total_failed = 0
    
    for batch_num in range(CURRENT_BATCH, CURRENT_BATCH + 1):  # 只跑当前批次
        start = (CURRENT_BATCH - 1) * BATCH_SIZE
        end = min(start + BATCH_SIZE, total)
        batch = symbols[start:end]
        
        logger.info(f"\n{'='*60}")
        logger.info(f"批次 {batch_num}/{TOTAL_BATCHES} | {batch[0]} ~ {batch[-1]} | {len(batch)}只")
        logger.info(f"{'='*60}")
        
        t0 = time.time()
        success = fail = 0
        
        for i, sym in enumerate(batch):
            try:
                # 获取全量历史（与 low_income 脚本相同）
                data = asyncio.run(tushare.get_financial_data(
                    symbol=sym,
                    limit=500,
                    start_date='19900101'
                ))
                
                if not data:
                    logger.info(f"  {sym}: ❌ 无数据")
                    fail += 1
                    continue
                
                # 保存（直接 upsert），从 raw_data 每条记录中移除 ts_code（已有 full_symbol）
                raw = data.get('raw_data', {})
                for table_name in ['income_statement', 'balance_sheet', 'cashflow_statement', 'financial_indicators', 'main_business']:
                    records = raw.get(table_name, [])
                    if isinstance(records, list):
                        for record in records:
                            record.pop('ts_code', None)
                
                inc = raw.get('income_statement', [])
                end_dates = [x.get('end_date') for x in inc if x.get('end_date')]
                latest_end = max(end_dates) if end_dates else None
                now = datetime.now(timezone.utc)
                coll.update_one(
                    {'symbol': sym, 'data_source': 'tushare'},
                    {'$set': {'report_period': latest_end, 'updated_at': now, 'raw_data': raw},
                     '$inc': {'version': 1}},
                    upsert=True
                )
                
                # 验证
                doc = coll.find_one({'symbol': sym, 'data_source': 'tushare'})
                saved = len(doc.get('raw_data', {}).get('income_statement', [])) if isinstance(doc.get('raw_data'), dict) else 0
                
                logger.info(f"  {sym}: ✅ {saved}条")
                success += 1
                
            except Exception as e:
                logger.info(f"  {sym}: ❌ {e}")
                fail += 1
            
            if (i + 1) % 20 == 0:
                logger.info(f"  进度: {i+1}/{len(batch)} ({success}✅ {fail}❌)")
        
        elapsed = time.time() - t0
        stats, issues = validate_batch(coll, batch)
        
        logger.info(f"\n--- 批次 {batch_num} 验证 ---")
        logger.info(f"  处理: {stats['total']}只 | 耗时: {elapsed:.0f}秒")
        logger.info(f"  ✅ 更新: {stats['updated']} | ⚠️ 未更新: {stats['not_updated']}")
        logger.info(f"  🔢 重复: {stats['duplicates']} | 📊 period不一致: {stats['rp_mismatch']} | 📉 income<=3: {stats['income_low']}")
        
        if issues:
            for issue in issues[:5]:
                logger.info(issue)
        
        critical_ok = stats['duplicates'] == 0 and stats['rp_mismatch'] == 0
        
        if critical_ok:
            logger.info(f"\n  ✅ 验证通过，继续")
            total_updated += stats['updated']
            total_failed += stats['not_updated']
        else:
            logger.info(f"\n  ❌ 验证失败，停止！")
            break
    
    logger.info(f"\n{'='*60}")
    logger.info(f"完成 {batch_num} 批 | 更新: {total_updated} | 未更新: {total_failed}")
    logger.info(f"{'='*60}")

if __name__ == '__main__':
    main()
