#!/usr/bin/env python3
"""
全量财务数据同步：所有5522只股票，分50批处理。
每批完成后进行完整数据核查，验证通过后继续下一批。
"""
import os
import sys
import time
import logging
import asyncio
from datetime import datetime, timezone
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['MONGODB_HOST'] = '172.25.240.1'
os.environ['MONGODB_PORT'] = '27017'
os.environ['MONGODB_USERNAME'] = 'myq'
os.environ['MONGODB_PASSWORD'] = '6812345'

from pymongo import MongoClient
from tradingagents.dataflows.providers.china.tushare import TushareProvider

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

TOTAL_BATCHES = 50
MAX_WORKERS = 8
CHUNK_SIZE = 500  # Tushare API limit

def get_all_tushare_symbols(coll):
    """获取所有tushare股票代码"""
    symbols = [r['symbol'] for r in coll.find({'data_source': 'tushare'}, {'symbol': 1})]
    return sorted(symbols)

def ts_code_of(symbol):
    if symbol.startswith('9'):
        return f'{symbol}.BJ'
    elif symbol.startswith('6') or symbol.startswith('5'):
        return f'{symbol}.SH'
    else:
        return f'{symbol}.SZ'

def fetch_financial(args):
    """调TushareProvider获取财务数据（共享实例）"""
    ts_code, provider = args
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        data = loop.run_until_complete(
            provider.get_financial_data(ts_code, limit=CHUNK_SIZE, start_date='19900101')
        )
        loop.close()
        return data
    except Exception as e:
        return None

def standardize_and_save(coll, symbol, data):
    """标准化数据并写入MongoDB"""
    if not data or not data.get('raw_data'):
        return None, 'no_data'

    raw = data.get('raw_data', {})
    inc = raw.get('income_statement', [])

    if not inc:
        return None, 'no_income'

    # 最新 end_date
    end_dates = [x.get('end_date') for x in inc if x.get('end_date')]
    latest_end = max(end_dates) if end_dates else None
    report_type = 'annual' if latest_end and latest_end.endswith('1231') else 'quarterly'

    # 写入
    now = datetime.now(timezone.utc)
    coll.update_one(
        {'symbol': symbol, 'data_source': 'tushare'},
        {'$set': {
            'report_period': latest_end,
            'report_type': report_type,
            'updated_at': now,
            'raw_data': raw,
        }, '$inc': {'version': 1}},
        upsert=True
    )

    # 验证写入
    doc = coll.find_one({'symbol': symbol, 'data_source': 'tushare'})
    saved_inc = doc.get('raw_data', {}).get('income_statement', []) if isinstance(doc.get('raw_data'), dict) else []
    saved_count = len(saved_inc)

    return saved_count, 'success' if saved_count > 0 else 'save_failed'

def validate_batch(coll, batch_symbols, batch_num):
    """核查本批次数据"""
    issues = []
    stats = {
        'total': len(batch_symbols),
        'updated': 0,
        'not_updated': 0,
        'duplicates': 0,
        'rp_mismatch': 0,
        'income_too_low': 0,
    }

    for sym in batch_symbols:
        docs = list(coll.find({'symbol': sym, 'data_source': 'tushare'}))
        stats['duplicates'] += max(0, len(docs) - 1)

        if len(docs) == 0:
            issues.append(f"  {sym}: 无记录")
            continue

        doc = docs[0]
        outer_rp = doc.get('report_period', '')
        updated_at = doc.get('updated_at', '')
        raw = doc.get('raw_data', {})
        inc = raw.get('income_statement', []) if isinstance(raw, dict) else []
        saved_count = len(inc)

        # 是否本批次更新
        if updated_at:
            batch_start = datetime.fromisoformat('2026-04-30T00:00:00+00:00')
            if updated_at.replace(tzinfo=timezone.utc) >= batch_start:
                stats['updated'] += 1
            else:
                stats['not_updated'] += 1

        # 外层 period 一致性
        if inc:
            raw_latest = max(x.get('end_date') for x in inc if x.get('end_date'))
            if raw_latest and raw_latest != outer_rp:
                stats['rp_mismatch'] += 1
                issues.append(f"  {sym}: outer_rp={outer_rp}, raw_latest={raw_latest}")

        # income 数据量检查（低于3条可能是问题）
        if saved_count <= 3:
            stats['income_too_low'] += 1
            issues.append(f"  {sym}: income仅{saved_count}条 (可能Tushare数据不足)")

    return stats, issues

def main():
    client = MongoClient('mongodb://myq:6812345@172.25.240.1:27017/')
    db = client['tradingagents']
    coll = db['stock_financial_data']

    symbols = get_all_tushare_symbols(coll)
    total = len(symbols)
    per_batch = (total + TOTAL_BATCHES - 1) // TOTAL_BATCHES

    logger.info(f"全量同步: {total}只股票，分{TOTAL_BATCHES}批，每批~{per_batch}只")
    logger.info(f"使用 TushareProvider + connect() 初始化\n")

    # 共享一个 provider 实例
    shared_provider = TushareProvider()
    asyncio.run(shared_provider.connect())
    logger.info(f"Provider 已连接: {shared_provider.is_available()}\n")

    total_updated = 0
    total_failed = 0
    all_stats = []

    for batch_num in range(1, TOTAL_BATCHES + 1):
        start = (batch_num - 1) * per_batch
        end = min(start + per_batch, total)
        batch_symbols = symbols[start:end]

        logger.info(f"\n{'='*60}")
        logger.info(f"批次 {batch_num}/{TOTAL_BATCHES} | {batch_symbols[0]} ~ {batch_symbols[-1]}")
        logger.info(f"{'='*60}")

        t0 = time.time()
        results = {}

        # 并发获取（共享provider）
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_financial, (ts_code_of(sym), shared_provider)): sym for sym in batch_symbols}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    data = future.result()
                    results[sym] = data
                except Exception as e:
                    results[sym] = None

        # 写入数据库
        saved_counts = {}
        for sym in batch_symbols:
            data = results.get(sym)
            if data:
                saved_cnt, status = standardize_and_save(coll, sym, data)
                saved_counts[sym] = saved_cnt
                if status == 'success':
                    logger.info(f"  {sym}: ✅ {saved_cnt}条")
                else:
                    logger.info(f"  {sym}: ❌ {status}")
            else:
                logger.info(f"  {sym}: ❌ fetch失败")
                saved_counts[sym] = None

        elapsed = time.time() - t0

        # 验证
        stats, issues = validate_batch(coll, batch_symbols, batch_num)
        all_stats.append(stats)

        logger.info(f"\n--- 批次 {batch_num} 验证结果 ---")
        logger.info(f"  处理: {stats['total']}只 | 耗时: {elapsed:.0f}秒")
        logger.info(f"  ✅ 本批次更新: {stats['updated']}只")
        logger.info(f"  ⚠️ 未更新(时间未变): {stats['not_updated']}只")
        logger.info(f"  🔢 重复记录: {stats['duplicates']}只")
        logger.info(f"  📊 外层period不一致: {stats['rp_mismatch']}只")
        logger.info(f"  📉 income<=3条: {stats['income_too_low']}只")

        if issues:
            logger.info(f"\n  问题详情:")
            for issue in issues[:10]:
                logger.info(issue)
            if len(issues) > 10:
                logger.info(f"  ... 还有{len(issues)-10}条问题")

        # 验证通过条件
        critical_ok = stats['duplicates'] == 0 and stats['rp_mismatch'] == 0

        if critical_ok:
            logger.info(f"\n  ✅ 验证通过，继续下一批")
            total_updated += stats['updated']
            total_failed += stats['not_updated']
        else:
            logger.info(f"\n  ❌ 验证失败，停止！")
            logger.info(f"  请检查上述问题后再继续")
            break

    logger.info(f"\n{'='*60}")
    logger.info(f"全部完成！共处理 {batch_num} 批")
    logger.info(f"总更新: {total_updated}只 | 未更新: {total_failed}只")
    logger.info(f"{'='*60}")

if __name__ == '__main__':
    main()
