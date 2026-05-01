#!/usr/bin/env python3
"""
全量财务数据同步 v3：直接调Tushare REST API，限速200次/分钟。
分50批，每批完成后验证，通过后继续。
"""
import os
import sys
import time
import logging
import requests
from datetime import datetime, timezone
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['MONGODB_HOST'] = '172.25.240.1'
os.environ['MONGODB_PORT'] = '27017'
os.environ['MONGODB_USERNAME'] = 'myq'
os.environ['MONGODB_PASSWORD'] = '6812345'

from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

TOKEN = '3b633dea5db1ed3a161359154cb19b1d71c786370643d4519f302e6b'
TUSHARE_API = 'http://api.tushare.pro'
TOTAL_BATCHES = 50
RATE_LIMIT = 180  # 留20次余量
CALL_DELAY = 60.0 / RATE_LIMIT  # 每秒最多N次

def ts_code_of(symbol):
    if symbol.startswith('9'):
        return f'{symbol}.BJ'
    elif symbol.startswith('6') or symbol.startswith('5'):
        return f'{symbol}.SH'
    else:
        return f'{symbol}.SZ'

def call_api(api_name, params, fields=''):
    """限速调Tushare REST API"""
    time.sleep(CALL_DELAY)
    try:
        resp = requests.post(TUSHARE_API, json={
            'api_name': api_name,
            'token': TOKEN,
            'params': params,
            'fields': fields
        }, timeout=15)
        return resp.json()
    except Exception as e:
        return None

def fetch_financial(symbol):
    """获取单只股票全量财务数据"""
    ts = ts_code_of(symbol)
    result = {'income_statement': [], 'balance_sheet': [], 'cashflow_statement': [], 'financial_indicators': []}
    
    # 4个接口，串行调用（受限于200次/分钟）
    apis = [
        ('income', {'ts_code': ts, 'limit': 500, 'start_date': '19900101'}, result['income_statement']),
        ('balancesheet', {'ts_code': ts, 'limit': 500, 'start_date': '19900101'}, result['balance_sheet']),
        ('cashflow', {'ts_code': ts, 'limit': 500, 'start_date': '19900101'}, result['cashflow_statement']),
        ('fina_indicator', {'ts_code': ts, 'limit': 500, 'start_date': '19900101'}, result['financial_indicators']),
    ]
    
    for api_name, params, dest in apis:
        data = call_api(api_name, params)
        if data and data.get('data') and data['data'].get('fields'):
            fields = data['data']['fields']
            for row in data['data']['items']:
                dest.append(dict(zip(fields, row)))
    
    return result if any(result.values()) else None

def standardize_and_save(coll, symbol, raw_data):
    """标准化并写入"""
    inc = raw_data.get('income_statement', [])
    if not inc:
        return None, 'no_income'
    
    end_dates = [x.get('end_date') for x in inc if x.get('end_date')]
    latest_end = max(end_dates) if end_dates else None
    report_type = 'annual' if latest_end and str(latest_end).endswith('1231') else 'quarterly'
    
    # 移除 raw_data 中每条记录的 ts_code（已有 full_symbol）
    for table_name in ['income_statement', 'balance_sheet', 'cashflow_statement', 'financial_indicators', 'main_business']:
        records = raw_data.get(table_name, [])
        if isinstance(records, list):
            for record in records:
                if isinstance(record, dict):
                    record.pop('ts_code', None)
    
    now = datetime.now(timezone.utc)
    coll.update_one(
        {'symbol': symbol, 'data_source': 'tushare'},
        {'$set': {'report_period': latest_end, 'report_type': report_type,
                  'updated_at': now, 'raw_data': raw_data},
         '$inc': {'version': 1}},
        upsert=True
    )
    
    doc = coll.find_one({'symbol': symbol, 'data_source': 'tushare'})
    saved = len(doc.get('raw_data', {}).get('income_statement', [])) if isinstance(doc.get('raw_data'), dict) else 0
    return saved, 'success' if saved > 0 else 'save_failed'

def validate_batch(coll, batch_symbols):
    """验证批次数据"""
    stats = {'total': len(batch_symbols), 'updated': 0, 'not_updated': 0,
             'duplicates': 0, 'rp_mismatch': 0, 'income_low': 0}
    issues = []
    
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
            today = datetime.now(timezone.utc).date()
            if updated_at.replace(tzinfo=timezone.utc).date() >= today:
                stats['updated'] += 1
            else:
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
    
    symbols = sorted([r['symbol'] for r in coll.find({'data_source': 'tushare'}, {'symbol': 1})])
    total = len(symbols)
    per_batch = (total + TOTAL_BATCHES - 1) // TOTAL_BATCHES
    
    logger.info(f"全量同步: {total}只，分{TOTAL_BATCHES}批，每批~{per_batch}只")
    logger.info(f"限速: {RATE_LIMIT}次/分钟，预计总耗时: {total * 4 * CALL_DELAY / 60:.0f}分钟\n")
    
    total_updated = total_failed = 0
    
    for batch_num in range(1, TOTAL_BATCHES + 1):
        start = (batch_num - 1) * per_batch
        end = min(start + per_batch, total)
        batch = symbols[start:end]
        
        logger.info(f"\n{'='*60}")
        logger.info(f"批次 {batch_num}/{TOTAL_BATCHES} | {batch[0]} ~ {batch[-1]} | {len(batch)}只")
        logger.info(f"{'='*60}")
        
        t0 = time.time()
        success = fail = 0
        
        for i, sym in enumerate(batch):
            raw = fetch_financial(sym)
            if raw:
                cnt, status = standardize_and_save(coll, sym, raw)
                if status == 'success':
                    logger.info(f"  {sym}: ✅ {cnt}条")
                    success += 1
                else:
                    logger.info(f"  {sym}: ❌ {status}")
                    fail += 1
            else:
                logger.info(f"  {sym}: ❌ 无数据")
                fail += 1
            
            if (i + 1) % 20 == 0:
                logger.info(f"  进度: {i+1}/{len(batch)} ({success}✅ {fail}❌)")
        
        elapsed = time.time() - t0
        stats, issues = validate_batch(coll, batch)
        
        logger.info(f"\n--- 批次 {batch_num} 验证 ---")
        logger.info(f"  处理: {stats['total']}只 | 耗时: {elapsed:.0f}秒 ({elapsed/60:.1f}分钟)")
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
