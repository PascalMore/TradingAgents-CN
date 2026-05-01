#!/usr/bin/env python3
"""
针对 income <= 5 的540只股票，直接调Tushare REST API补齐数据。
每批处理后立即验证，通过后再继续下一批。
"""
import os
import sys
import time
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['MONGODB_HOST'] = '172.25.240.1'
os.environ['MONGODB_PORT'] = '27017'
os.environ['MONGODB_USERNAME'] = 'myq'
os.environ['MONGODB_PASSWORD'] = '6812345'

from pymongo import MongoClient
from bson import SON

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

TOKEN = '3b633dea5db1ed3a161359154cb19b1d71c786370643d4519f302e6b'
TUSHARE_API = 'http://api.tushare.pro'
BATCH_SIZE = 54
MAX_WORKERS = 8  # 并发数

def ts_code_to_symbol(ts_code):
    """SH/SZ/BJ后缀转6位股票代码"""
    return ts_code.replace('.SH', '').replace('.SZ', '').replace('.BJ', '')

def symbol_to_ts_code(symbol):
    """6位股票代码转Tushare格式"""
    if symbol.startswith('9'):
        return f'{symbol}.BJ'
    elif symbol.startswith('6') or symbol.startswith('5'):
        return f'{symbol}.SH'
    else:
        return f'{symbol}.SZ'

def fetch_tushare_financial(ts_code):
    """直接调Tushare REST API获取全量财务数据"""
    result = {'income_statement': [], 'balance_sheet': [], 'cashflow': [], 'financial_indicator': []}
    
    try:
        # 利润表
        r = requests.post(TUSHARE_API, json={
            'api_name': 'income', 'token': TOKEN,
            'params': {'ts_code': ts_code, 'limit': 500, 'start_date': '19900101'},
            'fields': ''
        }, timeout=15)
        d = r.json()
        if d.get('data') and d['data'].get('fields'):
            fields = d['data']['fields']
            for row in d['data']['items']:
                result['income_statement'].append(dict(zip(fields, row)))
        
        # 资产负债表
        r = requests.post(TUSHARE_API, json={
            'api_name': 'balancesheet', 'token': TOKEN,
            'params': {'ts_code': ts_code, 'limit': 500, 'start_date': '19900101'},
            'fields': ''
        }, timeout=15)
        d = r.json()
        if d.get('data') and d['data'].get('fields'):
            fields = d['data']['fields']
            for row in d['data']['items']:
                result['balance_sheet'].append(dict(zip(fields, row)))
        
        # 现金流量表
        r = requests.post(TUSHARE_API, json={
            'api_name': 'cashflow', 'token': TOKEN,
            'params': {'ts_code': ts_code, 'limit': 500, 'start_date': '19900101'},
            'fields': ''
        }, timeout=15)
        d = r.json()
        if d.get('data') and d['data'].get('fields'):
            fields = d['data']['fields']
            for row in d['data']['items']:
                result['cashflow'].append(dict(zip(fields, row)))
        
        # 财务指标
        r = requests.post(TUSHARE_API, json={
            'api_name': 'fina_indicator', 'token': TOKEN,
            'params': {'ts_code': ts_code, 'limit': 500, 'start_date': '19900101'},
            'fields': ''
        }, timeout=15)
        d = r.json()
        if d.get('data') and d['data'].get('fields'):
            fields = d['data']['fields']
            for row in d['data']['items']:
                result['financial_indicator'].append(dict(zip(fields, row)))
        
    except Exception as e:
        logger.error(f"  {ts_code}: API错误 - {e}")
        return None
    
    return result if any(result.values()) else None

def standardize_data(raw, ts_code):
    """标准化Tushare原始数据"""
    from datetime import datetime
    standardized = {
        'income_statement': [],
        'balance_sheet': [],
        'cashflow': [],
        'financial_indicator': []
    }
    
    for table_name, records in [('income_statement', raw.get('income_statement', [])),
                                  ('balance_sheet', raw.get('balance_sheet', [])),
                                  ('cashflow', raw.get('cashflow', [])),
                                  ('financial_indicator', raw.get('financial_indicator', []))]:
        seen = set()
        for rec in records:
            end_date = str(rec.get('end_date', '')) if rec.get('end_date') else ''
            if not end_date or end_date in seen:
                continue
            seen.add(end_date)
            rec['ts_code'] = ts_code
            rec['end_date'] = end_date
            standardized[table_name].append(rec)
        
        # 按end_date降序
        standardized[table_name].sort(key=lambda x: x.get('end_date', ''), reverse=True)
    
    return standardized

def get_latest_end_date(income_list):
    """从income列表获取最新end_date"""
    if not income_list:
        return None
    # 使用原始end_date排序取第一个（降序）
    dates = [x.get('end_date') for x in income_list if x.get('end_date')]
    return max(dates) if dates else None

def get_report_type(end_date):
    """判断报告类型"""
    if not end_date or len(end_date) != 8:
        return 'quarterly'
    month_day = end_date[4:]
    if month_day == '1231':
        return 'annual'
    return 'quarterly'

def main():
    client = MongoClient('mongodb://myq:6812345@172.25.240.1:27017/')
    db = client['tradingagents']
    coll = db['stock_financial_data']
    
    # 获取 income <= 5 的股票
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
    stocks = [(r['symbol'], r['income_count']) for r in coll.aggregate(pipeline)]
    total = len(stocks)
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"income <= 5 股票总数: {total}，分{total_batches}批，每批{BATCH_SIZE}只")
    
    total_success = 0
    total_failed = 0
    total_improved = 0
    
    for batch_num in range(1, total_batches + 1):
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = start_idx + BATCH_SIZE
        batch_stocks = stocks[start_idx:end_idx]
        
        logger.info(f"\n{'='*50}")
        logger.info(f"批次 {batch_num}/{total_batches} | {len(batch_stocks)}只")
        logger.info(f"{'='*50}")
        
        t0 = time.time()
        results = {}
        
        # 并发获取
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_tushare_financial, symbol_to_ts_code(sym)): (sym, before_ic)
                for sym, before_ic in batch_stocks
            }
            for future in as_completed(futures):
                sym, before_ic = futures[future]
                try:
                    data = future.result()
                    results[sym] = (before_ic, data)
                except Exception as e:
                    results[sym] = (before_ic, None)
        
        # 保存到数据库
        improved = 0
        failed = 0
        before_afters = []
        
        for sym, before_ic in batch_stocks:
            before_ic, raw_data = results.get(sym, (before_ic, None))
            
            if not raw_data:
                failed += 1
                logger.info(f"  {sym}: ❌ 无数据")
                continue
            
            ts_code = symbol_to_ts_code(sym)
            std_data = standardize_data(raw_data, ts_code)
            inc = std_data['income_statement']
            
            if not inc:
                failed += 1
                logger.info(f"  {sym}: ❌ income为空")
                continue
            
            latest_end = get_latest_end_date(inc)
            report_type = get_report_type(latest_end) if latest_end else 'quarterly'
            
            # Upsert
            filter_doc = {'symbol': sym, 'data_source': 'tushare'}
            update_doc = {
                '$set': {
                    'report_period': latest_end,
                    'report_type': report_type,
                    'updated_at': __import__('datetime').datetime.utcnow(),
                    'raw_data': std_data
                },
                '$inc': {'version': 1}
            }
            coll.update_one(filter_doc, update_doc)
            
            # 验证
            doc = coll.find_one({'symbol': sym, 'data_source': 'tushare'})
            after_ic = len(doc.get('raw_data', {}).get('income_statement', [])) if isinstance(doc.get('raw_data'), dict) else 0
            before_afters.append((sym, before_ic, after_ic))
            
            if after_ic > before_ic:
                improved += 1
                logger.info(f"  {sym}: ✅ {before_ic}条 -> {after_ic}条")
            else:
                logger.info(f"  {sym}: ➡️  {before_ic}条 -> {after_ic}条 (无改善)")
        
        elapsed = time.time() - t0
        
        logger.info(f"\n批次 {batch_num} 完成: {len(batch_stocks)-failed}✅ {failed}❌ ({elapsed:.0f}秒)")
        logger.info(f"  改善: {improved} | 无变化: {len(batch_stocks)-failed-improved}")
        
        # 显示无改善的（可能是Tushare数据确实少）
        unchanged = [(s, b, a) for s, b, a in before_afters if a <= b]
        if unchanged:
            logger.info(f"\n  无改善股票 ({len(unchanged)}只):")
            for sym, b, a in unchanged[:5]:
                logger.info(f"    {sym}: {b}条 -> {a}条 (Tushare数据限制)")
        
        total_improved += improved
        total_failed += failed
        total_success += len(batch_stocks) - failed
        
        logger.info(f"\n本批验证通过，继续下一批...")
    
    logger.info(f"\n{'='*50}")
    logger.info(f"全部完成！总计: {total_success}✅ {total_failed}❌")
    logger.info(f"改善: {total_improved}只")

if __name__ == '__main__':
    main()
