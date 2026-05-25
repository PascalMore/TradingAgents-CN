#!/usr/bin/env python3
"""
补指数历史数据脚本 v2 - 直接从Tushare API获取数据并更新MongoDB
"""
import sys
import os
from datetime import date, timedelta
from pathlib import Path
import time

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from pymongo import MongoClient
import tushare as ts


def get_ts_pro():
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        try:
            client = MongoClient('mongodb://myq:6812345@172.25.240.1:27017/admin?authSource=admin', serverSelectionTimeoutMS=5000)
            doc = client['tradingagents'].llm_providers.find_one({'type': 'tushare', 'is_active': True})
            if doc and doc.get('api_key'):
                token = doc['api_key']
        except:
            pass
    
    if token:
        ts.set_token(token)
        return ts.pro_api()
    return ts.pro_api()


def parse_date(date_str):
    """解析日期字符串为date对象"""
    if not date_str:
        return None
    if len(date_str) == 8 and date_str.isdigit():
        return date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    elif '-' in date_str:
        parts = date_str.split('-')
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    return None


def format_date(d):
    """格式化日期为 YYYY-MM-DD 字符串"""
    return d.strftime('%Y-%m-%d')


def backfill_index_data():
    print("=" * 60)
    print("A股指数历史数据补数据脚本 v2")
    print("=" * 60)
    
    client = MongoClient('mongodb://myq:6812345@172.25.240.1:27017/admin?authSource=admin', serverSelectionTimeoutMS=10000)
    db = client['tradingagents']
    
    pro = get_ts_pro()
    
    existing_codes = set(db.index_daily_quotes.distinct('code'))
    print(f"已有K线的指数: {len(existing_codes)}")
    
    indices = list(db.index_basic_info.find(
        {'market': {'$in': ['上证指数', '深证指数']}, 'code': {'$in': list(existing_codes)}},
        {'code': 1, 'name': 1, 'full_symbol': 1}
    ))
    print(f"需要检查更新的指数: {len(indices)}")
    
    # 计算日期范围 - 从数据库最新日期的下一天开始
    last_date = db.index_daily_quotes.find_one(sort=[('trade_date', -1)])
    if last_date:
        last_date_obj = parse_date(last_date['trade_date'])
        if last_date_obj:
            start_date_obj = last_date_obj + timedelta(days=1)
        else:
            start_date_obj = date(2026, 2, 14)
    else:
        start_date_obj = date(2026, 2, 14)
    
    today_obj = date.today()
    start_date = format_date(start_date_obj)
    end_date = format_date(today_obj)
    
    print(f"数据范围: {start_date} ~ {end_date}")
    
    # 计算缺失交易日
    missing = 0
    check_date = start_date_obj
    while check_date <= today_obj:
        if check_date.weekday() < 5:
            missing += 1
        check_date += timedelta(days=1)
    print(f"需要补的交易日约: {missing} 个工作日")
    print()
    
    updated = 0
    errors = 0
    total_records = 0
    skipped = 0
    
    batch_size = 50
    for i in range(0, len(indices), batch_size):
        batch = indices[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(indices) - 1) // batch_size + 1
        print(f"处理批次 {batch_num}/{total_batches} ({len(batch)} 个指数)...")
        
        for idx in batch:
            code = idx.get('code')
            name = idx.get('name', code)
            full_symbol = idx.get('full_symbol', code + '.SH')
            
            try:
                if full_symbol.endswith('.SH'):
                    ts_code = code + '.SH'
                else:
                    ts_code = code + '.SZ'
                
                df = pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                
                if df is not None and len(df) > 0:
                    records = []
                    for _, row in df.iterrows():
                        # 保持 YYYY-MM-DD 格式，与历史数据一致
                        td = str(row['trade_date'])
                        if len(td) == 10 and '-' in td:
                            trade_date = td  # 已经是 YYYY-MM-DD 格式
                        elif len(td) == 8:
                            trade_date = f'{td[:4]}-{td[4:6]}-{td[6:8]}'  # YYYYMMDD -> YYYY-MM-DD
                        else:
                            trade_date = td[:10]
                        records.append({
                            'symbol': code,
                            'code': code,
                            'full_symbol': full_symbol,
                            'market': 'CN',
                            'trade_date': trade_date,
                            'period': 'daily',
                            'data_source': 'tushare',
                            'open': float(row['open']),
                            'high': float(row['high']),
                            'low': float(row['low']),
                            'close': float(row['close']),
                            'pre_close': float(row['pre_close']),
                            'volume': float(row['vol']),
                            'amount': float(row['amount']) if 'amount' in row else 0,
                            'change': float(row['change']) if 'change' in row else 0,
                            'pct_chg': float(row['pct_chg']) if 'pct_chg' in row else 0,
                        })
                    
                    if records:
                        try:
                            result = db.index_daily_quotes.insert_many(records, ordered=False)
                            total_records += len(result.inserted_ids)
                            updated += 1
                        except Exception as insert_err:
                            if 'duplicate' in str(insert_err).lower():
                                skipped += len(records)
                            else:
                                errors += 1
                                if errors <= 3:
                                    print(f"    Insert error {code}: {str(insert_err)[:60]}")
                        
                time.sleep(0.5)
                
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"    Error {code} {name}: {str(e)[:60]}")
        
        time.sleep(2)
    
    print()
    print("=" * 60)
    print(f"指数历史数据同步完成!")
    print(f"更新指数数: {updated}")
    print(f"跳过(已存在): {skipped}")
    print(f"失败指数数: {errors}")
    print(f"新增记录数: {total_records}")
    print("=" * 60)
    
    new_latest = db.index_daily_quotes.find_one(sort=[('trade_date', -1)])
    new_count = db.index_daily_quotes.count_documents({})
    print()
    print("验证:")
    latest_date = new_latest.get('trade_date') if new_latest else 'N/A'
    print(f"  最新日期: {latest_date}")
    print(f"  总记录数: {new_count:,}")


if __name__ == "__main__":
    backfill_index_data()
