"""
清理 stock_financial_data.raw_data 中的重复记录

用法:
    python cleanup_financial_duplicates.py [--dry-run] [--batch-size 100]
"""

import argparse
import sys
from pymongo import MongoClient
from collections import Counter

LIST_KEYS = ['income_statement', 'balance_sheet', 'cashflow_statement', 
             'financial_indicators', 'main_business']

def deduplicate_list(items):
    """对列表按 end_date 去重，保留每期最新的一条"""
    if not items:
        return items
    
    # 按 end_date 降序排序后去重（保留第一条，即最新的）
    seen = set()
    result = []
    for item in items:
        date = item.get('end_date')
        if date and date not in seen:
            seen.add(date)
            result.append(item)
    
    return result

def cleanup_document(doc):
    """清理单个文档的 raw_data"""
    raw = doc.get('raw_data', {})
    if not raw:
        return raw
    
    cleaned = {}
    for key, value in raw.items():
        if key in LIST_KEYS and isinstance(value, list):
            cleaned[key] = deduplicate_list(value)
        else:
            cleaned[key] = value
    
    return cleaned

def main():
    parser = argparse.ArgumentParser(description='清理财务数据重复记录')
    parser.add_argument('--dry-run', action='store_true', help='只检查不写入')
    parser.add_argument('--batch-size', type=int, default=100, help='每批处理数量')
    args = parser.parse_args()
    
    # 连接数据库
    client = MongoClient('mongodb://myq:6812345@172.25.240.1:27017/', serverSelectionTimeoutMS=5000)
    db = client['tradingagents']
    coll = db['stock_financial_data']
    
    total_docs = coll.count_documents({})
    print(f'总文档数: {total_docs}')
    
    # 统计清理前重复
    stats_before = {'total_docs': total_docs, 'total_dup_items': 0, 'affected_docs': 0}
    
    processed = 0
    updated = 0
    batch_size = args.batch_size
    
    cursor = coll.find({}, {'symbol': 1, 'raw_data': 1})
    
    for doc in cursor:
        raw = doc.get('raw_data', {})
        if not raw:
            continue
        
        # 统计清理前重复
        total_dup = 0
        for key in LIST_KEYS:
            items = raw.get(key, [])
            if not items:
                continue
            dates = [item.get('end_date') for item in items if isinstance(item, dict)]
            dup = Counter(dates)
            total_dup += sum(count - 1 for count in dup.values() if count > 1)
        
        if total_dup > 0:
            stats_before['total_dup_items'] += total_dup
            stats_before['affected_docs'] += 1
        
        # 清理重复
        cleaned = cleanup_document(raw)
        
        # 检查是否需要更新
        needs_update = False
        for key in LIST_KEYS:
            orig_list = raw.get(key, [])
            clean_list = cleaned.get(key, [])
            if len(orig_list) != len(clean_list):
                needs_update = True
                break
        
        if needs_update:
            if args.dry_run:
                print(f'[DRY-RUN] {doc["symbol"]}: 清理 {sum(len(raw.get(k, [])) - len(cleaned.get(k, [])) for k in LIST_KEYS)} 条重复')
            else:
                coll.update_one(
                    {'_id': doc['_id']},
                    {'$set': {'raw_data': cleaned}}
                )
                updated += 1
        
        processed += 1
        if processed % batch_size == 0:
            print(f'进度: {processed}/{total_docs} (已更新: {updated})')
    
    print()
    print('='*50)
    print(f'清理前: {stats_before["affected_docs"]} 个文档有重复，共 {stats_before["total_dup_items"]} 条重复')
    print(f'处理完成: {processed}/{total_docs}')
    print(f'已更新: {updated} 个文档')
    
    # 验证清理后
    if not args.dry_run:
        print()
        print('验证清理后...')
        total_dup_after = 0
        for doc in coll.find({}, {'raw_data': 1}):
            raw = doc.get('raw_data', {})
            for key in LIST_KEYS:
                items = raw.get(key, [])
                dates = [item.get('end_date') for item in items if isinstance(item, dict)]
                dup = Counter(dates)
                total_dup_after += sum(count - 1 for count in dup.values() if count > 1)
        
        print(f'清理后重复记录数: {total_dup_after}')
        if total_dup_after == 0:
            print('✅ 清理完成，无重复!')
        else:
            print(f'⚠️ 仍有 {total_dup_after} 条重复')

if __name__ == '__main__':
    main()
