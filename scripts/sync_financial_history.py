#!/usr/bin/env python3
"""
财务数据历史补全脚本
=====================

修复 save_financial_data 中 raw_data 未写入的问题，对所有数据不足的股票进行全量历史回补。

设计原则:
- 以 stock_financial_data 为准，避免 stock_basic_info 中已退市/重复股票干扰
- 分 20 批执行，每批 ~272 只
- 每批完成后自动暂停，等用户确认后再继续
- 遵守 Tushare 2000积分限制（160次/分钟）
- 自动跳过已充足股票（income >= 50 条）

用法:
    python sync_financial_history.py --full      # 全量回补（limit=500）
    python sync_financial_history.py --resume    # 从上次的断点继续
    python sync_financial_history.py --incremental  # 增量同步（limit=8）
    python sync_financial_history.py --dry-run   # 只检查，不写入
"""

import asyncio
import logging
import sys
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional, Set
import argparse

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tradingagents.dataflows.providers.china.tushare import TushareProvider
from pymongo import MongoClient

# ── 配置 ──────────────────────────────────────────────
MONGODB_URI = 'mongodb://myq:6812345@172.25.240.1:27017/'
MONGODB_DB = 'tradingagents'
COLLECTION = 'stock_financial_data'

# 同步参数
HISTORY_LIMIT = 500        # 全量回补：取最新500期
INCREMENTAL_LIMIT = 8     # 增量同步：只取最近几期
START_DATE = '19900101'

# 数据质量阈值
INCOME_THRESHOLD = 50     # income >= 50 条视为充足

# Tushare 2000积分档位（basic tier = 200次/分，安全边际80%）
CALLS_PER_MINUTE = 160
TOTAL_BATCHES = 20        # 分成20批

# ─────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class FinancialHistorySyncer:
    def __init__(self, dry_run: bool = False, auto_continue: bool = False):
        self.dry_run = dry_run
        self.auto_continue = auto_continue
        self.mongo_client = MongoClient(MONGODB_URI)
        self.db = self.mongo_client[MONGODB_DB]
        self.col = self.db[COLLECTION]
        self.tp: Optional[TushareProvider] = None

        # 断点文件
        self.checkpoint_file = Path(__file__).parent / '.sync_checkpoint.txt'

        self.stats = {
            'total': 0,
            'skipped': 0,
            'synced': 0,
            'failed': 0,
            'errors': []
        }

    def load_checkpoint(self) -> Set[str]:
        """加载已处理的股票（断点续传）"""
        if not self.checkpoint_file.exists():
            return set()
        processed = set()
        with open(self.checkpoint_file) as f:
            for line in f:
                sym = line.strip()
                if sym:
                    processed.add(sym)
        if processed:
            logger.info(f"📍 断点续传: 已跳过 {len(processed)} 只已处理股票")
        return processed

    def save_checkpoint(self, symbol: str):
        """保存断点"""
        if self.dry_run:
            return
        with open(self.checkpoint_file, 'a') as f:
            f.write(symbol + '\n')

    def get_candidates(self, processed: Set[str]) -> List[str]:
        """获取需要同步的股票列表（排除已处理和已充足的）"""
        all_symbols = set()
        symbol_data = {}  # symbol -> inc_count

        for doc in self.col.find({'data_source': 'tushare'}, {'symbol': 1, 'raw_data': 1}):
            sym = doc.get('symbol')
            if not sym or sym in all_symbols:
                continue
            all_symbols.add(sym)
            raw = doc.get('raw_data') or {}
            inc = raw.get('income_statement', []) if isinstance(raw, dict) else []
            symbol_data[sym] = len(inc)

        # 过滤：排除已处理 + 已充足的
        candidates = [s for s in all_symbols
                      if s not in processed and symbol_data.get(s, 0) < INCOME_THRESHOLD]

        # 统计
        self.stats['total'] = len(candidates)
        self.stats['skipped'] = len([s for s in all_symbols if symbol_data.get(s, 0) >= INCOME_THRESHOLD])

        logger.info(f"需同步: {len(candidates)} 只 | 已充足跳过: {self.stats['skipped']} 只")
        return candidates

    async def init_provider(self):
        os.environ['TUSHARE_TOKEN'] = '3b633dea5db1ed3a161359154cb19b1d71c786370643d4519f302e6b'
        self.tp = TushareProvider()
        await self.tp.connect()
        logger.info("✅ Tushare Provider 初始化完成")

    def _deep_merge_raw(self, existing: Dict, new: Dict) -> Dict:
        LIST_KEYS = ['income_statement', 'balance_sheet', 'cashflow_statement',
                     'financial_indicators', 'main_business']
        result = (existing.copy() if existing else {})
        for key, value in new.items():
            if key in LIST_KEYS and isinstance(value, list):
                existing_map = {}
                for item in result.get(key, []):
                    if isinstance(item, dict) and 'end_date' in item:
                        existing_map[item['end_date']] = item
                for item in value:
                    if isinstance(item, dict) and 'end_date' in item:
                        ed = item['end_date']
                        if ed not in existing_map:
                            result[key] = result.get(key, []) + [item]
                            existing_map[ed] = item
            else:
                result[key] = value
        return result

    async def sync_stock(self, code: str, limit: int) -> bool:
        try:
            data = await self.tp.get_financial_data(code, limit=limit, start_date=START_DATE)
            if not data:
                logger.warning(f"⚠️ {code}: 无数据返回")
                return False

            # 构建查询条件
            # ⚠️ 用 {symbol, data_source} 作为 upsert key（不加 report_period），
            #    这样每只股票只有一条记录，raw_data 累积所有历史 period
            data_source_val = data.get('data_source', 'tushare')
            filter_doc = {
                'symbol': data.get('symbol'),
                'data_source': data_source_val
            }

            existing = self.col.find_one(filter_doc)
            now = datetime.now(timezone.utc)
            merged = data.copy()

            # 如果已存在，保留原有字段（code, full_symbol, market 等）
            if existing:
                for field in ['code', 'full_symbol', 'market']:
                    if field in existing and existing[field]:
                        merged[field] = existing[field]
                merged['raw_data'] = self._deep_merge_raw(
                    existing.get('raw_data', {}),
                    data.get('raw_data', {})
                )
                merged['updated_at'] = now
                merged['version'] = existing.get('version', 0) + 1
            else:
                merged['created_at'] = now
                merged['updated_at'] = now
                merged['version'] = 1
                # 新文档：从 ts_code 推导 code、full_symbol、market
                ts_code = merged.get('ts_code', '')
                if '.' in ts_code:
                    suffix = ts_code.split('.')[-1]
                    merged['code'] = merged.get('symbol')
                    merged['full_symbol'] = f"{merged.get('symbol')}.{suffix}"
                    merged['market'] = 'CN' if suffix in ('SH', 'SZ', 'BJ') else ('HK' if suffix == 'HK' else 'US')
                else:
                    merged['code'] = merged.get('symbol')
                    merged['full_symbol'] = f"{merged.get('symbol')}.SH"
                    merged['market'] = 'CN'

            if not self.dry_run:
                self.col.replace_one(filter_doc, merged, upsert=True)
                self.save_checkpoint(code)

            verify = self.col.find_one(filter_doc)
            raw_verify = verify.get('raw_data') or {}
            inc_count = len(raw_verify.get('income_statement', [])) if isinstance(raw_verify, dict) else 0
            logger.info(f"✅ {code}: period={data.get('report_period')} inc={inc_count} roe={data.get('roe')}")
            return True

        except Exception as e:
            logger.error(f"❌ {code}: {e}")
            self.stats['errors'].append({'code': code, 'error': str(e)})
            return False

    def validate_batch(self, batch: List[str]) -> Dict:
        """批次完成后数据质量验证"""
        results = {'total': len(batch), 'verified': 0, 'issues': []}
        for sym in batch:
            doc = self.col.find_one(
                {'symbol': sym, 'data_source': 'tushare'},
                sort=[('report_period', -1)]
            )
            if not doc:
                results['issues'].append(f"{sym}: 文档未找到")
                continue
            raw = doc.get('raw_data') or {}
            inc = raw.get('income_statement', []) if isinstance(raw, dict) else []
            outer_rp = doc.get('report_period')
            outer_roe = doc.get('roe')

            # 检查外层字段与 raw_data 一致性
            inc_match = any(item.get('end_date') == outer_rp for item in inc) if inc else False
            if not inc_match:
                results['issues'].append(f"{sym}: 外层period={outer_rp}与raw_data不匹配")
                continue

            # 检查roe来源（应来自financial_indicators）
            fi = raw.get('financial_indicators', []) if isinstance(raw, dict) else []
            fi_match = any(
                item.get('end_date') == outer_rp and item.get('roe') == outer_roe
                for item in fi
            ) if fi else (outer_roe is None)

            if not fi_match and outer_roe is not None:
                results['issues'].append(
                    f"{sym}: roe={outer_roe}与FI表period={outer_rp}不匹配"
                )
                continue

            results['verified'] += 1

        return results

    async def run_full(self, resume: bool = False):
        await self.init_provider()
        processed = set() if not resume else self.load_checkpoint()
        candidates = self.get_candidates(processed)
        self.stats['total'] = len(candidates)

        if not candidates:
            logger.info("✅ 所有股票数据已充足或已处理完毕")
            return

        # 计算每批数量
        batch_size = max(1, len(candidates) // TOTAL_BATCHES)
        logger.info(f"🚀 全量历史回补: {len(candidates)} 只 | 每批 ~{batch_size} 只 | 共 {TOTAL_BATCHES} 批")
        logger.info(f"   速率: {CALLS_PER_MINUTE}次/分钟 | 每批间隔 ~{batch_size * 4 * 60 // CALLS_PER_MINUTE}秒")

        for batch_idx in range(TOTAL_BATCHES):
            start_i = batch_idx * batch_size
            end_i = start_i + batch_size if batch_idx < TOTAL_BATCHES - 1 else len(candidates)
            batch = candidates[start_i:end_i]
            if not batch:
                break

            logger.info(f"\n{'='*60}")
            logger.info(f"📦 批次 {batch_idx + 1}/{TOTAL_BATCHES}: {batch[0]}~{batch[-1]}")
            logger.info(f"   ({start_i+1}~{end_i}/{len(candidates)})")
            logger.info(f"{'='*60}")

            batch_synced = []
            for code in batch:
                ok = await self.sync_stock(code, limit=HISTORY_LIMIT)
                if ok:
                    self.stats['synced'] += 1
                    batch_synced.append(code)
                else:
                    self.stats['failed'] += 1

                # 速率控制
                await asyncio.sleep(60 / CALLS_PER_MINUTE * 4)

            # ── 每批完成后验证 ──
            logger.info(f"\n🔍 批次 {batch_idx + 1} 数据验证中...")
            val = self.validate_batch(batch_synced)
            pct = val['verified'] / val['total'] * 100 if val['total'] > 0 else 0
            logger.info(f"   验证: {val['verified']}/{val['total']} ({pct:.0f}%) ✅")
            if val['issues']:
                for iss in val['issues'][:5]:
                    logger.warning(f"   ⚠️ {iss}")

            self._print_stats()

            if self.dry_run:
                continue

            # 暂停，等用户确认
            if self.auto_continue or self.dry_run:
                logger.info("⏩ 自动继续下一批...")
            else:
                logger.info(f"\n⏸️ 批次 {batch_idx + 1} 完成，已保存 {len(batch_synced)} 只股票的断点")
                logger.info(f"   输入 `c` 继续下一批，或 `q` 退出...")
                try:
                    cmd = input().strip().lower()
                    if cmd == 'q':
                        logger.info("用户退出")
                        return
                    elif cmd == 'c':
                        logger.info("继续下一批...")
                except EOFError:
                    return

        self._print_summary()

    async def run_incremental(self):
        await self.init_provider()
        processed = self.load_checkpoint()
        candidates = self.get_candidates(processed)
        self.stats['total'] = len(candidates)

        if not candidates:
            logger.info("✅ 无需增量同步")
            return

        batch_size = max(1, len(candidates) // TOTAL_BATCHES)
        logger.info(f"🚀 增量同步: {len(candidates)} 只 | 每批 ~{batch_size} 只")

        for batch_idx in range(TOTAL_BATCHES):
            start_i = batch_idx * batch_size
            end_i = start_i + batch_size if batch_idx < TOTAL_BATCHES - 1 else len(candidates)
            batch = candidates[start_i:end_i]
            if not batch:
                break

            logger.info(f"\n📦 批次 {batch_idx+1}/{TOTAL_BATCHES} ({start_i+1}~{end_i})")
            for code in batch:
                ok = await self.sync_stock(code, limit=INCREMENTAL_LIMIT)
                if ok:
                    self.stats['synced'] += 1
                else:
                    self.stats['failed'] += 1
                await asyncio.sleep(60 / CALLS_PER_MINUTE * 4)

            val = self.validate_batch(batch)
            logger.info(f"   验证: {val['verified']}/{val['total']}")
            if val['issues']:
                for iss in val['issues'][:3]:
                    logger.warning(f"   ⚠️ {iss}")

            if batch_idx < TOTAL_BATCHES - 1:
                if self.auto_continue:
                    logger.info("⏩ 自动继续...")
                else:
                    logger.info("⏸️ 批次完成，等待确认...")
                    try:
                        cmd = input().strip().lower()
                        if cmd == 'q':
                            return
                    except EOFError:
                        return

        self._print_summary()

    def _print_stats(self):
        logger.info(f"   累计: 成功={self.stats['synced']} 失败={self.stats['failed']} 跳过={self.stats['skipped']}")

    def _print_summary(self):
        logger.info("\n" + "=" * 60)
        logger.info(f"📊 同步完成")
        logger.info(f"   总计: {self.stats['total']}")
        logger.info(f"   成功: {self.stats['synced']} ✅")
        logger.info(f"   跳过: {self.stats['skipped']} ⏭️")
        logger.info(f"   失败: {self.stats['failed']} ❌")
        if self.stats['errors']:
            for e in self.stats['errors'][:5]:
                logger.info(f"     - {e['code']}: {e['error']}")
        logger.info("=" * 60)


async def main():
    parser = argparse.ArgumentParser(description='Tushare 财务数据历史补全')
    parser.add_argument('--full', action='store_true', help='全量历史回补（limit=500）')
    parser.add_argument('--resume', action='store_true', help='从断点继续（配合--full使用）')
    parser.add_argument('--incremental', action='store_true', help='增量同步（limit=8）')
    parser.add_argument('--auto', action='store_true', help='自动继续，不等待确认')
    parser.add_argument('--dry-run', action='store_true', help='仅检查，不写入数据库')
    args = parser.parse_args()

    syncer = FinancialHistorySyncer(dry_run=args.dry_run, auto_continue=args.auto)
    if args.dry_run:
        logger.info("🔍 Dry Run 模式：不写入数据库")

    if args.full:
        await syncer.run_full(resume=args.resume)
    elif args.incremental:
        await syncer.run_incremental()
    else:
        parser.print_help()


if __name__ == '__main__':
    asyncio.run(main())
