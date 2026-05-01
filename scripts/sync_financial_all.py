#!/usr/bin/env python3
"""
股票全量财务数据同步脚本 - 分20批执行 + 验证
==============================================

用法:
    python sync_financial_all.py --batch N    # 执行第N批 (1-20)
    python sync_financial_all.py --list       # 查看批次信息
    python sync_financial_all.py --status      # 查看同步进度
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional
import argparse

# 加载 .env
dotenv_path = Path(__file__).parent.parent / ".env"
if dotenv_path.exists():
    from dotenv import load_dotenv
    load_dotenv(dotenv_path)

MONGODB_URI = "mongodb://myq:6812345@172.25.240.1:27017/tradingagents?authSource=admin"
MONGODB_DB = "tradingagents"
COLLECTION = "stock_financial_data"
HISTORY_LIMIT = 500
START_DATE = "19900101"
TOTAL_BATCHES = 20

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


class BatchSyncer:
    def __init__(self, batch_num: int):
        self.batch_num = batch_num
        self.mongo_client = None
        self.db = None
        self.col = None
        self.tp = None
        self.stats = {"synced": 0, "failed": 0, "errors": []}
        self.batch_symbols = []

    async def init(self):
        from pymongo import MongoClient
        self.mongo_client = MongoClient(MONGODB_URI)
        self.db = self.mongo_client[MONGODB_DB]
        self.col = self.db[COLLECTION]
        os.environ["TUSHARE_TOKEN"] = "3b633dea5db1ed3a161359154cb19b1d71c786370643d4519f302e6b"
        from tradingagents.dataflows.providers.china.tushare import TushareProvider
        self.tp = TushareProvider()
        await self.tp.connect()
        logger.info("✅ Tushare Provider 初始化完成")

    def get_all_symbols(self) -> List[str]:
        """获取所有股票列表（从 stock_basic_info）"""
        symbols = self.db['stock_basic_info'].distinct('symbol')
        return sorted(symbols)

    def get_batch_symbols(self) -> List[str]:
        """获取当前批次的股票列表"""
        all_symbols = self.get_all_symbols()
        total = len(all_symbols)
        batch_size = (total + TOTAL_BATCHES - 1) // TOTAL_BATCHES
        start = (self.batch_num - 1) * batch_size
        end = min(start + batch_size, total)
        self.batch_symbols = all_symbols[start:end]
        return self.batch_symbols

    def _deep_merge_raw(self, existing: Dict, new: Dict) -> Dict:
        """深度合并 raw_data"""
        LIST_KEYS = ["income_statement", "balance_sheet", "cashflow_statement",
                     "financial_indicators", "main_business"]
        result = (existing.copy() if existing else {})
        for key, value in new.items():
            if key in LIST_KEYS and isinstance(value, list):
                existing_map = {}
                for item in result.get(key, []):
                    if isinstance(item, dict) and "end_date" in item:
                        existing_map[item["end_date"]] = item
                for item in value:
                    if isinstance(item, dict) and "end_date" in item:
                        ed = item["end_date"]
                        if ed not in existing_map:
                            result[key] = result.get(key, []) + [item]
                            existing_map[ed] = item
            else:
                result[key] = value
        return result

    async def sync_stock(self, code: str) -> bool:
        try:
            data = await self.tp.get_financial_data(code, limit=HISTORY_LIMIT, start_date=START_DATE)
            if not data:
                logger.warning(f"⚠️ {code}: 无数据返回")
                return False

            data_source_val = data.get("data_source", "tushare")
            filter_doc = {"symbol": data.get("symbol"), "data_source": data_source_val}

            existing = self.col.find_one(filter_doc)
            now = datetime.now(timezone.utc)
            merged = data.copy()

            if existing:
                for field in ["code", "full_symbol", "market"]:
                    if field in existing and existing[field]:
                        merged[field] = existing[field]
                merged["raw_data"] = self._deep_merge_raw(
                    existing.get("raw_data", {}), data.get("raw_data", {})
                )
                merged["updated_at"] = now
                merged["version"] = existing.get("version", 0) + 1
            else:
                merged["created_at"] = now
                merged["updated_at"] = now
                merged["version"] = 1
                ts_code = merged.get("ts_code", "")
                if "." in ts_code:
                    suffix = ts_code.split(".")[-1]
                    merged["code"] = merged.get("symbol")
                    merged["full_symbol"] = f"{merged.get('symbol')}.{suffix}"
                    merged["market"] = "CN" if suffix in ("SH", "SZ", "BJ") else ("HK" if suffix == "HK" else "US")
                else:
                    merged["code"] = merged.get("symbol")
                    merged["full_symbol"] = f"{merged.get('symbol')}.SH"
                    merged["market"] = "CN"

            self.col.replace_one(filter_doc, merged, upsert=True)
            return True

        except Exception as e:
            logger.error(f"❌ {code}: {e}")
            self.stats["errors"].append(f"{code}: {str(e)}")
            return False

    async def run_batch(self) -> Tuple[int, int]:
        """执行当前批次同步"""
        symbols = self.get_batch_symbols()
        logger.info(f"📦 批次 {self.batch_num}/{TOTAL_BATCHES}: {len(symbols)} 只股票")

        # 限制调用频率 160次/分钟，约 9.5 秒间隔
        for i, code in enumerate(symbols):
            success = await self.sync_stock(code)
            if success:
                self.stats["synced"] += 1
            else:
                self.stats["failed"] += 1

            # 每50只报告进度
            if (i + 1) % 50 == 0:
                logger.info(f"  进度: {i+1}/{len(symbols)} (成功:{self.stats['synced']}, 失败:{self.stats['failed']})")

            # 调用间隔控制（约 0.2 秒一次，300次/分钟，安全边际）
            await asyncio.sleep(0.2)

        return self.stats["synced"], self.stats["failed"]


class DataValidator:
    """数据验证器"""

    def __init__(self):
        from pymongo import MongoClient
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[MONGODB_DB]
        self.col = self.db[COLLECTION]

    def validate_batch(self, symbols: List[str]) -> Dict:
        """验证一批股票的数据质量"""
        results = {
            "total": len(symbols),
            "field_validation": {},      # 每条记录的字段空值情况
            "period_mismatch": [],      # 外层与内层 report_period 不一致
            "critical_issues": [],      # 关键问题
            "field_stats": {},          # 字段级别统计
        }

        for sym in symbols:
            doc = self.col.find_one({"symbol": sym, "data_source": "tushare"})
            if not doc:
                results["critical_issues"].append(f"{sym}: 记录不存在")
                continue

            # 1. 字段空值检查
            empty_fields = []
            for field in ["symbol", "ts_code", "report_period", "report_type", "data_source",
                         "raw_data", "version", "created_at", "updated_at"]:
                if field not in doc or doc[field] is None or doc[field] == "":
                    empty_fields.append(field)
            results["field_validation"][sym] = empty_fields

            # 2. 外层 report_period 与内层 raw_data 最新 end_date 一致性检查
            outer_period = doc.get("report_period")
            raw_data = doc.get("raw_data", {})

            # 获取 income_statement 最新 end_date
            income_list = raw_data.get("income_statement", [])
            if income_list and isinstance(income_list[0], dict):
                inner_latest = income_list[0].get("end_date")
                if outer_period != inner_latest:
                    results["period_mismatch"].append({
                        "symbol": sym,
                        "outer_period": outer_period,
                        "inner_latest": inner_latest,
                        "income_count": len(income_list)
                    })

            # 3. raw_data 内部各表的 end_date 一致性检查
            tables = ["income_statement", "balance_sheet", "cashflow_statement",
                     "financial_indicators", "main_business"]
            table_end_dates = {}
            for table in tables:
                table_data = raw_data.get(table, [])
                if table_data and isinstance(table_data[0], dict):
                    table_end_dates[table] = table_data[0].get("end_date")

            # 4. 统计字段非空情况
            for field in ["income_statement", "balance_sheet", "cashflow_statement",
                         "financial_indicators", "main_business"]:
                count = len(raw_data.get(field, []))
                if field not in results["field_stats"]:
                    results["field_stats"][field] = {"empty": 0, "1-10": 0, "11-50": 0, "50+": 0}
                if count == 0:
                    results["field_stats"][field]["empty"] += 1
                elif count <= 10:
                    results["field_stats"][field]["1-10"] += 1
                elif count <= 50:
                    results["field_stats"][field]["11-50"] += 1
                else:
                    results["field_stats"][field]["50+"] += 1

        return results

    def print_validation_report(self, results: Dict, batch_num: int):
        """打印验证报告"""
        print("\n" + "=" * 70)
        print(f"📊 批次 {batch_num} 数据核验报告")
        print("=" * 70)

        # 1. 总体情况
        print(f"\n【1. 字段空值检查】")
        total_empty = sum(len(v) for v in results["field_validation"].values())
        print(f"  总记录数: {results['total']}")
        print(f"  有空字段的记录: {sum(1 for v in results['field_validation'].values() if v)}")
        print(f"  全部字段完整的记录: {sum(1 for v in results['field_validation'].values() if not v)}")

        if total_empty > 0:
            print(f"  ⚠️ 空字段明细:")
            for sym, fields in results["field_validation"].items():
                if fields:
                    print(f"    {sym}: {fields}")

        # 2. 外层与内层 report_period 一致性
        print(f"\n【2. 外层与内层 report_period 一致性】")
        mismatch = results["period_mismatch"]
        print(f"  一致: {results['total'] - len(mismatch)} 条")
        print(f"  不一致: {len(mismatch)} 条")

        if mismatch:
            print(f"  ⚠️ 不一致明细 (前10条):")
            for item in mismatch[:10]:
                print(f"    {item['symbol']}: 外层={item['outer_period']}, 内层={item['inner_latest']}, income={item['income_count']}")

        # 3. raw_data 各表数据量统计
        print(f"\n【3. raw_data 各表数据量统计】")
        for field, stats in results["field_stats"].items():
            print(f"  {field}:")
            print(f"    空(0条): {stats['empty']}, 少量(1-10): {stats['1-10']}, 中量(11-50): {stats['11-50']}, 大量(50+): {stats['50+']}")

        # 4. 关键问题
        print(f"\n【4. 关键问题】")
        if results["critical_issues"]:
            for issue in results["critical_issues"][:10]:
                print(f"  ❌ {issue}")
        else:
            print(f"  ✅ 无关键问题")

        print("\n" + "=" * 70)


async def run_batch(batch_num: int):
    """执行单个批次"""
    syncer = BatchSyncer(batch_num)
    await syncer.init()

    # 执行同步
    logger.info(f"🚀 开始批次 {batch_num} 同步...")
    synced, failed = await syncer.run_batch()
    logger.info(f"📦 批次 {batch_num} 完成: 成功={synced}, 失败={failed}")

    # 验证数据
    logger.info(f"🔍 开始数据核验...")
    validator = DataValidator()
    symbols = syncer.batch_symbols
    results = validator.validate_batch(symbols)
    validator.print_validation_report(results, batch_num)

    return synced, failed, results


def list_batches():
    """列出所有批次信息"""
    from pymongo import MongoClient
    client = MongoClient(MONGODB_URI)
    db = client['tradingagents']
    symbols = sorted(db['stock_basic_info'].distinct('symbol'))
    total = len(symbols)
    batch_size = (total + TOTAL_BATCHES - 1) // TOTAL_BATCHES

    print(f"\n📋 总股票数: {total}, 分 {TOTAL_BATCHES} 批, 每批约 {batch_size} 只")
    print("-" * 50)
    for i in range(1, TOTAL_BATCHES + 1):
        start = (i - 1) * batch_size
        end = min(start + batch_size, total)
        batch_symbols = symbols[start:end]
        print(f"批次 {i:2d}: {symbols[start] if start < total else 'N/A'} - {symbols[end-1] if end <= total else 'N/A'} ({len(batch_symbols)}只)")


def show_status():
    """显示当前同步进度"""
    from pymongo import MongoClient
    client = MongoClient(MONGODB_URI)
    db = client['tradingagents']
    col = db[COLLECTION]

    total_stocks = len(db['stock_basic_info'].distinct('symbol'))
    synced_stocks = len(col.distinct('symbol', {'data_source': 'tushare'}))

    print(f"\n📊 同步进度")
    print("-" * 50)
    print(f"  总股票数: {total_stocks}")
    print(f"  已同步: {synced_stocks}")
    print(f"  未同步: {total_stocks - synced_stocks}")
    print(f"  进度: {synced_stocks/total_stocks*100:.1f}%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="股票全量财务数据同步")
    parser.add_argument("--batch", type=int, help="执行指定批次 (1-20)")
    parser.add_argument("--list", action="store_true", help="列出所有批次")
    parser.add_argument("--status", action="store_true", help="显示同步进度")
    args = parser.parse_args()

    if args.list:
        list_batches()
    elif args.status:
        show_status()
    elif args.batch:
        if args.batch < 1 or args.batch > TOTAL_BATCHES:
            print(f"❌ 批次必须在 1-{TOTAL_BATCHES} 之间")
            sys.exit(1)
        asyncio.run(run_batch(args.batch))
    else:
        parser.print_help()