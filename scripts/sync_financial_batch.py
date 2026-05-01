#!/usr/bin/env python3
"""
分批财务数据全量同步 + 验证脚本
================================
对 income_count <= 20 的股票执行 --full 同步，每批完成后验证数据质量。

用法:
    python sync_financial_batch.py --batch 1    # 执行第1批
    python sync_financial_batch.py --batch 2    # 执行第2批
    python sync_financial_batch.py --list       # 只列出批次信息，不执行
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Set

# 加载 .env
dotenv_path = Path(__file__).parent.parent / ".env"
if dotenv_path.exists():
    from dotenv import load_dotenv
    load_dotenv(dotenv_path)

MONGODB_URI = "mongodb://myq:6812345@172.25.240.1:27017/"
MONGODB_DB = "tradingagents"
COLLECTION = "stock_financial_data"
HISTORY_LIMIT = 500
START_DATE = "19900101"
TOTAL_BATCHES = 10

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

    def get_candidates(self) -> List[str]:
        """获取 income_count <= 20 的所有股票，分批返回"""
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
            {"$match": {"income_count": {"$lte": 20}}},
            {"$sort": {"income_count": 1}}
        ]
        results = list(self.col.aggregate(pipeline))
        symbols = [r["symbol"] for r in results]
        total = len(symbols)
        batch_size = (total + TOTAL_BATCHES - 1) // TOTAL_BATCHES
        start = (self.batch_num - 1) * batch_size
        end = min(start + batch_size, total)
        return symbols[start:end]

    def _deep_merge_raw(self, existing: Dict, new: Dict) -> Dict:
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
            filter_doc = {
                "symbol": data.get("symbol"),
                "data_source": data_source_val
            }

            existing = self.col.find_one(filter_doc)
            now = datetime.now(timezone.utc)
            merged = data.copy()

            if existing:
                for field in ["code", "full_symbol", "market"]:
                    if field in existing and existing[field]:
                        merged[field] = existing[field]
                merged["raw_data"] = self._deep_merge_raw(
                    existing.get("raw_data", {}),
                    data.get("raw_data", {})
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
            self.stats["errors"].append({"code": code, "error": str(e)})
            return False

    async def run_batch(self):
        await self.init()
        batch = self.get_candidates()
        if not batch:
            logger.info(f"批次 {self.batch_num}: 无待同步股票")
            return

        logger.info(f"\n{'='*60}")
        logger.info(f"📦 批次 {self.batch_num}/{TOTAL_BATCHES}: {len(batch)} 只股票")
        logger.info(f"   范围: {batch[0]} ~ {batch[-1]}")
        logger.info(f"{'='*60}")

        synced, failed = 0, 0
        for i, code in enumerate(batch):
            ok = await self.sync_stock(code)
            if ok:
                synced += 1
                self.stats["synced"] += 1
            else:
                failed += 1
                self.stats["failed"] += 1

            # 进度输出
            if (i + 1) % 20 == 0 or (i + 1) == len(batch):
                logger.info(f"   进度: {i+1}/{len(batch)} ({synced}✅ {failed}❌)")

            # Tushare 速率限制：160次/分钟，每次调用涉及3个API(income/balance/cashflow)
            # 保守点，每4次调用后休息一下
            if (i + 1) % 4 == 0:
                await asyncio.sleep(60 / 160 * 4)

        logger.info(f"\n批次 {self.batch_num} 完成: {synced}✅ {failed}❌")

        # 验证
        await self.validate_batch(batch)

    async def validate_batch(self, batch: List[str]) -> Dict:
        """验证批次同步结果"""
        logger.info(f"\n🔍 批次 {self.batch_num} 数据验证中...")

        total = len(batch)
        verified_ok = 0
        issues = []

        for sym in batch:
            doc = self.col.find_one({"symbol": sym, "data_source": "tushare"})
            if not doc:
                issues.append(f"{sym}: 文档未找到")
                continue

            raw = doc.get("raw_data") or {}
            inc = raw.get("income_statement", []) if isinstance(raw, dict) else []
            bal = raw.get("balance_sheet", []) if isinstance(raw, dict) else []
            outer_rp = doc.get("report_period")
            inc_count = len(inc)

            # 检查外层 report_period 是否在 raw_data 中存在
            inc_match = any(item.get("end_date") == outer_rp for item in inc) if inc else False
            if not inc_match and outer_rp:
                issues.append(f"{sym}: 外层period={outer_rp}与raw_data不匹配")
                continue

            # 数据质量评估
            if inc_count >= 20:
                verified_ok += 1
            elif inc_count >= 5:
                # 数据偏少但可能正常（新股票、数据源限制）
                verified_ok += 1
            else:
                # 数据异常少
                issues.append(f"{sym}: income仅{inc_count}条，可能Tushare数据不足")

        pct = verified_ok / total * 100 if total > 0 else 0
        logger.info(f"\n{'='*50}")
        logger.info(f"📊 批次 {self.batch_num} 验证结果")
        logger.info(f"   总计: {total} 只")
        logger.info(f"   ✅ 通过: {verified_ok} ({pct:.1f}%)")
        logger.info(f"   ⚠️ 问题: {len(issues)}")
        if issues:
            for iss in issues[:10]:
                logger.warning(f"      {iss}")
        logger.info(f"{'='*50}")

        return {"total": total, "verified": verified_ok, "issues": issues}


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="分批财务数据全量同步")
    parser.add_argument("--batch", type=int, required=True, help="批次号 (1-10)")
    parser.add_argument("--list", action="store_true", help="只列出所有批次信息")
    args = parser.parse_args()

    # 先获取候选列表（不连接Tushare）
    from pymongo import MongoClient
    mc = MongoClient(MONGODB_URI)
    db_tmp = mc[MONGODB_DB]
    col_tmp = db_tmp[COLLECTION]

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
        {"$match": {"income_count": {"$lte": 20}}},
        {"$sort": {"income_count": 1}}
    ]
    results = list(col_tmp.aggregate(pipeline))
    symbols = [r["symbol"] for r in results]
    total = len(symbols)
    batch_size = (total + TOTAL_BATCHES - 1) // TOTAL_BATCHES

    if args.list:
        logger.info(f"income <= 20 的股票总数: {total}")
        for i in range(1, TOTAL_BATCHES + 1):
            start = (i - 1) * batch_size
            end = min(start + batch_size, total)
            logger.info(f"批次{i}: {end - start} 只, 范围 {start+1}-{end}")
        return

    if args.batch < 1 or args.batch > TOTAL_BATCHES:
        logger.error(f"批次号必须为 1-{TOTAL_BATCHES}")
        return

    syncer = BatchSyncer(args.batch)
    await syncer.run_batch()


if __name__ == "__main__":
    asyncio.run(main())
