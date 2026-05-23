"""Stock sector classification mapping service."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd
from pymongo import UpdateOne

if TYPE_CHECKING:
    from motor.motor_asyncio import AsyncIOMotorDatabase
    from tradingagents.dataflows.providers.china.tushare import TushareProvider

logger = logging.getLogger(__name__)

COLLECTION_NAME = "stock_sector_info"


@dataclass
class SectorInfo:
    """股票行业分类映射。"""

    code: str
    symbol: str
    full_symbol: str
    name: str
    classify_system: str
    l1_code: str
    l1_name: str
    l2_code: Optional[str] = None
    l2_name: Optional[str] = None
    l3_code: Optional[str] = None
    l3_name: Optional[str] = None
    datasource: str = "tushare"
    update_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class StockSectorInfoService:
    """股票行业分类映射服务。"""

    def __init__(
        self,
        db: Optional["AsyncIOMotorDatabase"] = None,
        provider: Optional["TushareProvider"] = None,
    ) -> None:
        if db is None:
            from app.core.database import get_mongo_db

            db = get_mongo_db()
        self.db = db
        self.collection = self.db[COLLECTION_NAME]
        if provider is None:
            from tradingagents.dataflows.providers.china.tushare import TushareProvider

            provider = TushareProvider()
        self.provider = provider
        self._indexes_ensured = False

    async def ensure_indexes(self) -> None:
        """Create indexes required by stock_sector_info."""
        if self._indexes_ensured:
            return

        await self.collection.create_index(
            [("full_symbol", 1), ("classify_system", 1)],
            unique=True,
            name="uk_full_symbol_classify_system",
            background=True,
        )
        await self.collection.create_index(
            [("classify_system", 1), ("l1_code", 1)],
            name="idx_classify_l1",
            background=True,
        )
        await self.collection.create_index([("l2_code", 1)], name="idx_l2_code", background=True)
        await self.collection.create_index([("l3_code", 1)], name="idx_l3_code", background=True)
        await self.collection.create_index([("full_symbol", 1)], name="idx_full_symbol", background=True)
        self._indexes_ensured = True

    async def sync_from_tushare(self, classify_system: str = "SW", force: bool = False) -> Dict[str, Any]:
        """从 Tushare index_member_all 按申万一级行业全量同步行业分类数据。"""
        started_at = datetime.utcnow()
        tushare_src = self._resolve_tushare_classify_src(classify_system)
        try:
            await self.ensure_indexes()
            await self._ensure_provider_connected()
            classify_df = await self._fetch_tushare_l1_classify(tushare_src)
        except Exception as exc:
            logger.exception("Tushare stock sector sync failed: %s", exc)
            return {
                "status": "failed",
                "total": 0,
                "total_unique_stocks": 0,
                "l1_count": 0,
                "fetched_rows": 0,
                "duplicate_count": 0,
                "inserted": 0,
                "updated": 0,
                "errors": 1,
                "message": str(exc),
                "started_at": started_at.isoformat(),
                "finished_at": datetime.utcnow().isoformat(),
            }

        self._log_tushare_classify_debug(classify_df, classify_system, tushare_src)
        if classify_df is None or getattr(classify_df, "empty", True):
            return {
                "status": "success",
                "total": 0,
                "total_unique_stocks": 0,
                "l1_count": 0,
                "fetched_rows": 0,
                "duplicate_count": 0,
                "inserted": 0,
                "updated": 0,
                "errors": 0,
                "message": f"Tushare returned empty index_classify L1 result (src={tushare_src})",
                "started_at": started_at.isoformat(),
                "finished_at": datetime.utcnow().isoformat(),
            }

        if "index_code" not in classify_df.columns:
            return {
                "status": "failed",
                "total": 0,
                "total_unique_stocks": 0,
                "l1_count": 0,
                "fetched_rows": 0,
                "duplicate_count": 0,
                "inserted": 0,
                "updated": 0,
                "errors": 1,
                "message": "Tushare index_classify L1 result missing index_code column",
                "started_at": started_at.isoformat(),
                "finished_at": datetime.utcnow().isoformat(),
            }

        l1_codes = list(
            dict.fromkeys(
                code
                for code in (self._clean_str(value) for value in classify_df.get("index_code", []))
                if code
            )
        )
        all_records: List[Dict[str, Any]] = []
        l1_errors = 0
        for l1_code in l1_codes:
            try:
                member_df = await self._fetch_tushare_index_members(l1_code)
            except Exception as exc:
                l1_errors += 1
                logger.exception("Fetch Tushare index members failed for L1 %s: %s", l1_code, exc)
                continue
            if member_df is not None and not getattr(member_df, "empty", True):
                all_records.extend(member_df.to_dict("records"))

        unique_records = self._deduplicate_tushare_records(all_records)
        now = datetime.utcnow()
        operations = [
            UpdateOne(
                {"full_symbol": doc["full_symbol"], "classify_system": doc["classify_system"]},
                {"$set": doc},
                upsert=True,
            )
            for doc in (
                self._standardize_tushare_row(row, classify_system, now)
                for row in unique_records
            )
            if doc["full_symbol"]
        ]

        inserted = 0
        updated = 0
        errors = l1_errors
        for start in range(0, len(operations), 1000):
            batch = operations[start : start + 1000]
            if not batch:
                continue
            try:
                result = await self.collection.bulk_write(batch, ordered=False)
                inserted += len(result.upserted_ids or {})
                updated += result.modified_count or 0
            except Exception as exc:
                errors += len(batch)
                logger.exception("Bulk upsert stock sector info failed: %s", exc)

        return {
            "status": "success" if errors == 0 else "success_with_errors",
            "total": len(operations),
            "total_unique_stocks": len(operations),
            "l1_count": len(l1_codes),
            "fetched_rows": len(all_records),
            "duplicate_count": max(len(all_records) - len(unique_records), 0),
            "l1_errors": l1_errors,
            "inserted": inserted,
            "updated": updated,
            "errors": errors,
            "message": "",
            "started_at": started_at.isoformat(),
            "finished_at": datetime.utcnow().isoformat(),
        }

    async def get_sector_by_symbol(
        self,
        full_symbol: str,
        classify_system: str = "SW",
    ) -> Optional[SectorInfo]:
        """获取单只股票的行业分类。"""
        doc = await self.collection.find_one(
            {
                "full_symbol": self._normalize_full_symbol(full_symbol),
                "classify_system": classify_system,
            }
        )
        return self._to_sector_info(doc) if doc else None

    async def get_stocks_by_industry(
        self,
        l1_code: Optional[str] = None,
        l2_code: Optional[str] = None,
        l3_code: Optional[str] = None,
        classify_system: str = "SW",
        limit: int = 1000,
    ) -> List[SectorInfo]:
        """按行业获取股票列表。"""
        query: Dict[str, Any] = {"classify_system": classify_system}
        if l1_code:
            query["l1_code"] = l1_code
        if l2_code:
            query["l2_code"] = l2_code
        if l3_code:
            query["l3_code"] = l3_code
        if len(query) == 1:
            raise ValueError("At least one industry code is required")

        cursor = self.collection.find(query).sort("full_symbol", 1).limit(limit)
        docs = await cursor.to_list(length=limit)
        return [sector for sector in (self._to_sector_info(doc) for doc in docs) if sector is not None]

    async def _ensure_provider_connected(self) -> None:
        if self.provider.is_available():
            return
        connected = await self.provider.connect()
        if not connected:
            raise RuntimeError("Tushare provider is not available")

    async def _fetch_tushare_l1_classify(self, tushare_src: str):
        index_classify = getattr(self.provider.api, "index_classify", None)
        if index_classify is None:
            logger.warning("Tushare API object has no index_classify method; falling back to index_member_all")
            return pd.DataFrame([{"index_code": "__all__"}])
        return await asyncio.to_thread(index_classify, level="L1", src=tushare_src)

    async def _fetch_tushare_index_members(self, l1_code: str):
        kwargs = {"is_new": "Y"}
        if l1_code and l1_code != "__all__":
            kwargs["l1_code"] = l1_code
        return await asyncio.to_thread(self.provider.api.index_member_all, **kwargs)

    def _log_tushare_classify_debug(self, classify_df, classify_system: str, tushare_src: str) -> None:
        if classify_df is None:
            logger.info(
                "Tushare index_classify L1 debug: classify_system=%s src=%s result=None",
                classify_system,
                tushare_src,
            )
            return

        columns = list(getattr(classify_df, "columns", []))
        shape = getattr(classify_df, "shape", None)
        try:
            preview = classify_df.head(5).to_dict("records")
        except Exception:
            preview = "<preview unavailable>"
        logger.info(
            "Tushare index_classify L1 debug: classify_system=%s src=%s shape=%s columns=%s "
            "has_index_code=%s preview=%s",
            classify_system,
            tushare_src,
            shape,
            columns,
            "index_code" in columns,
            preview,
        )

    @staticmethod
    def _resolve_tushare_classify_src(classify_system: str) -> str:
        """Tushare 当前申万行业接口使用 SW2021；SW 已返回空结果。"""
        value = str(classify_system or "SW").strip().upper()
        return "SW2021" if value == "SW" else value

    def _standardize_tushare_row(
        self,
        row: Dict[str, Any],
        classify_system: str,
        update_at: datetime,
    ) -> Dict[str, Any]:
        full_symbol = self._clean_str(row.get("ts_code"))
        code = full_symbol.split(".")[0] if "." in full_symbol else full_symbol
        return {
            "full_symbol": full_symbol,
            "classify_system": classify_system,
            "code": code,
            "symbol": code,
            "name": self._clean_str(row.get("name")),
            "l1_code": self._clean_str(row.get("l1_code")),
            "l1_name": self._clean_str(row.get("l1_name")),
            "l2_code": self._clean_optional_str(row.get("l2_code")),
            "l2_name": self._clean_optional_str(row.get("l2_name")),
            "l3_code": self._clean_optional_str(row.get("l3_code")),
            "l3_name": self._clean_optional_str(row.get("l3_name")),
            "datasource": "tushare",
            "update_at": update_at,
        }

    def _deduplicate_tushare_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """按 ts_code 去重，保留日期更新、行业层级更完整的记录。"""
        unique_records: Dict[str, Dict[str, Any]] = {}
        for record in records:
            ts_code = self._clean_str(record.get("ts_code"))
            if not ts_code:
                continue
            current = unique_records.get(ts_code)
            if current is None or self._is_newer_tushare_record(record, current):
                unique_records[ts_code] = record
        return list(unique_records.values())

    def _is_newer_tushare_record(self, candidate: Dict[str, Any], existing: Dict[str, Any]) -> bool:
        candidate_date = self._latest_tushare_record_date(candidate)
        existing_date = self._latest_tushare_record_date(existing)
        if candidate_date != existing_date:
            return candidate_date > existing_date
        candidate_depth = sum(bool(self._clean_str(candidate.get(key))) for key in ("l1_code", "l2_code", "l3_code"))
        existing_depth = sum(bool(self._clean_str(existing.get(key))) for key in ("l1_code", "l2_code", "l3_code"))
        return candidate_depth > existing_depth

    def _latest_tushare_record_date(self, record: Dict[str, Any]) -> str:
        date_values = (
            self._clean_str(record.get(key)).replace("-", "")
            for key in ("update_date", "trade_date", "in_date", "out_date")
        )
        return max((value for value in date_values if value), default="")

    def _to_sector_info(self, doc: Dict[str, Any]) -> Optional[SectorInfo]:
        if not doc:
            return None
        return SectorInfo(
            code=doc.get("code", ""),
            symbol=doc.get("symbol") or doc.get("code", ""),
            full_symbol=doc.get("full_symbol", ""),
            name=doc.get("name", ""),
            classify_system=doc.get("classify_system", "SW"),
            l1_code=doc.get("l1_code", ""),
            l1_name=doc.get("l1_name", ""),
            l2_code=doc.get("l2_code"),
            l2_name=doc.get("l2_name"),
            l3_code=doc.get("l3_code"),
            l3_name=doc.get("l3_name"),
            datasource=doc.get("datasource", "tushare"),
            update_at=doc.get("update_at"),
        )

    def _normalize_full_symbol(self, full_symbol: str) -> str:
        value = str(full_symbol or "").strip().upper()
        if value.endswith(".SS"):
            return f"{value[:-3]}.SH"
        if "." in value:
            return value
        return self.provider._normalize_ts_code(value)

    @staticmethod
    def _clean_str(value: Any) -> str:
        if value is None:
            return ""
        try:
            if value != value:
                return ""
        except Exception:
            pass
        return str(value).strip()

    @classmethod
    def _clean_optional_str(cls, value: Any) -> Optional[str]:
        cleaned = cls._clean_str(value)
        return cleaned or None


_stock_sector_info_service: Optional[StockSectorInfoService] = None


def get_stock_sector_info_service() -> StockSectorInfoService:
    """Return singleton stock sector info service."""
    global _stock_sector_info_service
    if _stock_sector_info_service is None:
        _stock_sector_info_service = StockSectorInfoService()
    return _stock_sector_info_service


async def run_stock_sector_info_sync(classify_system: str = "SW") -> Dict[str, Any]:
    """APScheduler entrypoint for stock sector info sync."""
    service = get_stock_sector_info_service()
    result = await service.sync_from_tushare(classify_system=classify_system)
    logger.info("Stock sector info sync finished: %s", result)
    return result
