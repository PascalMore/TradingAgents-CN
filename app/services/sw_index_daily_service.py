"""SW industry index daily quote sync service."""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd
from pymongo import UpdateOne

if TYPE_CHECKING:
    from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger(__name__)

INDEX_DAILY_COLLECTION = "index_daily_quotes"
SECTOR_INFO_COLLECTION = "stock_sector_info"
DATA_SOURCE = "akshare"
PERIOD = "daily"


class SwIndexDailyService:
    """申万一级行业指数日线同步服务。"""

    def __init__(
        self,
        db: Optional["AsyncIOMotorDatabase"] = None,
        ak_module: Any = None,
        concurrency: int = 5,
    ) -> None:
        if db is None:
            from app.core.database import get_mongo_db

            db = get_mongo_db()
        self.db = db
        self.collection = self.db[INDEX_DAILY_COLLECTION]
        self.sector_collection = self.db[SECTOR_INFO_COLLECTION]
        if ak_module is None:
            import akshare as ak_module

        self.ak = ak_module
        self.semaphore = asyncio.Semaphore(concurrency)
        self._indexes_ensured = False

    async def ensure_indexes(self) -> None:
        """Create indexes required by SW index daily quotes (idempotent)."""
        if self._indexes_ensured:
            return
        # Use try/except since create_index is idempotent when index already exists with same definition
        try:
            await self.collection.create_index(
                [("full_symbol", 1), ("trade_date", 1)],
                unique=True,
                name="uk_full_symbol_trade_date",
                background=True,
            )
        except Exception:
            pass  # Index already exists with same or different options
        try:
            await self.collection.create_index([("trade_date", -1)], name="idx_trade_date", background=True)
        except Exception:
            pass  # Index already exists
        try:
            await self.collection.create_index([("code", 1)], name="idx_code", background=True)
        except Exception:
            pass  # Index already exists
        self._indexes_ensured = True

    async def sync_all(self, force_full: bool = False, end_date: str = None) -> Dict[str, Any]:
        """
        并发同步全部申万一级行业指数日线。

        Args:
            force_full: 是否强制全量同步（从2000-01-01开始）。
            end_date: 可选结束日期。
        """
        started_at = datetime.utcnow()
        try:
            await self.ensure_indexes()
            sector_map = await self._get_l1_sector_map()
            if not sector_map:
                return self._result(
                    started_at,
                    status="success",
                    total_symbols=0,
                    total_records=0,
                    inserted=0,
                    updated=0,
                    errors=0,
                    message="No SW L1 sector codes found in stock_sector_info",
                )

            normalized_end = self._format_date(end_date) if end_date else None
            start_date = "2000-01-01" if force_full else None
            tasks = [
                self._sync_one(code, name, start_date, normalized_end, force_full=force_full)
                for code, name in sector_map.items()
            ]
            results = await asyncio.gather(*tasks)
        except Exception as exc:
            logger.exception("SW index daily sync failed: %s", exc)
            return self._result(
                started_at,
                status="failed",
                total_symbols=0,
                total_records=0,
                inserted=0,
                updated=0,
                errors=1,
                message=str(exc),
            )

        errors = sum(1 for item in results if item.get("status") == "failed")
        total_records = sum(item.get("records", 0) for item in results)
        inserted = sum(item.get("inserted", 0) for item in results)
        updated = sum(item.get("updated", 0) for item in results)
        return self._result(
            started_at,
            status="success" if errors == 0 else "success_with_errors",
            total_symbols=len(results),
            total_records=total_records,
            inserted=inserted,
            updated=updated,
            errors=errors,
            message="",
            details=results,
        )

    async def get_drawdown(self, sector_code: str, date: str, window: int = 20) -> float:
        """计算N日回撤，被 Darwin Detector 调用。"""
        code = self._normalize_code(sector_code)
        full_symbol = self._full_symbol(code)
        trade_date = self._format_date(date)
        cursor = (
            self.collection.find(
                {
                    "full_symbol": full_symbol,
                    "period": PERIOD,
                    "trade_date": {"$lte": trade_date},
                },
                {"_id": 0, "trade_date": 1, "close": 1},
            )
            .sort("trade_date", -1)
            .limit(max(int(window), 1))
        )
        docs = await cursor.to_list(length=max(int(window), 1))
        closes = [self._safe_float(doc.get("close")) for doc in docs]
        closes = [value for value in closes if value is not None]
        if not closes:
            return 0.0
        peak = max(closes)
        return round((closes[0] - peak) / peak, 6) if peak else 0.0

    async def get_sector_info(self, sector_code: str) -> Optional[Dict[str, Any]]:
        """获取行业指数基本信息。"""
        code = self._normalize_code(sector_code)
        doc = await self.sector_collection.find_one(
            {"classify_system": "SW", "l1_code": code},
            {"_id": 0, "l1_code": 1, "l1_name": 1},
        )
        if not doc:
            return None
        return {
            "code": code,
            "symbol": code,
            "full_symbol": self._full_symbol(code),
            "market": "CN",
            "name": doc.get("l1_name", ""),
            "classify_system": "SW",
            "data_source": DATA_SOURCE,
        }

    async def _sync_one(
        self,
        code: str,
        name: str,
        start_date: Optional[str],
        end_date: Optional[str],
        force_full: bool = False,
    ) -> Dict[str, Any]:
        async with self.semaphore:
            try:
                normalized_start = start_date or await self._get_sync_start_date(self._full_symbol(code))
                raw_df = await asyncio.to_thread(self.ak.index_hist_sw, symbol=code, period="day")
                records = self._standardize_dataframe(code, name, raw_df, normalized_start, end_date)
                if not records and not force_full and start_date is None:
                    records = await self._fallback_realtime_sw(code, normalized_start)
                if not records:
                    return {"code": code, "status": "success", "records": 0, "inserted": 0, "updated": 0}

                operations = [
                    UpdateOne(
                        {"full_symbol": record["full_symbol"], "trade_date": record["trade_date"]},
                        {"$set": record},
                        upsert=True,
                    )
                    for record in records
                ]
                result = await self.collection.bulk_write(operations, ordered=False)
                inserted = self._result_count(result, "upserted_count", len(getattr(result, "upserted_ids", {}) or {}))
                updated = self._result_count(result, "modified_count", 0)
                return {
                    "code": code,
                    "status": "success",
                    "records": len(records),
                    "inserted": inserted,
                    "updated": updated,
                }
            except Exception as exc:
                logger.exception("Sync SW index daily failed for %s: %s", code, exc)
                return {"code": code, "status": "failed", "records": 0, "inserted": 0, "updated": 0, "error": str(exc)}

    async def _fallback_realtime_sw(self, code: str, trade_date: str) -> List[Dict[str, Any]]:
        """Fallback to realtime SW index data when daily history has not published today's bar."""
        try:
            logger.info("Fallback to index_realtime_sw for %s on %s", code, trade_date)
            realtime_df = await asyncio.to_thread(self.ak.index_realtime_sw, symbol="一级行业")
            if realtime_df is None or getattr(realtime_df, "empty", True):
                return []

            row = next(
                (
                    row
                    for _, row in realtime_df.iterrows()
                    if self._normalize_code(row.get("指数代码")) == self._normalize_code(code)
                ),
                None,
            )
            if row is None:
                return []

            open_price = self._safe_float(row.get("今开盘"))
            high_price = self._safe_float(row.get("最高价"))
            low_price = self._safe_float(row.get("最低价"))
            close_price = self._safe_float(row.get("最新价"))
            pre_close = self._safe_float(row.get("昨收盘"))
            now = datetime.utcnow()
            logger.info("Fallback index_realtime_sw produced daily record for %s on %s", code, trade_date)
            return [
                {
                    "full_symbol": self._full_symbol(code),
                    "code": code,
                    "symbol": code,
                    "market": "CN",
                    "trade_date": trade_date,
                    "period": PERIOD,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "pre_close": pre_close,
                    "volume": self._scale(row.get("成交量"), 10000),
                    "amount": self._scale(row.get("成交额"), 100000000),
                    "change": round(close_price - pre_close, 4)
                    if close_price is not None and pre_close is not None
                    else None,
                    "pct_chg": round((close_price - pre_close) / pre_close * 100, 4)
                    if close_price is not None and pre_close
                    else None,
                    "data_source": DATA_SOURCE,
                    "created_at": now,
                    "updated_at": now,
                    "version": 1,
                }
            ]
        except Exception as fallback_error:
            logger.warning("Fallback index_realtime_sw failed for %s: %s", code, fallback_error)
            return []

    async def _get_sync_start_date(self, full_symbol: str) -> str:
        """获取单个指数的同步起始日期。"""
        latest = await self.collection.find_one(
            {"full_symbol": full_symbol},
            sort=[("trade_date", -1)],
            projection={"trade_date": 1},
        )
        if latest:
            from dateutil.parser import parse

            return (parse(latest["trade_date"]) + timedelta(days=1)).strftime("%Y-%m-%d")
        return "2000-01-01"

    async def _get_l1_sector_map(self) -> Dict[str, str]:
        codes = await self.sector_collection.distinct("l1_code", {"classify_system": "SW"})
        cleaned_codes = [self._normalize_code(code) for code in codes if self._normalize_code(code)]
        if not cleaned_codes:
            return {}

        # stock_sector_info may store l1_code with or without .SI suffix.
        l1_codes_for_query = [code for code in cleaned_codes] + [f"{code}.SI" for code in cleaned_codes]
        cursor = self.sector_collection.find(
            {"classify_system": "SW", "l1_code": {"$in": l1_codes_for_query}},
            {"_id": 0, "l1_code": 1, "l1_name": 1},
        )
        docs = await cursor.to_list(length=None)
        sector_map: Dict[str, str] = {}
        for doc in docs:
            code = self._normalize_code(doc.get("l1_code"))
            if code and code not in sector_map:
                sector_map[code] = self._clean_str(doc.get("l1_name"))
        return {code: sector_map.get(code, "") for code in cleaned_codes}

    def _standardize_dataframe(
        self,
        code: str,
        name: str,
        data: pd.DataFrame,
        start_date: str,
        end_date: Optional[str],
    ) -> List[Dict[str, Any]]:
        if data is None or getattr(data, "empty", True):
            return []

        df = data.rename(columns={column: self._normalize_column(column) for column in data.columns}).copy()
        if "trade_date" not in df.columns:
            return []

        df["trade_date"] = df["trade_date"].map(self._format_date)
        df = df.sort_values("trade_date")
        if "pre_close" not in df.columns:
            df["pre_close"] = df["close"].shift(1) if "close" in df.columns else None

        df = df[df["trade_date"] >= start_date]
        if end_date:
            df = df[df["trade_date"] <= end_date]
        if df.empty:
            return []

        now = datetime.utcnow()
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            open_price = self._safe_float(row.get("open"))
            close_price = self._safe_float(row.get("close"))
            pre_close = self._safe_float(row.get("pre_close"))
            record = {
                "full_symbol": self._full_symbol(code),
                "code": code,
                "symbol": code,
                "market": "CN",
                "trade_date": row["trade_date"],
                "period": PERIOD,
                "open": open_price,
                "high": self._safe_float(row.get("high")),
                "low": self._safe_float(row.get("low")),
                "close": close_price,
                "pre_close": pre_close,
                "volume": self._scale(row.get("volume"), 10000),
                "amount": self._scale(row.get("amount"), 100000000),
                "change": round(close_price - pre_close, 4) if close_price is not None and pre_close is not None else None,
                "pct_chg": round((close_price - open_price) / open_price * 100, 4)
                if close_price is not None and open_price
                else None,
                "data_source": DATA_SOURCE,
                "created_at": now,
                "updated_at": now,
                "version": 1,
            }
            records.append(record)
        return records

    @staticmethod
    def _normalize_column(column: Any) -> str:
        mapping = {
            "代码": "code",
            "日期": "trade_date",
            "收盘": "close",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
        }
        value = str(column).strip()
        return mapping.get(value, value.lower())

    @staticmethod
    def _normalize_code(sector_code: Any) -> str:
        value = str(sector_code or "").strip().upper()
        return value.split(".")[0]

    @staticmethod
    def _full_symbol(code: str) -> str:
        return f"{code}.SI"

    @staticmethod
    def _format_date(value: Any) -> str:
        if value is None:
            return datetime.utcnow().strftime("%Y-%m-%d")
        if isinstance(value, pd.Timestamp):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, (datetime, date)):
            return value.strftime("%Y-%m-%d")
        text = str(value).strip()
        if len(text) == 8 and text.isdigit():
            return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
        return text[:10]

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            if pd.isna(value):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _scale(cls, value: Any, multiplier: int) -> Optional[float]:
        number = cls._safe_float(value)
        return number * multiplier if number is not None else None

    @staticmethod
    def _clean_str(value: Any) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        return str(value).strip()

    @staticmethod
    def _result_count(result: Any, attr: str, default: int) -> int:
        return int(getattr(result, attr, default) or 0)

    @staticmethod
    def _result(
        started_at: datetime,
        status: str,
        total_symbols: int,
        total_records: int,
        inserted: int,
        updated: int,
        errors: int,
        message: str,
        details: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        result = {
            "status": status,
            "total_symbols": total_symbols,
            "total_records": total_records,
            "inserted": inserted,
            "updated": updated,
            "errors": errors,
            "message": message,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.utcnow().isoformat(),
        }
        if details is not None:
            result["details"] = details
        return result


_sw_index_daily_service: Optional[SwIndexDailyService] = None


def get_sw_index_daily_service() -> SwIndexDailyService:
    """Return singleton SW index daily service."""
    global _sw_index_daily_service
    if _sw_index_daily_service is None:
        _sw_index_daily_service = SwIndexDailyService()
    return _sw_index_daily_service


async def run_sw_index_daily_sync(force_full: bool = False) -> Dict[str, Any]:
    """APScheduler entrypoint for SW index daily sync."""
    service = get_sw_index_daily_service()
    result = await service.sync_all(force_full=force_full)
    logger.info("SW index daily sync finished: %s", result)
    return result
