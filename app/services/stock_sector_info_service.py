"""Stock sector classification mapping service."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from zipfile import ZipFile
from xml.etree import ElementTree as ET
import re

import pandas as pd
from pymongo import UpdateOne

if TYPE_CHECKING:
    from motor.motor_asyncio import AsyncIOMotorDatabase
    from tradingagents.dataflows.providers.china.tushare import TushareProvider

logger = logging.getLogger(__name__)

COLLECTION_NAME = "stock_sector_info"
DEFAULT_HKSSE_SW_INDUSTRY_FILE = (
    Path(__file__).resolve().parents[2] / "data" / "imports" / "港股通申万行业数据.xlsx"
)

SW_L1_CODE_BY_NAME = {
    "农林牧渔": "801010",
    "基础化工": "801030",
    "钢铁": "801040",
    "有色金属": "801050",
    "电子": "801080",
    "家用电器": "801110",
    "食品饮料": "801120",
    "纺织服饰": "801130",
    "轻工制造": "801140",
    "医药生物": "801150",
    "公用事业": "801160",
    "交通运输": "801170",
    "房地产": "801180",
    "商贸零售": "801200",
    "社会服务": "801210",
    "综合": "801230",
    "建筑材料": "801710",
    "建筑装饰": "801720",
    "电力设备": "801730",
    "国防军工": "801740",
    "计算机": "801750",
    "传媒": "801760",
    "通信": "801770",
    "银行": "801780",
    "非银金融": "801790",
    "汽车": "801880",
    "机械设备": "801890",
    "煤炭": "801950",
    "石油石化": "801960",
    "环保": "801970",
    "美容护理": "801980",
}


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

    async def sync_hksse_sw_industry_from_excel(
        self,
        file_path: str | Path = DEFAULT_HKSSE_SW_INDUSTRY_FILE,
        data_date: Optional[str] = None,
        source: str = "Wind",
        replace_source: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """从港股通申万行业 Excel 导入港股行业映射。

        Args:
            file_path: 港股通申万行业 Excel 路径。
            data_date: 数据日期，默认使用文件 mtime 对应日期。
            source: 数据来源标记。
            replace_source: 写入前是否删除同来源旧记录，用于全量覆盖。
            dry_run: 仅解析和校验，不写入数据库。
        """
        started_at = datetime.utcnow()
        records = parse_hksse_sw_industry_excel(file_path, data_date=data_date, source=source)
        if dry_run:
            return {
                "status": "success",
                "mode": "dry_run",
                "total": len(records),
                "inserted": 0,
                "updated": 0,
                "deleted": 0,
                "errors": 0,
                "started_at": started_at.isoformat(),
                "finished_at": datetime.utcnow().isoformat(),
            }

        await self.ensure_indexes()
        l2_code_by_name, l3_code_by_name = await self._build_a_share_sw_industry_code_maps()
        for doc in records:
            l2_name = self._clean_optional_str(doc.get("l2_name"))
            l3_name = self._clean_optional_str(doc.get("l3_name"))
            doc["l2_code"] = l2_code_by_name.get(l2_name) if l2_name else None
            doc["l3_code"] = l3_code_by_name.get(l3_name) if l3_name else None

        deleted = 0
        if replace_source:
            result = await self.collection.delete_many({"classify_system": "SW", "datasource": source})
            deleted = result.deleted_count or 0

        operations = [
            UpdateOne(
                {"full_symbol": doc["full_symbol"], "classify_system": doc["classify_system"]},
                {"$set": doc},
                upsert=True,
            )
            for doc in records
        ]

        inserted = 0
        updated = 0
        errors = 0
        for start in range(0, len(operations), 1000):
            batch = operations[start : start + 1000]
            try:
                result = await self.collection.bulk_write(batch, ordered=False)
                inserted += len(result.upserted_ids or {})
                updated += result.modified_count or 0
            except Exception as exc:
                errors += len(batch)
                logger.exception("Bulk upsert HKSSE SW industry info failed: %s", exc)

        return {
            "status": "success" if errors == 0 else "success_with_errors",
            "mode": "replace_source" if replace_source else "upsert",
            "total": len(records),
            "inserted": inserted,
            "updated": updated,
            "deleted": deleted,
            "errors": errors,
            "source": source,
            "data_date": data_date or records[0].get("data_date") if records else None,
            "l2_code_matched": sum(1 for doc in records if doc.get("l2_code")),
            "l2_code_unmatched": sum(1 for doc in records if doc.get("l2_name") and not doc.get("l2_code")),
            "l3_code_matched": sum(1 for doc in records if doc.get("l3_code")),
            "l3_code_unmatched": sum(1 for doc in records if doc.get("l3_name") and not doc.get("l3_code")),
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

    async def _build_a_share_sw_industry_code_maps(self) -> tuple[Dict[str, str], Dict[str, str]]:
        """Build A-share SW L2/L3 name-to-code indexes from existing stock_sector_info rows."""
        cursor = self.collection.find({"classify_system": "SW"})
        docs = await cursor.to_list(length=None)
        l2_code_by_name: Dict[str, str] = {}
        l3_code_by_name: Dict[str, str] = {}
        for doc in docs:
            if not self._is_a_share_symbol(doc.get("full_symbol")):
                continue
            l2_name = self._clean_optional_str(doc.get("l2_name"))
            l2_code = self._clean_optional_str(doc.get("l2_code"))
            l3_name = self._clean_optional_str(doc.get("l3_name"))
            l3_code = self._clean_optional_str(doc.get("l3_code"))
            if l2_name and l2_code:
                l2_code_by_name.setdefault(l2_name, l2_code)
            if l3_name and l3_code:
                l3_code_by_name.setdefault(l3_name, l3_code)
        return l2_code_by_name, l3_code_by_name

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
    def _is_a_share_symbol(full_symbol: Any) -> bool:
        value = str(full_symbol or "").strip().upper()
        return value.endswith((".SH", ".SZ", ".BJ"))

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


def parse_hksse_sw_industry_excel(
    file_path: str | Path = DEFAULT_HKSSE_SW_INDUSTRY_FILE,
    data_date: Optional[str] = None,
    source: str = "Wind",
    sw1_code_by_name: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """解析港股通申万行业 Excel，输出 stock_sector_info 兼容记录。"""
    path = Path(file_path)
    rows = _read_xlsx_rows(path)
    header_index = _find_hksse_header_row(rows)
    if header_index is None:
        raise ValueError("港股通申万行业 Excel 缺少必要表头：证券代码/证券简称/申万行业名称")

    mapping = sw1_code_by_name or SW_L1_CODE_BY_NAME
    date_value = data_date or datetime.fromtimestamp(path.stat().st_mtime).date().isoformat()
    parsed: List[Dict[str, Any]] = []
    seen = set()
    for row in rows[header_index + 1 :]:
        values = [_clean_excel_value(value) for value in row]
        if not values or not values[0] or values[0].startswith("数据来源"):
            continue
        stock_code = _normalize_hk_symbol(values[0])
        stock_name = values[1] if len(values) > 1 else ""
        industry_path = values[2] if len(values) > 2 else ""
        industry_code_path = values[3] if len(values) > 3 else ""
        sw1_name = _first_path_part(industry_path)
        sector_code = _resolve_sw1_code(sw1_name, industry_code_path, mapping)
        if not stock_code or not stock_name or not sw1_name or not sector_code:
            continue
        if stock_code in seen:
            continue
        seen.add(stock_code)
        parsed.append(
            {
                "full_symbol": stock_code,
                "classify_system": "SW",
                "code": stock_code.split(".")[0],
                "symbol": stock_code.split(".")[0],
                "name": stock_name,
                "l1_code": sector_code,
                "l1_name": sw1_name,
                "l2_code": None,
                "l2_name": _path_part(industry_path, 1),
                "l3_code": None,
                "l3_name": _path_part(industry_path, 2),
                "datasource": source,
                "update_at": datetime.utcnow(),
            }
        )
    if not parsed:
        raise ValueError("港股通申万行业 Excel 未解析出有效记录")
    return parsed


def _read_xlsx_rows(path: Path) -> List[List[str]]:
    """Read XLSX cell values without relying on workbook styles."""
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with ZipFile(path) as archive:
        shared_strings = _read_shared_strings(archive, ns)
        sheet_names = [name for name in archive.namelist() if name.startswith("xl/worksheets/sheet")]
        if not sheet_names:
            raise ValueError(f"Excel 文件没有 worksheet: {path}")
        sheet = ET.fromstring(archive.read(sorted(sheet_names)[0]))

    rows: List[List[str]] = []
    for row in sheet.findall(".//x:sheetData/x:row", ns):
        values: Dict[int, str] = {}
        for cell in row.findall("x:c", ns):
            col_index = _column_index(cell.attrib.get("r", "A1"))
            values[col_index] = _cell_value(cell, shared_strings, ns)
        if values:
            max_col = max(values)
            rows.append([values.get(idx, "") for idx in range(max_col + 1)])
    return rows


def _read_shared_strings(archive: ZipFile, ns: Dict[str, str]) -> List[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    return ["".join(t.text or "" for t in item.findall(".//x:t", ns)) for item in root.findall("x:si", ns)]


def _cell_value(cell: ET.Element, shared_strings: List[str], ns: Dict[str, str]) -> str:
    if cell.attrib.get("t") == "inlineStr":
        return "".join(t.text or "" for t in cell.findall(".//x:t", ns))
    value_node = cell.find("x:v", ns)
    if value_node is None or value_node.text is None:
        return ""
    value = value_node.text
    if cell.attrib.get("t") == "s":
        return shared_strings[int(value)]
    return value


def _find_hksse_header_row(rows: List[List[str]]) -> Optional[int]:
    for index, row in enumerate(rows[:20]):
        values = [_clean_excel_value(value) for value in row]
        if len(values) >= 3 and "证券代码" in values[0] and "证券简称" in values[1] and "申万行业名称" in values[2]:
            return index
    return None


def _resolve_sw1_code(sw1_name: str, code_path: str, mapping: Dict[str, str]) -> str:
    code = _first_path_part(code_path)
    if re.fullmatch(r"801\d{3}(?:\.SI)?", code):
        return code if code.endswith(".SI") else f"{code}.SI"
    mapped = mapping.get(sw1_name, "")
    return mapped if mapped.endswith(".SI") else f"{mapped}.SI" if mapped else ""


def _normalize_hk_symbol(value: str) -> str:
    symbol = _clean_excel_value(value).upper()
    if symbol.endswith(".HK"):
        return f"{symbol.split('.')[0].zfill(4)}.HK"
    if symbol.isdigit():
        return f"{symbol.zfill(4)}.HK"
    return symbol


def _first_path_part(value: str) -> str:
    return _path_part(value, 0) or ""


def _path_part(value: str, index: int) -> Optional[str]:
    parts = [_clean_excel_value(part) for part in _clean_excel_value(value).split("--")]
    parts = [part for part in parts if part]
    return parts[index] if index < len(parts) else None


def _clean_excel_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _column_index(cell_ref: str) -> int:
    letters = re.sub(r"[^A-Z]", "", cell_ref.upper())
    index = 0
    for letter in letters:
        index = index * 26 + ord(letter) - ord("A") + 1
    return index - 1


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
