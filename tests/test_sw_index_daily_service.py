from types import SimpleNamespace

import pandas as pd
import pytest

from app.services.sw_index_daily_service import SwIndexDailyService


class FakeCursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, field, direction):
        self.docs = sorted(self.docs, key=lambda doc: doc.get(field, ""), reverse=direction < 0)
        return self

    def limit(self, limit):
        self.docs = self.docs[:limit]
        return self

    async def to_list(self, length):
        return self.docs if length is None else self.docs[:length]


class FakeBulkResult:
    def __init__(self, upserted_ids, modified_count):
        self.upserted_ids = upserted_ids
        self.upserted_count = len(upserted_ids)
        self.modified_count = modified_count


class FakeCollection:
    def __init__(self, docs=None, key_fields=None):
        self.docs = {}
        self.indexes = []
        self.key_fields = key_fields or ("full_symbol", "trade_date")
        for doc in docs or []:
            self.docs[self._key(doc)] = doc.copy()

    def _key(self, doc):
        return tuple(doc[field] for field in self.key_fields)

    async def create_index(self, keys, **kwargs):
        self.indexes.append((tuple(keys), kwargs))

    async def bulk_write(self, operations, ordered=False):
        upserted_ids = {}
        modified_count = 0
        for idx, operation in enumerate(operations):
            doc = operation._doc["$set"]
            key = tuple(operation._filter[field] for field in self.key_fields)
            if key in self.docs:
                modified_count += 1
            else:
                upserted_ids[idx] = f"id-{idx}"
            self.docs[key] = doc.copy()
        return FakeBulkResult(upserted_ids, modified_count)

    async def distinct(self, field, query):
        return sorted({doc.get(field) for doc in self._filter(query) if doc.get(field)})

    async def find_one(self, query, projection=None, sort=None):
        docs = self._filter(query)
        if sort:
            field, direction = sort[0]
            docs = sorted(docs, key=lambda doc: doc.get(field, ""), reverse=direction < 0)
        return docs[0].copy() if docs else None

    def find(self, query, projection=None):
        return FakeCursor([doc.copy() for doc in self._filter(query)])

    def _filter(self, query):
        return [doc for doc in self.docs.values() if self._matches(doc, query)]

    def _matches(self, doc, query):
        for key, value in query.items():
            doc_value = doc.get(key)
            if isinstance(value, dict):
                if "$in" in value and doc_value not in value["$in"]:
                    return False
                if "$lte" in value and doc_value > value["$lte"]:
                    return False
                if "$gte" in value and doc_value < value["$gte"]:
                    return False
            elif doc_value != value:
                return False
        return True


class FakeDB:
    def __init__(self):
        self.collections = {
            "stock_sector_info": FakeCollection(
                [
                    {"classify_system": "SW", "l1_code": "801120", "l1_name": "食品饮料"},
                    {"classify_system": "SW", "l1_code": "801120", "l1_name": "食品饮料"},
                    {"classify_system": "SW", "l1_code": "801050", "l1_name": "有色金属"},
                    {"classify_system": "CITICS", "l1_code": "CI005", "l1_name": "非申万"},
                ],
                key_fields=("classify_system", "l1_code", "l1_name"),
            ),
            "index_daily_quotes": FakeCollection(),
        }

    def __getitem__(self, name):
        return self.collections[name]

    def __setitem__(self, name, value):
        self.collections[name] = value


class FakeAk:
    def __init__(self):
        self.calls = []

    def index_hist_sw(self, symbol, period):
        self.calls.append((symbol, period))
        return pd.DataFrame(
            [
                {
                    "代码": symbol,
                    "日期": "2026-05-19",
                    "收盘": 14859.22,
                    "开盘": 14800.0,
                    "最高": 14900.0,
                    "最低": 14700.0,
                    "成交量": 10.0,
                    "成交额": 20.0,
                },
                {
                    "代码": symbol,
                    "日期": "2026-05-20",
                    "收盘": 14857.95,
                    "开盘": 14966.82,
                    "最高": 14972.07,
                    "最低": 14807.05,
                    "成交量": 164486.15,
                    "成交额": 335.877498,
                },
            ]
        )


class FakeAkRealtimeFallback:
    def __init__(self):
        self.hist_calls = []
        self.realtime_calls = []

    def index_hist_sw(self, symbol, period):
        self.hist_calls.append((symbol, period))
        return pd.DataFrame(
            [
                {
                    "代码": symbol,
                    "日期": "2026-05-20",
                    "收盘": 14857.95,
                    "开盘": 14966.82,
                    "最高": 14972.07,
                    "最低": 14807.05,
                    "成交量": 164486.15,
                    "成交额": 335.877498,
                }
            ]
        )

    def index_realtime_sw(self, symbol):
        self.realtime_calls.append(symbol)
        return pd.DataFrame(
            [
                {
                    "指数代码": "801120.SI",
                    "指数名称": "食品饮料",
                    "今开盘": 14900.0,
                    "最高价": 15050.0,
                    "最低价": 14880.0,
                    "最新价": 15000.0,
                    "昨收盘": 14857.95,
                    "成交量": 12.34,
                    "成交额": 56.78,
                },
                {
                    "指数代码": "801050.SI",
                    "指数名称": "有色金属",
                    "今开盘": 1000.0,
                    "最高价": 1010.0,
                    "最低价": 990.0,
                    "最新价": 1005.0,
                    "昨收盘": 1001.0,
                    "成交量": 1.0,
                    "成交额": 2.0,
                },
            ]
        )


@pytest.mark.asyncio
async def test_sync_all_upserts_sw_l1_index_daily_quotes():
    db = FakeDB()
    db["index_daily_quotes"] = FakeCollection(
        [
            {"full_symbol": "801120.SI", "trade_date": "2026-05-19"},
        ]
    )
    ak = FakeAk()
    service = SwIndexDailyService(db=db, ak_module=ak, concurrency=5)

    result = await service.sync_all(end_date="20260520")

    assert result["status"] == "success"
    assert result["total_symbols"] == 2
    assert result["total_records"] == 3
    assert result["inserted"] == 3
    assert sorted(symbol for symbol, _ in ak.calls) == ["801050", "801120"]
    assert all(period == "day" for _, period in ak.calls)
    assert any(
        kwargs.get("name") == "uk_full_symbol_trade_date" and kwargs.get("unique") is True
        for keys, kwargs in db["index_daily_quotes"].indexes
    )

    doc = db["index_daily_quotes"].docs[("801120.SI", "2026-05-20")]
    assert doc["full_symbol"] == "801120.SI"
    assert doc["code"] == "801120"
    assert doc["symbol"] == "801120"
    assert doc["market"] == "CN"
    assert "name" not in doc
    assert doc["period"] == "daily"
    assert doc["version"] == 1
    assert doc["open"] == 14966.82
    assert doc["high"] == 14972.07
    assert doc["low"] == 14807.05
    assert doc["close"] == 14857.95
    assert doc["pre_close"] == 14859.22
    assert doc["volume"] == 1644861500.0
    assert doc["amount"] == 33587749800.0
    assert doc["change"] == -1.27
    assert doc["pct_chg"] == round((14857.95 - 14966.82) / 14966.82 * 100, 4)
    assert doc["data_source"] == "akshare"
    assert ("801120.SI", "2026-05-19") in db["index_daily_quotes"].docs
    assert ("801050.SI", "2026-05-19") in db["index_daily_quotes"].docs


@pytest.mark.asyncio
async def test_sync_all_force_full_starts_from_default_date():
    db = FakeDB()
    db["index_daily_quotes"] = FakeCollection(
        [
            {"full_symbol": "801120.SI", "trade_date": "2026-05-19"},
        ]
    )
    service = SwIndexDailyService(db=db, ak_module=FakeAk(), concurrency=5)

    result = await service.sync_all(force_full=True, end_date="20260520")

    assert result["status"] == "success"
    assert result["total_records"] == 4
    assert result["inserted"] == 3
    assert result["updated"] == 1


@pytest.mark.asyncio
async def test_get_sync_start_date_uses_latest_trade_date_or_default():
    db = FakeDB()
    db["index_daily_quotes"] = FakeCollection(
        [
            {"full_symbol": "801120.SI", "trade_date": "2026-05-18"},
            {"full_symbol": "801120.SI", "trade_date": "2026-05-20"},
        ]
    )
    service = SwIndexDailyService(db=db, ak_module=SimpleNamespace(index_hist_sw=None))

    assert await service._get_sync_start_date("801120.SI") == "2026-05-21"
    assert await service._get_sync_start_date("801050.SI") == "2000-01-01"


@pytest.mark.asyncio
async def test_get_sector_info_and_drawdown():
    db = FakeDB()
    db["index_daily_quotes"] = FakeCollection(
        [
            {"full_symbol": "801120.SI", "trade_date": "2026-05-18", "period": "daily", "close": 100.0},
            {"full_symbol": "801120.SI", "trade_date": "2026-05-19", "period": "daily", "close": 120.0},
            {"full_symbol": "801120.SI", "trade_date": "2026-05-20", "period": "daily", "close": 90.0},
        ]
    )
    service = SwIndexDailyService(db=db, ak_module=SimpleNamespace(index_hist_sw=None))

    info = await service.get_sector_info("801120.SI")
    drawdown = await service.get_drawdown("801120.SI", date="2026-05-20", window=3)

    assert info["full_symbol"] == "801120.SI"
    assert info["name"] == "食品饮料"
    assert drawdown == -0.25


@pytest.mark.asyncio
async def test_sync_one_falls_back_to_realtime_when_incremental_history_empty():
    db = FakeDB()
    db["index_daily_quotes"] = FakeCollection(
        [
            {"full_symbol": "801120.SI", "trade_date": "2026-05-20"},
        ]
    )
    ak = FakeAkRealtimeFallback()
    service = SwIndexDailyService(db=db, ak_module=ak)

    result = await service._sync_one("801120", "食品饮料", start_date=None, end_date=None, force_full=False)

    assert result["status"] == "success"
    assert result["records"] == 1
    assert result["inserted"] == 1
    assert ak.hist_calls == [("801120", "day")]
    assert ak.realtime_calls == ["一级行业"]

    doc = db["index_daily_quotes"].docs[("801120.SI", "2026-05-21")]
    assert doc["full_symbol"] == "801120.SI"
    assert doc["code"] == "801120"
    assert doc["symbol"] == "801120"
    assert doc["market"] == "CN"
    assert "name" not in doc
    assert doc["trade_date"] == "2026-05-21"
    assert doc["period"] == "daily"
    assert doc["data_source"] == "akshare"
    assert doc["version"] == 1
    assert doc["open"] == 14900.0
    assert doc["high"] == 15050.0
    assert doc["low"] == 14880.0
    assert doc["close"] == 15000.0
    assert doc["pre_close"] == 14857.95
    assert doc["volume"] == 123400.0
    assert doc["amount"] == 5678000000.0
    assert doc["change"] == round(15000.0 - 14857.95, 4)
    assert doc["pct_chg"] == round((15000.0 - 14857.95) / 14857.95 * 100, 4)
