from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from app.services.stock_sector_info_service import StockSectorInfoService


class FakeCursor:
    def __init__(self, docs):
        self.docs = docs

    def sort(self, field, direction):
        self.docs = sorted(self.docs, key=lambda doc: doc.get(field, ""), reverse=direction < 0)
        return self

    def limit(self, limit):
        self.docs = self.docs[:limit]
        return self

    async def to_list(self, length):
        return self.docs[:length]


class FakeBulkResult:
    def __init__(self, upserted_ids, modified_count):
        self.upserted_ids = upserted_ids
        self.modified_count = modified_count


class FakeCollection:
    def __init__(self):
        self.docs = {}
        self.indexes = []

    async def create_index(self, keys, **kwargs):
        self.indexes.append((tuple(keys), kwargs))

    async def bulk_write(self, operations, ordered=False):
        upserted_ids = {}
        modified_count = 0
        for idx, operation in enumerate(operations):
            doc = operation._doc["$set"]
            key = (operation._filter["full_symbol"], operation._filter["classify_system"])
            if key in self.docs:
                modified_count += 1
            else:
                upserted_ids[idx] = f"id-{idx}"
            self.docs[key] = doc.copy()
        return FakeBulkResult(upserted_ids, modified_count)

    async def find_one(self, query):
        return self.docs.get((query["full_symbol"], query["classify_system"]))

    def find(self, query):
        docs = [
            doc
            for doc in self.docs.values()
            if all(doc.get(key) == value for key, value in query.items())
        ]
        return FakeCursor(docs)


class FakeDB:
    def __init__(self):
        self.collections = {"stock_sector_info": FakeCollection()}

    def __getitem__(self, name):
        return self.collections[name]


class FakeProvider:
    def __init__(self, df):
        self.api = SimpleNamespace(index_member_all=lambda **kwargs: df)
        self.connected = False

    def is_available(self):
        return self.connected

    async def connect(self):
        self.connected = True
        return True

    def _normalize_ts_code(self, symbol):
        if "." in symbol:
            return symbol
        return f"{symbol}.SH" if symbol.startswith(("60", "68", "90")) else f"{symbol}.SZ"


@pytest.fixture
def sector_df():
    return pd.DataFrame(
        [
            {
                "ts_code": "600519.SH",
                "name": "贵州茅台",
                "l1_code": "801120",
                "l1_name": "食品饮料",
                "l2_code": "801123",
                "l2_name": "白酒",
                "l3_code": "851231",
                "l3_name": "白酒Ⅲ",
            },
            {
                "ts_code": "000858.SZ",
                "name": "五粮液",
                "l1_code": "801120",
                "l1_name": "食品饮料",
                "l2_code": "801123",
                "l2_name": "白酒",
                "l3_code": "851231",
                "l3_name": "白酒Ⅲ",
            },
        ]
    )


@pytest.mark.asyncio
async def test_sync_from_tushare_upserts_sector_info_and_indexes(sector_df):
    db = FakeDB()
    service = StockSectorInfoService(db=db, provider=FakeProvider(sector_df))

    result = await service.sync_from_tushare()

    assert result["status"] == "success"
    assert result["total"] == 2
    assert result["inserted"] == 2
    assert ("600519.SH", "SW") in db["stock_sector_info"].docs
    assert any(
        kwargs.get("name") == "uk_full_symbol_classify_system" and kwargs.get("unique") is True
        for keys, kwargs in db["stock_sector_info"].indexes
    )


@pytest.mark.asyncio
async def test_get_sector_by_symbol_supports_full_symbol_lookup(sector_df):
    service = StockSectorInfoService(db=FakeDB(), provider=FakeProvider(sector_df))
    await service.sync_from_tushare()

    sector = await service.get_sector_by_symbol("600519.SH")

    assert sector is not None
    assert sector.code == "600519"
    assert sector.symbol == "600519"
    assert sector.full_symbol == "600519.SH"
    assert sector.name == "贵州茅台"
    assert sector.l1_name == "食品饮料"
    assert sector.datasource == "tushare"
    assert isinstance(sector.update_at, datetime)


@pytest.mark.asyncio
async def test_get_stocks_by_industry_filters_by_l1_l2_l3(sector_df):
    service = StockSectorInfoService(db=FakeDB(), provider=FakeProvider(sector_df))
    await service.sync_from_tushare()

    stocks = await service.get_stocks_by_industry(
        l1_code="801120",
        l2_code="801123",
        l3_code="851231",
    )

    assert [stock.full_symbol for stock in stocks] == ["000858.SZ", "600519.SH"]


@pytest.mark.asyncio
async def test_get_stocks_by_industry_requires_industry_code(sector_df):
    service = StockSectorInfoService(db=FakeDB(), provider=FakeProvider(sector_df))

    with pytest.raises(ValueError, match="At least one industry code is required"):
        await service.get_stocks_by_industry()
