#!/usr/bin/env python3
"""Import HKSSE SW industry mappings into stock_sector_info."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.database import init_db, get_mongo_db
from app.services.stock_sector_info_service import (
    DEFAULT_HKSSE_SW_INDUSTRY_FILE,
    StockSectorInfoService,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import 港股通申万行业数据.xlsx into stock_sector_info")
    parser.add_argument("--file", default=str(DEFAULT_HKSSE_SW_INDUSTRY_FILE), help="Excel file path")
    parser.add_argument("--data-date", default=None, help="Data date in YYYY-MM-DD, defaults to file mtime date")
    parser.add_argument("--source", default="Wind", help="Datasource marker")
    parser.add_argument("--replace-source", action="store_true", help="Delete old records from the same source before import")
    parser.add_argument("--dry-run", action="store_true", help="Parse and validate only; do not write MongoDB")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    # Initialize DB connection (same pattern as other CLI scripts in this project)
    await init_db()
    get_mongo_db()  # ensure global mongo_db is set

    service = StockSectorInfoService()
    if args.dry_run:
        result = await service.sync_hksse_sw_industry_from_excel(
            file_path=args.file,
            data_date=args.data_date,
            source=args.source,
            replace_source=args.replace_source,
            dry_run=True,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return

    result = await service.sync_hksse_sw_industry_from_excel(
        file_path=args.file,
        data_date=args.data_date,
        source=args.source,
        replace_source=args.replace_source,
        dry_run=False,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())