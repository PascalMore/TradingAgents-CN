"""Stock sector info API routes."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status

from app.core.response import ok
from app.routers.auth_db import get_current_user
from app.services.stock_sector_info_service import get_stock_sector_info_service

router = APIRouter(prefix="/api/sector", tags=["stock-sector-info"])


@router.get("/info/{full_symbol}")
async def get_sector_info(
    full_symbol: str,
    classify_system: str = Query("SW", min_length=1),
    current_user: dict = Depends(get_current_user),
):
    """获取单只股票行业分类信息。"""
    sector = await get_stock_sector_info_service().get_sector_by_symbol(
        full_symbol=full_symbol,
        classify_system=classify_system,
    )
    if sector is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="stock sector info not found")
    return ok(sector.to_dict())


@router.get("/stocks")
async def get_stocks_by_industry(
    l1_code: Optional[str] = Query(None),
    l2_code: Optional[str] = Query(None),
    l3_code: Optional[str] = Query(None),
    classify_system: str = Query("SW", min_length=1),
    limit: int = Query(1000, ge=1, le=5000),
    current_user: dict = Depends(get_current_user),
):
    """按行业代码查询股票列表。"""
    try:
        stocks = await get_stock_sector_info_service().get_stocks_by_industry(
            l1_code=l1_code,
            l2_code=l2_code,
            l3_code=l3_code,
            classify_system=classify_system,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return ok([stock.to_dict() for stock in stocks])


@router.post("/sync")
async def sync_stock_sector_info(
    background_tasks: BackgroundTasks,
    classify_system: str = Query("SW", min_length=1),
    wait: bool = Query(False, description="是否等待同步完成后返回结果"),
    current_user: dict = Depends(get_current_user),
):
    """触发股票行业分类全量同步。"""
    service = get_stock_sector_info_service()
    if wait:
        return ok(await service.sync_from_tushare(classify_system=classify_system))

    background_tasks.add_task(service.sync_from_tushare, classify_system=classify_system)
    return ok({"status": "scheduled", "classify_system": classify_system}, message="sync scheduled")
