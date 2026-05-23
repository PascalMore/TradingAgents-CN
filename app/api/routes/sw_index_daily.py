"""SW industry index daily quote API routes."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status

from app.core.response import ok
from app.routers.auth_db import get_current_user
from app.services.sw_index_daily_service import get_sw_index_daily_service

router = APIRouter(prefix="/api/index-data", tags=["sw-index-daily"])


@router.post("/sync")
async def sync_sw_index_daily(
    background_tasks: BackgroundTasks,
    end_date: Optional[str] = Query(None),
    force_full: bool = Query(False, description="是否强制全量同步"),
    wait: bool = Query(False, description="是否等待同步完成后返回结果"),
    current_user: dict = Depends(get_current_user),
):
    """触发申万一级行业指数日线同步。"""
    service = get_sw_index_daily_service()
    if wait:
        return ok(await service.sync_all(force_full=force_full, end_date=end_date))

    background_tasks.add_task(service.sync_all, force_full=force_full, end_date=end_date)
    return ok({"status": "scheduled", "force_full": force_full, "end_date": end_date}, message="sync scheduled")


@router.get("/info/{sector_code}")
async def get_sw_index_info(
    sector_code: str,
    current_user: dict = Depends(get_current_user),
):
    """查询申万行业指数基本信息。"""
    info = await get_sw_index_daily_service().get_sector_info(sector_code)
    if info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="SW sector index info not found")
    return ok(info)


@router.get("/drawdown/{sector_code}")
async def get_sw_index_drawdown(
    sector_code: str,
    date: str = Query(..., description="YYYY-MM-DD"),
    window: int = Query(20, ge=1, le=252),
    current_user: dict = Depends(get_current_user),
):
    """查询申万行业指数N日回撤。"""
    drawdown = await get_sw_index_daily_service().get_drawdown(sector_code, date=date, window=window)
    return ok({"sector_code": sector_code, "date": date, "window": window, "drawdown": drawdown})
