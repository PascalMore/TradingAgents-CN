"""Portfolio stock pool API routes."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from app.core.response import ok
from app.routers.auth_db import get_current_user

WORKSPACE_ROOT = Path(__file__).resolve().parents[5]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.append(str(WORKSPACE_ROOT))

from skills.portfolio.stock_pool.api import get_stock_pool_service  # noqa: E402
from skills.portfolio.stock_pool.auto_promoter import StockPoolAutoPromoter  # noqa: E402
from skills.portfolio.stock_pool.models import PoolZone, StockPoolSource  # noqa: E402

router = APIRouter(tags=["portfolio-stock-pool"])


class StockPoolCreateRequest(BaseModel):
    """Manual stock pool entry payload."""

    stock_code: str = Field(..., min_length=1)
    wind_code: str = Field(..., min_length=1)
    stock_name: str = Field(..., min_length=1)
    pool_zone: PoolZone = PoolZone.SCAN
    source: StockPoolSource = StockPoolSource.MANUAL
    source_detail: Optional[str] = None
    source_project: str = "manual"
    source_signal_id: Optional[str] = None
    entry_reason: Dict[str, Any] = Field(default_factory=lambda: {"reason": "manual"})
    tags: list[str] = Field(default_factory=list)
    memo: str = ""


class TriggerRequest(BaseModel):
    """Manual trigger payload for automatic zone evaluation."""

    trade_date: Optional[str] = None
    dry_run: bool = True


def _actor(current_user: dict) -> str:
    return current_user.get("username") or current_user.get("id") or "webui"


@router.get("/api/portfolio/stock-pool")
async def list_stock_pool(
    pool_zone: Optional[PoolZone] = Query(None),
    source: Optional[StockPoolSource] = Query(None),
    status_filter: Optional[str] = Query("active", alias="status"),
    wind_code: Optional[str] = Query(None),
    sort_by: Optional[str] = Query("bayesian", alias="sort"),  # bayesian | entry_date
    limit: int = Query(100, ge=1, le=200),
    cursor: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user),
):
    """List stock pool entries, optionally filtered by zone or source."""
    service = get_stock_pool_service()
    page = service.get_pool(
        pool_zone=pool_zone.value if pool_zone else None,
        source=source.value if source else None,
        status=status_filter,
        wind_code=wind_code,
        sort_by=sort_by,
        limit=limit,
        cursor=cursor,
    )
    return ok(page)


@router.post("/api/portfolio/stock-pool")
async def create_stock_pool_entry(
    payload: StockPoolCreateRequest,
    current_user: dict = Depends(get_current_user),
):
    """Create a stock pool entry manually."""
    service = get_stock_pool_service()
    record = payload.dict()
    record["pool_zone"] = payload.pool_zone.value
    record["source"] = payload.source.value
    record["entry_date"] = datetime.utcnow()
    record_id = service.create_entry(record, actor=_actor(current_user))
    return ok({"id": record_id}, message="created")


@router.post("/api/portfolio/stock-pool/trigger-promote")
async def trigger_promote(
    payload: TriggerRequest = Body(default=TriggerRequest()),
    current_user: dict = Depends(get_current_user),
):
    """Run automatic promotion evaluation."""
    service = get_stock_pool_service()
    promoter = StockPoolAutoPromoter(service, actor=_actor(current_user))
    trade_date = payload.trade_date or datetime.utcnow().date().isoformat()
    return ok(promoter.evaluate_and_promote(trade_date=trade_date, dry_run=payload.dry_run))


@router.post("/api/portfolio/stock-pool/trigger-demote")
async def trigger_demote(
    payload: TriggerRequest = Body(default=TriggerRequest()),
    current_user: dict = Depends(get_current_user),
):
    """Run automatic demotion evaluation."""
    service = get_stock_pool_service()
    promoter = StockPoolAutoPromoter(service, actor=_actor(current_user))
    trade_date = payload.trade_date or datetime.utcnow().date().isoformat()
    return ok(promoter.evaluate_and_demote(trade_date=trade_date, dry_run=payload.dry_run))


@router.get("/api/portfolio/stock-pool/audit/{record_id}")
async def get_stock_pool_audit(
    record_id: str,
    limit: int = Query(100, ge=1, le=200),
    current_user: dict = Depends(get_current_user),
):
    """Return audit events for a stock pool record."""
    return ok(get_stock_pool_service().get_audit_history(record_id, limit=limit))


@router.get("/api/portfolio/stock-pool/capacity")
async def get_stock_pool_capacity(current_user: dict = Depends(get_current_user)):
    """Return active stock pool counts by zone."""
    return ok(get_stock_pool_service().get_capacity())


@router.get("/api/portfolio/stock-pool/{record_id}")
async def get_stock_pool_entry(
    record_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Fetch one stock pool entry by ID."""
    record = get_stock_pool_service().repository.get_by_id(record_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="stock pool entry not found")
    return ok(record)


@router.patch("/api/portfolio/stock-pool/{record_id}")
async def update_stock_pool_entry(
    record_id: str,
    patch: Dict[str, Any],
    current_user: dict = Depends(get_current_user),
):
    """Patch mutable stock pool entry fields."""
    changed = get_stock_pool_service().update_entry(record_id, patch, actor=_actor(current_user))
    if not changed:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="stock pool entry not found")
    return ok({"id": record_id, "changed": True})
