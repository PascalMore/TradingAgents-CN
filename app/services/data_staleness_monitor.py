"""Monitor ``stock_basic_info`` freshness and persist stale-data alerts."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

STALENESS_THRESHOLD_HOURS = 48
TARGET_COLLECTION = "stock_basic_info"
ALERTS_COLLECTION = "data_staleness_alerts"
DEFAULT_TZ_NAME = "Asia/Shanghai"


def _resolve_timezone() -> ZoneInfo:
    """Return the configured timezone and fail fast when it is invalid."""
    try:
        from app.core.config import settings
    except ModuleNotFoundError as exc:
        if exc.name != "pydantic":
            raise
        return ZoneInfo(DEFAULT_TZ_NAME)

    return ZoneInfo(getattr(settings, "TIMEZONE", DEFAULT_TZ_NAME) or DEFAULT_TZ_NAME)


def _is_workday(now_local: datetime) -> bool:
    """Return whether the date is Monday through Friday.

    The project has no authoritative holiday calendar in this path, so only weekends
    are exempted. A future holiday-calendar integration must be specified separately.
    """
    return now_local.weekday() < 5


def _parse_updated_at(value: Any) -> datetime | None:
    """Normalize BSON datetimes and ISO-8601 strings to UTC."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _latest_updated_at(db: Any) -> datetime | None:
    """Read and compare all BSON date/ISO string ``updated_at`` values."""
    documents = db[TARGET_COLLECTION].find(
        {"updated_at": {"$type": {"$in": ["date", "string"]}}},
        projection={"updated_at": 1},
    )
    parsed_values = (
        parsed
        for document in documents
        if (parsed := _parse_updated_at(document.get("updated_at"))) is not None
    )
    return max(parsed_values, default=None)


def _age_hours(updated_at: datetime, now_local: datetime) -> float:
    """Calculate age in hours, treating PyMongo's naive datetimes as UTC."""
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    else:
        updated_at = updated_at.astimezone(timezone.utc)
    return (now_local.astimezone(timezone.utc) - updated_at).total_seconds() / 3600


def check_staleness(*, now: datetime | None = None, db: Any = None) -> dict[str, Any]:
    """Check freshness once and upsert an alert when data is over 48 hours old."""
    tz = _resolve_timezone()
    now_local = now or datetime.now(tz)
    if now_local.tzinfo is None:
        now_local = now_local.replace(tzinfo=tz)
    else:
        now_local = now_local.astimezone(tz)

    result: dict[str, Any] = {
        "checked_at": now_local.isoformat(),
        "stale": False,
        "max_updated_at": None,
        "age_hours": None,
        "threshold_hours": STALENESS_THRESHOLD_HOURS,
        "alert_written": False,
        "skipped_reason": None,
    }

    if not _is_workday(now_local):
        result["skipped_reason"] = "weekend"
        logger.info("Skipping data staleness check on weekend: %s", now_local.date())
        return result

    if db is None:
        from app.core.database import get_mongo_db_sync

        database = get_mongo_db_sync()
    else:
        database = db
    latest = _latest_updated_at(database)
    if latest is None:
        result["skipped_reason"] = "missing_updated_at"
        logger.warning("%s has no valid updated_at value", TARGET_COLLECTION)
        return result

    age = _age_hours(latest, now_local)
    result["max_updated_at"] = latest.isoformat()
    result["age_hours"] = round(age, 2)
    if age <= STALENESS_THRESHOLD_HOURS:
        logger.info("%s freshness is %.1f hours", TARGET_COLLECTION, age)
        return result

    alert = {
        "collection_name": TARGET_COLLECTION,
        "max_updated_at": latest,
        "age_hours": round(age, 2),
        "threshold_hours": STALENESS_THRESHOLD_HOURS,
        "checked_at": now_local.astimezone(timezone.utc),
        "status": "stale",
    }
    database[ALERTS_COLLECTION].update_one(
        {"collection_name": TARGET_COLLECTION},
        {"$set": alert},
        upsert=True,
    )
    result["stale"] = True
    result["alert_written"] = True
    logger.error(
        "%s data is stale: latest=%s age=%.1fh threshold=%dh",
        TARGET_COLLECTION,
        latest.isoformat(),
        age,
        STALENESS_THRESHOLD_HOURS,
    )
    return result
