"""Unit tests for the stock basics data staleness monitor."""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from app.services import data_staleness_monitor as monitor

SHANGHAI = timezone(timedelta(hours=8))


def _database_with_latest(updated_at):
    database = MagicMock()
    source = MagicMock()
    source.find_one.return_value = (
        {"updated_at": updated_at} if updated_at is not None else None
    )
    alerts = MagicMock()
    database.__getitem__.side_effect = {
        monitor.TARGET_COLLECTION: source,
        monitor.ALERTS_COLLECTION: alerts,
    }.__getitem__
    return database, source, alerts


def test_fresh_data_does_not_write_alert():
    now = datetime(2026, 7, 23, 10, 0, tzinfo=SHANGHAI)  # Thursday
    database, source, alerts = _database_with_latest(
        datetime(2026, 7, 22, 2, 0, tzinfo=timezone.utc)
    )

    result = monitor.check_staleness(now=now, db=database)

    assert result["stale"] is False
    assert result["alert_written"] is False
    assert result["age_hours"] == 24.0
    source.find_one.assert_called_once_with(
        {"updated_at": {"$type": "date"}},
        sort=[("updated_at", -1)],
        projection={"updated_at": 1},
    )
    alerts.update_one.assert_not_called()


def test_stale_data_on_weekday_upserts_alert():
    now = datetime(2026, 7, 23, 10, 0, tzinfo=SHANGHAI)  # Thursday
    database, _, alerts = _database_with_latest(
        datetime(2026, 7, 21, 0, 0, tzinfo=timezone.utc)
    )

    result = monitor.check_staleness(now=now, db=database)

    assert result["stale"] is True
    assert result["alert_written"] is True
    assert result["age_hours"] == 50.0
    alerts.update_one.assert_called_once()
    query, update = alerts.update_one.call_args.args
    assert query == {"collection_name": monitor.TARGET_COLLECTION}
    assert update["$set"]["status"] == "stale"
    assert update["$set"]["threshold_hours"] == 48
    assert alerts.update_one.call_args.kwargs == {"upsert": True}


def test_stale_data_on_weekend_is_exempt_without_database_access():
    now = datetime(2026, 7, 25, 10, 0, tzinfo=SHANGHAI)  # Saturday
    database, source, alerts = _database_with_latest(
        datetime(2026, 7, 20, 0, 0, tzinfo=timezone.utc)
    )

    result = monitor.check_staleness(now=now, db=database)

    assert result["stale"] is False
    assert result["alert_written"] is False
    assert result["skipped_reason"] == "weekend"
    source.find_one.assert_not_called()
    alerts.update_one.assert_not_called()
