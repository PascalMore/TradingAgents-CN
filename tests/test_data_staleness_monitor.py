"""Unit tests for the stock basics data staleness monitor."""
import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

from app.services import data_staleness_monitor as monitor

SHANGHAI = timezone(timedelta(hours=8))


def _database_with_updates(*updated_at_values):
    database = MagicMock()
    source = MagicMock()
    source.find.return_value = [
        {"updated_at": updated_at} for updated_at in updated_at_values
    ]
    alerts = MagicMock()
    database.__getitem__.side_effect = {
        monitor.TARGET_COLLECTION: source,
        monitor.ALERTS_COLLECTION: alerts,
    }.__getitem__
    return database, source, alerts


def test_staleness_monitor_job_is_registered_and_targets_check_staleness():
    main_path = Path(__file__).parents[1] / "app" / "main.py"
    tree = ast.parse(main_path.read_text(encoding="utf-8"))
    matching_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "add_job"
        and node.args
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == "check_staleness"
        and any(
            keyword.arg == "id"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == "stock_basic_info_staleness_monitor"
            for keyword in node.keywords
        )
    ]

    assert len(matching_calls) == 1


def test_fresh_data_does_not_write_alert():
    now = datetime(2026, 7, 23, 10, 0, tzinfo=SHANGHAI)  # Thursday
    database, source, alerts = _database_with_updates(
        datetime(2026, 7, 22, 2, 0, tzinfo=timezone.utc)
    )

    result = monitor.check_staleness(now=now, db=database)

    assert result["stale"] is False
    assert result["alert_written"] is False
    assert result["age_hours"] == 24.0
    source.find.assert_called_once_with(
        {"updated_at": {"$type": {"$in": ["date", "string"]}}},
        projection={"updated_at": 1},
    )
    alerts.update_one.assert_not_called()


def test_string_updated_at_is_parsed_for_freshness():
    now = datetime(2026, 7, 23, 10, 0, tzinfo=SHANGHAI)
    database, _, alerts = _database_with_updates("2026-07-22T02:00:00Z")

    result = monitor.check_staleness(now=now, db=database)

    assert result["stale"] is False
    assert result["age_hours"] == 24.0
    assert result["max_updated_at"] == "2026-07-22T02:00:00+00:00"
    alerts.update_one.assert_not_called()


def test_mixed_updated_at_types_choose_true_latest_time():
    now = datetime(2026, 7, 23, 10, 0, tzinfo=SHANGHAI)
    database, _, alerts = _database_with_updates(
        datetime(2026, 7, 20, 0, 0, tzinfo=timezone.utc),
        "2026-07-23T00:00:00+00:00",
    )

    result = monitor.check_staleness(now=now, db=database)

    assert result["stale"] is False
    assert result["age_hours"] == 2.0
    assert result["max_updated_at"] == "2026-07-23T00:00:00+00:00"
    alerts.update_one.assert_not_called()


def test_invalid_updated_at_values_fail_safe_without_alert():
    now = datetime(2026, 7, 23, 10, 0, tzinfo=SHANGHAI)
    database, _, alerts = _database_with_updates("not-a-date")

    result = monitor.check_staleness(now=now, db=database)

    assert result["skipped_reason"] == "missing_updated_at"
    alerts.update_one.assert_not_called()


def test_stale_data_on_weekday_upserts_alert():
    now = datetime(2026, 7, 23, 10, 0, tzinfo=SHANGHAI)  # Thursday
    database, _, alerts = _database_with_updates(
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
    database, source, alerts = _database_with_updates(
        datetime(2026, 7, 20, 0, 0, tzinfo=timezone.utc)
    )

    result = monitor.check_staleness(now=now, db=database)

    assert result["stale"] is False
    assert result["alert_written"] is False
    assert result["skipped_reason"] == "weekend"
    source.find.assert_not_called()
    alerts.update_one.assert_not_called()
