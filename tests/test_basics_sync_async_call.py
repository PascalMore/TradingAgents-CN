"""Tests for the TA-CN stock basics scheduler's async invocation."""
import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock


async def _record_call(calls, *, force=False, preferred_sources=None):
    calls.append({"force": force, "preferred_sources": preferred_sources})
    return "completed"


def test_plain_sync_lambda_only_returns_coroutine():
    calls = []
    service = MagicMock()
    service.run_full_sync = lambda **kwargs: _record_call(calls, **kwargs)
    job = lambda: service.run_full_sync(force=False, preferred_sources=["akshare"])

    coroutine = job()
    try:
        assert inspect.iscoroutine(coroutine)
        assert calls == []
    finally:
        coroutine.close()


def test_asyncio_run_lambda_executes_coroutine_body():
    calls = []
    service = MagicMock()
    service.run_full_sync = lambda **kwargs: _record_call(calls, **kwargs)
    job = lambda: asyncio.run(
        service.run_full_sync(force=False, preferred_sources=["tushare", "akshare"])
    )

    assert job() == "completed"
    assert calls == [
        {"force": False, "preferred_sources": ["tushare", "akshare"]}
    ]


def test_registered_job_executes_async_body_in_scheduler_thread():
    calls = []
    service = MagicMock()
    service.run_full_sync = lambda **kwargs: _record_call(calls, **kwargs)
    scheduler = MagicMock()
    scheduler.add_job(
        lambda: asyncio.run(
            service.run_full_sync(force=False, preferred_sources=["baostock"])
        ),
        "cron",
        id="basics_sync_service",
    )
    registered_target = scheduler.add_job.call_args.args[0]

    with ThreadPoolExecutor(max_workers=1) as executor:
        result = executor.submit(registered_target).result(timeout=5)

    assert result == "completed"
    assert calls == [{"force": False, "preferred_sources": ["baostock"]}]
