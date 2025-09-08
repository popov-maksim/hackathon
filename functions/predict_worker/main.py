import os
import json
import time
import asyncio
from datetime import datetime, timezone

import httpx
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from common.constants import RunStatus
from common.models import Run, Prediction
from common.utils import normalize_pred, f1_macro
from common.config import REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT


_engine = None
_SessionLocal = None


def _db_url() -> str:
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")
    return f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


def _init_db():
    global _engine, _SessionLocal
    if _engine is None:
        _engine = create_async_engine(_db_url(), pool_pre_ping=True)
        _SessionLocal = async_sessionmaker(_engine, expire_on_commit=False)


async def _process_message(msg: dict, client: httpx.AsyncClient | None = None):
    run_id = int(msg["run_id"])  # required
    endpoint_url = str(msg["endpoint_url"]).rstrip("/")
    sample_idx = int(msg["sample_idx"])  # required
    sample = str(msg.get("sample", ""))
    gold = msg.get("gold") or []

    timeout = httpx.Timeout(REQUEST_READ_TIMEOUT, connect=REQUEST_CONNECT_TIMEOUT)
    latency_ms = None
    ok = False
    pred_json = None
    t0 = time.perf_counter()
    try:
        if client is None:
            async with httpx.AsyncClient(timeout=timeout) as _client:
                resp = await _client.post(endpoint_url, json={"input": sample})
        else:
            resp = await client.post(endpoint_url, json={"input": sample})
        latency_ms = (time.perf_counter() - t0) * 1000.0
        if resp.status_code == 200:
            data = resp.json()
            pred_json = normalize_pred(data)
            ok = True
    except Exception:
        pass

    _init_db()
    async with _SessionLocal() as db:
        # Insert prediction (idempotent via unique run_id+sample_idx)
        pred = Prediction(
            run_id=run_id,
            sample_idx=sample_idx,
            latency_ms=latency_ms,
            ok=ok,
            gold_json=gold,
            pred_json=pred_json,
        )
        try:
            async with db.begin():
                db.add(pred)
        except IntegrityError:
            ok = False

        # Update success counter if ok
        if ok:
            try:
                async with db.begin():
                    await db.execute(
                        update(Run)
                        .where(Run.id == run_id)
                        .values(samples_success=Run.samples_success + 1)
                    )
            except IntegrityError:
                pass


async def _finalize_if_complete(run_id: int):
    _init_db()
    async with _SessionLocal() as db:
        res = await db.execute(select(Run).where(Run.id == run_id))
        run: Run | None = res.scalar_one_or_none()
        if run is None:
            return
        # Count predictions
        cnt = (await db.execute(
            select(Prediction).where(Prediction.run_id == run_id)
        )).unique().scalars().all()
        n_preds = len(cnt)
        if n_preds < (run.samples_total or 0):
            return

        # Load data for metrics
        preds = cnt  # already loaded Prediction objects
        gold_pred_pairs = [
            (p.gold_json or [], p.pred_json or []) for p in preds
        ]
        latencies = [float(p.latency_ms) for p in preds if p.latency_ms is not None]
        avg_latency_ms = (sum(latencies) / len(latencies)) if latencies else None
        f1_val = f1_macro(gold_pred_pairs) if gold_pred_pairs else 0.0

        # Best-effort finalize (no hard lock; last write wins)
        async with db.begin():
            run.avg_latency_ms = avg_latency_ms
            run.f1 = f1_val
            run.finished_at = datetime.now(timezone.utc)
            run.status = RunStatus.DONE


def handler(event, context):
    """Yandex Cloud Function handler for YMQ trigger.
    Expects event with key 'messages', each message has 'body' as JSON string.
    """
    messages = []
    if isinstance(event, dict) and "messages" in event:
        for m in event["messages"]:
            try:
                messages.append(json.loads(m.get("body") or "{}"))
            except Exception:
                continue
    else:
        # direct invocation with a single payload
        if isinstance(event, dict):
            messages.append(event)

    async def _run():
        # Bounded concurrency within a single invocation
        max_conc = int(os.getenv("WORKER_MAX_CONCURRENCY", "10") or 10)
        sem = asyncio.Semaphore(max_conc if max_conc > 0 else 1)
        timeout = httpx.Timeout(REQUEST_READ_TIMEOUT, connect=REQUEST_CONNECT_TIMEOUT)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async def _task(m: dict):
                async with sem:
                    await _process_message(m, client=client)

            await asyncio.gather(*[_task(m) for m in messages])

        # finalize runs touched by this batch
        run_ids = {int(m.get("run_id")) for m in messages if "run_id" in m}
        for rid in run_ids:
            await _finalize_if_complete(rid)

    asyncio.run(_run())
    return {"processed": len(messages)}
