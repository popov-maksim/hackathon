import os
import asyncio
from datetime import datetime, timezone, timedelta

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import select

from common.constants import RunStatus
from common.models import Run, Prediction
from common.utils import f1_macro
from common.config import RUN_TIME_LIMIT_SECONDS


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


async def _finalize_run(run_id: int):
    _init_db()
    async with _SessionLocal() as db:
        res = await db.execute(select(Run).where(Run.id == run_id))
        run: Run | None = res.scalar_one_or_none()
        if run is None:
            return False
        preds = (await db.execute(select(Prediction).where(Prediction.run_id == run_id))).scalars().all()
        if not preds:
            return False

        gold_pred_pairs = [(p.gold_json or [], p.pred_json or []) for p in preds]
        latencies = [float(p.latency_ms) for p in preds if p.latency_ms is not None]
        avg_latency_ms = (sum(latencies) / len(latencies)) if latencies else None
        f1_val = f1_macro(gold_pred_pairs) if gold_pred_pairs else 0.0

        run.avg_latency_ms = avg_latency_ms
        run.f1 = f1_val
        run.finished_at = datetime.now(timezone.utc)
        run.status = RunStatus.DONE
        await db.commit()
        return True


def handler(event, context):
    """Periodic finalizer: closes completed or timed-out runs.
    """
    async def _run():
        _init_db()
        now = datetime.now(timezone.utc)
        timed_out_before = now - timedelta(seconds=RUN_TIME_LIMIT_SECONDS)
        async with _SessionLocal() as db:
            # Candidates: RUNNING and started long ago, or simply RUNNING (we'll check completeness below)
            runs = (await db.execute(
                select(Run).where(Run.status == RunStatus.RUNNING)
            )).scalars().all()

        finalized = 0
        for r in runs:
            complete = False
            if r.samples_total and r.samples_total > 0:
                async with _SessionLocal() as db:
                    n_preds = len((await db.execute(select(Prediction).where(Prediction.run_id == r.id))).scalars().all())
                    complete = n_preds >= r.samples_total
            timeout_hit = (r.started_at or now) < timed_out_before
            if complete or timeout_hit:
                ok = await _finalize_run(r.id)
                if ok:
                    finalized += 1

        return finalized

    count = asyncio.run(_run())
    return {"finalized": count}
