import os
import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone

from pythonjsonlogger import jsonlogger
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from common.utils import f1_macro
from common.constants import RunStatus
from common.models import Run, Prediction


class YcLoggingFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(YcLoggingFormatter, self).add_fields(log_record, record, message_dict)
        log_record['logger'] = record.name
        log_record['level'] = str.replace(str.replace(record.levelname, "WARNING", "WARN"), "CRITICAL", "FATAL")


logHandler = logging.StreamHandler()
logHandler.setFormatter(YcLoggingFormatter('%(message)s %(level)s %(logger)s'))

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logHandler)
logger.setLevel(logging.DEBUG)


def _db_url() -> str:
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")
    return f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


async def _finalize_runs(*, SessionLocal: async_sessionmaker) -> int:
    async with SessionLocal() as db:
        async with db.begin():
            query = (
                select(Run)
                .where(Run.status == RunStatus.RUNNING)
                .where(and_(Run.samples_total > 0, Run.samples_processed >= Run.samples_total))
                .with_for_update(skip_locked=True)
            )

            ready_runs = (await db.execute(query)).scalars().all()
            if not ready_runs:
                return 0

            ready_ids = [r.id for r in ready_runs]
            logger.info("READY_RUN_IDS", extra={'ready_ids': ready_ids})

            preds_rows = (await db.execute(
                select(Prediction.run_id, Prediction.gold_json, Prediction.pred_json, Prediction.latency_ms)
                .where(Prediction.run_id.in_(ready_ids))
            )).all()

            pairs_by_run = defaultdict(list)
            latencies_by_run = defaultdict(list)

            for rid, gold_json, pred_json, latency_ms in preds_rows:
                pairs_by_run[rid].append(((gold_json or []), (pred_json or [])))
                if latency_ms is not None:
                    try:
                        latencies_by_run[rid].append(float(latency_ms))
                    except Exception:
                        pass

            now = datetime.now(timezone.utc)
            for run in ready_runs:
                pairs = pairs_by_run.get(run.id, [])
                latencies = latencies_by_run.get(run.id, [])
                run.avg_latency_ms = (sum(latencies) / len(latencies)) if latencies else None
                run.f1 = f1_macro(pairs) if pairs else 0.0
                run.finished_at = now
                run.status = RunStatus.DONE
            return len(ready_runs)


def handler(event, context):
    logger.info("EVENT", extra=event)

    async def _run():
        engine = create_async_engine(
            _db_url(),
            pool_pre_ping=True,
            pool_size=1,
            max_overflow=1,
        )
        SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
        try:
            # Финализацию и выборку готовых прогонов делаем внутри самой функции
            return await _finalize_runs(SessionLocal=SessionLocal)
        finally:
            await engine.dispose()

    count = asyncio.run(_run())
    return {"finalized": count}
