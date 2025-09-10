import os
import json
import time
import logging
import asyncio
from datetime import datetime, timezone

import httpx
from pythonjsonlogger import jsonlogger
from sqlalchemy import update, text, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from common.models import Run, Prediction
from common.utils import normalize_pred, f1_macro
from common.constants import RunStatus
from common.config import REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT


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


async def _maybe_finalize(run_id: int, *, SessionLocal: async_sessionmaker) -> bool:
    async with SessionLocal() as db:
        async with db.begin():
            run = (await db.execute(select(Run).where(Run.id == run_id).with_for_update())).scalar_one_or_none()
            if run is None:
                return False

            # Уже завершён или ещё не всё поставлено в очередь
            if run.status != RunStatus.RUNNING:
                return False
            if not (run.samples_total and run.samples_processed >= run.samples_total):
                return False

            # Соберём метрики по всем предиктам этого прогона
            preds = (
                await db.execute(
                    select(Prediction.gold_json, Prediction.pred_json, Prediction.latency_ms)
                    .where(Prediction.run_id == run_id)
                )
            ).all()

            pairs = []
            latencies = []
            for gold_json, pred_json, latency_ms in preds:
                pairs.append(((gold_json or []), (pred_json or [])))
                if latency_ms is not None:
                    try:
                        latencies.append(float(latency_ms))
                    except Exception:
                        pass

            run.avg_latency_ms = (sum(latencies) / len(latencies)) if latencies else None
            run.f1 = f1_macro(pairs) if pairs else 0.0
            run.finished_at = datetime.now(timezone.utc)
            run.status = RunStatus.DONE
            logger.info("RUN_FINALIZED", extra={"run_id": run_id, "f1": run.f1, "avg_latency_ms": run.avg_latency_ms})
            return True


async def _process_message(
    msg: dict,
    *,
    client: httpx.AsyncClient,
    SessionLocal: async_sessionmaker,
):
    run_id = int(msg["run_id"])
    endpoint_url = str(msg["endpoint_url"]).rstrip("/")
    sample_idx = int(msg["sample_idx"])
    sample = str(msg.get("sample", ""))
    gold = msg.get("gold", [])

    latency_ms = None
    ok = False
    pred_json = None
    try:
        t0 = time.perf_counter()
        resp = await client.post(endpoint_url, json={"input": sample})
        latency_ms = (time.perf_counter() - t0) * 1000.0
        if resp.status_code == 200:
            data = resp.json()
            pred_json = normalize_pred(data)
            ok = True
        else:
            logger.info("REQUEST ERROR", extra={'status_code': resp.status_code, 'text': resp.text})
    except Exception as e:
        logger.info("REQUEST ERROR", extra={'error': type(e)})

    async with SessionLocal() as db:
        async with db.begin():
            pred = Prediction(
                run_id=run_id,
                sample_idx=sample_idx,
                latency_ms=latency_ms,
                ok=ok,
                gold_json=gold,
                pred_json=pred_json,
            )
            try:
                db.add(pred)

                await db.execute(
                    update(Run)
                    .where(Run.id == run_id)
                    .values(samples_processed=Run.samples_processed + 1)
                )

                if ok:
                    await db.execute(
                        update(Run)
                        .where(Run.id == run_id)
                        .values(samples_success=Run.samples_success + 1)
                    )
            except IntegrityError:
                pass

    # После фиксации вставки — попробовать финализировать прогон
    try:
        await _maybe_finalize(run_id, SessionLocal=SessionLocal)
    except Exception as e:
        # Не мешаем обработке сообщений; финализатор по таймеру добьёт при сбое
        logger.info("EAGER_FINALIZE_ERROR", extra={"run_id": run_id, "error": str(e)})


def handler(event, context):
    logger.info("REQUEST_READ_TIMEOUT", extra={'REQUEST_READ_TIMEOUT': REQUEST_READ_TIMEOUT})
    logger.info("REQUEST_CONNECT_TIMEOUT", extra={'REQUEST_CONNECT_TIMEOUT': REQUEST_CONNECT_TIMEOUT})
    logger.info("EVENT", extra=event)

    messages = []
    for m in event["messages"]:
        body = m.get("details", {}).get("message", {}).get("body", None)
        logger.info("MESSAGE", extra={'type': type(m), 'body': body})
        if body is not None:
            messages.append(json.loads(body))

    logger.info("MESSAGES", extra={'parsed_messages': messages})
    logger.info("LENGTH MESSAGES", extra={'length_messages': len(messages)})

    async def _run():
        engine = create_async_engine(
            _db_url(),
            pool_pre_ping=True,
            pool_size=1,
            max_overflow=1,
        )
        SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
        try:
            timeout = httpx.Timeout(REQUEST_READ_TIMEOUT, connect=REQUEST_CONNECT_TIMEOUT)
            limits = httpx.Limits(max_connections=1, max_keepalive_connections=1)
            async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
                for m in messages:
                    await _process_message(m, client=client, SessionLocal=SessionLocal)
        finally:
            await engine.dispose()

    asyncio.run(_run())
    return {"processed": len(messages)}
