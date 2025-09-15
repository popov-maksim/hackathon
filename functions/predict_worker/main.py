import os
import json
import time
import base64
import random
import logging
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx
from pythonjsonlogger import jsonlogger
from sqlalchemy import update, select, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from common.models import Run, Prediction
from common.utils import normalize_pred, f1_macro
from common.constants import RunStatus
from common.config import REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT


CONCURRENCY = int(os.getenv("CONCURRENCY", "100"))
RETRIES = int(os.getenv("RETRIES", "2"))


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


async def _finalize_run(*, SessionLocal: async_sessionmaker, run_id: int, predictions: List[Dict[str, Any]]) -> None:
    logger.info("FINALIZE RUN", extra={'run_id': run_id})

    now = datetime.now(timezone.utc)

    values = [
        {
            "run_id": it["run_id"],
            "sample_idx": it["sample_idx"],
            "latency_ms": it["latency_ms"],
            "ok": bool(it["ok"]),
            "gold_json": it["gold_json"],
            "pred_json": it["pred_json"],
        }
        for it in predictions
    ]

    pairs = [(val["gold_json"] or [], val["pred_json"] or []) for val in values]
    latencies = [val["latency_ms"] for val in values if val["latency_ms"] is not None]

    async with SessionLocal() as db:
        async with db.begin():
            stmt = (
                pg_insert(Prediction)
                .values(values)
                .on_conflict_do_nothing(constraint="ux_predictions_run_sample")
            )
            await db.execute(stmt)

            await db.execute(
                update(Run)
                .where(Run.id == run_id)
                .values(
                    avg_latency_ms=(sum(latencies) / len(latencies)) if latencies else None,
                    f1=f1_macro(pairs) if pairs else 0.0,
                    finished_at=now,
                    status=RunStatus.DONE,
                    samples_processed=len(values),
                    samples_success=sum(int(it["ok"]) for it in values),
                )
            )


def _safe_jsonable(x):
    try:
        json.dumps(x)
        return x
    except Exception:
        return str(x)


async def _post_with_retries(client, url, payload):
    delay = 0.05
    for attempt in range(RETRIES + 1):
        try:
            return await client.post(url, json=payload)
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout):
            if attempt == RETRIES:
                raise
            await asyncio.sleep(delay + random.random() * delay)
            delay *= 2


async def _process_message(msg: dict, *, client: httpx.AsyncClient) -> Dict[str, Any]:
    run_id = int(msg["run_id"])
    endpoint_url = str(msg["endpoint_url"]).rstrip("/")
    sample_idx = int(msg["sample_idx"])
    sample = str(msg.get("sample", ""))
    gold = msg.get("gold", [])

    latency_ms = None
    ok = False
    pred_json = None

    t_0 = time.perf_counter_ns()
    try:
        resp = await _post_with_retries(client, endpoint_url, {"input": sample})
        t_1 = time.perf_counter_ns()
        if resp.status_code == 200:
            data = resp.json()
            pred_json = normalize_pred(data)
            ok = True
        else:
            logger.info("REQUEST STATUS", extra={'status_code': resp.status_code, 'text': resp.text[:1000]})
    except Exception as e:
        t_1 = time.perf_counter_ns()
        logger.info("REQUEST ERROR", extra={'error': type(e).__name__, 'str': f'{e}'})

    latency_ms = (t_1 - t_0) / 1e6

    logger.info("done processing message", extra={
        'run_id': run_id, 'sample_idx': sample_idx, 'latency_ms': latency_ms,
        'ok': ok, 'gold_json': _safe_jsonable(gold), 'pred_json': _safe_jsonable(pred_json)})

    return {
        "run_id": run_id,
        "sample_idx": sample_idx,
        "latency_ms": latency_ms,
        "ok": ok,
        "gold_json": gold,
        "pred_json": pred_json,
    }


def handler(event, context):
    logger.info("REQUEST_READ_TIMEOUT", extra={'REQUEST_READ_TIMEOUT': REQUEST_READ_TIMEOUT})
    logger.info("REQUEST_CONNECT_TIMEOUT", extra={'REQUEST_CONNECT_TIMEOUT': REQUEST_CONNECT_TIMEOUT})
    logger.info("EVENT_KEYS", extra={'keys': list(event.keys()) if isinstance(event, dict) else None})

    sample_messages = []

    items = event["items"]

    run_id = int(event.get("run_id"))
    endpoint_url = str(event.get("endpoint_url", "")).rstrip("/")
    for it in items:
        try:
            sample_messages.append({
                "run_id": run_id,
                "endpoint_url": endpoint_url,
                "sample_idx": int(it.get("sample_idx", 0)),
                "sample": str(it.get("sample", "")),
                "gold": it.get("gold", []),
            })
        except Exception:
            logger.warning("BAD_ITEM_SKIPPED")

    logger.info("MESSAGES_PARSED", extra={'sample_count': len(sample_messages)})

    async def _run():
        engine = create_async_engine(
            _db_url(),
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=2,
        )
        SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
        try:
            timeout = httpx.Timeout(
                connect=REQUEST_CONNECT_TIMEOUT,
                read=REQUEST_READ_TIMEOUT,
                write=REQUEST_READ_TIMEOUT,
                pool=None,
            )
            limits = httpx.Limits(
                max_connections=CONCURRENCY,
                max_keepalive_connections=CONCURRENCY,
            )
            sem = asyncio.Semaphore(CONCURRENCY)

            async with httpx.AsyncClient(timeout=timeout, limits=limits, http2=True) as client:
                async def bounded_process(m: dict):
                    async with sem:
                        try:
                            return await _process_message(m, client=client)
                        except BaseException as e:
                            logger.warning("TASK_CRASHED", extra={
                                "run_id": m.get("run_id"), "sample_idx": m.get("sample_idx"), "error": repr(e)[:500]})
                            return {
                                "run_id": m.get("run_id"),
                                "sample_idx": m.get("sample_idx"),
                                "latency_ms": None,
                                "ok": False,
                                "gold_json": m.get("gold"),
                                "pred_json": None,
                            }

                tasks = [asyncio.create_task(bounded_process(m)) for m in sample_messages]
                results = await asyncio.gather(*tasks)

            await _finalize_run(SessionLocal=SessionLocal, run_id=run_id, predictions=results)
        finally:
            await engine.dispose()

    asyncio.run(_run())
    return {"processed": len(sample_messages)}
