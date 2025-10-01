import math
import os
import json
import time
import logging
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List

import aiohttp
import stamina
import numpy as np
from pythonjsonlogger import jsonlogger
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from common.models import Run, Prediction
from common.utils import normalize_pred, f1_macro
from common.constants import RunStatus
from common.config import REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT


CONCURRENCY = int(os.getenv("CONCURRENCY", "100"))
RETRIES = int(os.getenv("RETRIES", "2"))
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_name = os.getenv("POSTGRES_DB")
db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")
async_dsn = f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


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

    pairs = [(val["gold_json"], val["pred_json"]) for val in values]
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
                    avg_latency_ms=float(np.median(latencies)) if latencies else None,
                    f1=f1_macro(pairs) if pairs else 0.0,
                    finished_at=now,
                    status=RunStatus.DONE,
                    samples_processed=len(values),
                    samples_success=sum(int(it["ok"]) for it in values),
                )
            )


async def make_request(session: aiohttp.ClientSession, url: str, data: dict) -> tuple[bool, float, str]:
    """
    :return: [успешный ли ответ, время в мс, текст тела ответа]
    """
    # Одна попытка: не-200 → не исключение (retry не нужен), сетевые/таймауты → исключение (retry)
    @stamina.retry(on=Exception, attempts=RETRIES)
    async def make_attempt() -> tuple[bool, float, str]:
        body = None
        ok = False
        start_ns = time.perf_counter_ns()
        async with session.post(url, json=data) as resp:
            end_ns = time.perf_counter_ns()
            if resp.status == 200:
                body = await resp.text()
                ok = True
        return ok, (end_ns - start_ns) / 1e6, body

    try:
        return await make_attempt()
    except Exception as e:
        return False, None, None


async def _run(run_id: int, messages: list[dict[str, str]]):
    try:
        data_to_save = []
        groups_count = math.ceil(len(messages) / CONCURRENCY)

        for group_number in range(groups_count):
            logger.info(f"Handling group {group_number+1}/{groups_count}", extra={'run_id': run_id})

            start_index = group_number * CONCURRENCY
            end_index = start_index + CONCURRENCY
            current_messages = messages[start_index: end_index]

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as aiohttp_client:
                tasks = [
                    make_request(
                        aiohttp_client,
                        msg["endpoint_url"],
                        {"input": msg["sample"]}
                    ) for msg in current_messages
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

            for ((is_passed, latency, response_body), msg) in zip(results, current_messages):
                data_to_save.append({
                    "run_id": run_id,
                    "sample_idx": int(msg["sample_idx"]),
                    "latency_ms": latency if is_passed else None,
                    "ok": is_passed,
                    "gold_json": msg.get("gold", []),
                    "pred_json": normalize_pred(json.loads(response_body)) if is_passed and response_body else [],
                })

            logger.info("done handling group")

        engine = create_async_engine(
            async_dsn,
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=2,
        )
        SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
        await _finalize_run(SessionLocal=SessionLocal, run_id=run_id, predictions=data_to_save)
    except Exception as e:
        logger.error("RUN ERROR", extra={'error': type(e).__name__, 'str': f'{e}'})


def handler(event, context):
    logger.info("EVENT", extra=event)
    logger.info("EVENT_KEYS", extra={'keys': list(event.keys()) if isinstance(event, dict) else None})

    body = event.get("body", None)
    if body is not None:
        body = json.loads(body)
    else:
        return {"processed": 0}

    items = body["items"]
    run_id = int(body.get("run_id"))
    endpoint_url = str(body.get("endpoint_url", "")).rstrip("/")

    sample_messages = []
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

    asyncio.run(_run(run_id, sample_messages))
    return {"processed": len(sample_messages)}
