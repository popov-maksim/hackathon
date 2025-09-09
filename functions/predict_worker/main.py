import os
import json
import time
import logging
import asyncio

import httpx
from pythonjsonlogger import jsonlogger
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from common.models import Run, Prediction
from common.utils import normalize_pred
from common.config import REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT


MAX_CONC = int(os.getenv("WORKER_MAX_CONCURRENCY", "10") or 10)


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

async def _process_message(
    msg: dict,
    *,
    client: httpx.AsyncClient | None = None,
    SessionLocal: async_sessionmaker | None = None,
):
    run_id = int(msg["run_id"])
    endpoint_url = str(msg["endpoint_url"]).rstrip("/")
    sample_idx = int(msg["sample_idx"])
    sample = str(msg.get("sample", ""))
    gold = msg.get("gold", [])

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
        if resp.status_code == 200:
            data = resp.json()
            pred_json = normalize_pred(data)
            ok = True
            latency_ms = (time.perf_counter() - t0) * 1000.0
        else:
            logger.info("REQUEST ERROR", extra={'status_code': resp.status_code, 'text': resp.text})
    except Exception as e:
        logger.info("REQUEST ERROR", extra={'error': type(e)})

    # DB session per message, using factory provided for this invocation
    assert SessionLocal is not None, "SessionLocal must be provided"
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

                if ok:
                    await db.execute(
                        update(Run)
                        .where(Run.id == run_id)
                        .values(samples_success=Run.samples_success + 1)
                    )
            except IntegrityError:
                pass


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

    async def _run():
        sem = asyncio.Semaphore(MAX_CONC if MAX_CONC > 0 else 1)
        engine = create_async_engine(
            _db_url(),
            pool_pre_ping=True,
            pool_size=MAX_CONC if MAX_CONC > 0 else 1,
            max_overflow=MAX_CONC if MAX_CONC > 0 else 1,
        )
        SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
        try:
            timeout = httpx.Timeout(REQUEST_READ_TIMEOUT, connect=REQUEST_CONNECT_TIMEOUT)
            async with httpx.AsyncClient(timeout=timeout) as client:
                async def _task(m: dict):
                    async with sem:
                        await _process_message(m, client=client, SessionLocal=SessionLocal)
                await asyncio.gather(*[_task(m) for m in messages])
        finally:
            await engine.dispose()

    asyncio.run(_run())
    return {"processed": len(messages)}
