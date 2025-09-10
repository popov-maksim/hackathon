import os
import json
import time
import logging
import asyncio

import httpx
from pythonjsonlogger import jsonlogger
from sqlalchemy import update, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from common.models import Run, Prediction
from common.utils import normalize_pred
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
