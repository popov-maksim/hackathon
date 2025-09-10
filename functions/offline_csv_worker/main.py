import os
import io
import csv
import json
import base64
import logging
import asyncio
from itertools import zip_longest

import boto3
from pythonjsonlogger import jsonlogger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from common.utils import parse_annotation_literal, f1_macro
from common.models import RunCSV
from common.config import (
    S3_ENDPOINT_URL,
    S3_REGION,
    ACCESS_KEY,
    SECRET_KEY,
)


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
logger.setLevel(logging.INFO)


def _db_url() -> str:
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")
    return f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


def _s3_client():
    kwargs = {
        "service_name": "s3",
        "endpoint_url": S3_ENDPOINT_URL,
        "region_name": S3_REGION,
    }
    if ACCESS_KEY and SECRET_KEY:
        kwargs.update({
            "aws_access_key_id": ACCESS_KEY,
            "aws_secret_access_key": SECRET_KEY,
        })
    return boto3.client(**kwargs)


def _compute_f1_from_s3_bytes(gold_bytes: bytes, pred_bytes: bytes) -> float:
    gold_text = gold_bytes.decode("utf-8-sig", errors="ignore")
    pred_text = pred_bytes.decode("utf-8-sig", errors="ignore")
    gold_reader = csv.DictReader(io.StringIO(gold_text), delimiter=';')
    pred_reader = csv.DictReader(io.StringIO(pred_text), delimiter=';')
    pairs = []
    for gold_row, pred_row in zip_longest(gold_reader, pred_reader):
        g = parse_annotation_literal((gold_row or {}).get("annotation", ""))
        p = parse_annotation_literal((pred_row or {}).get("annotation", ""))
        pairs.append((g, p))
    logger.info("PAIRS", extra={"pairs": pairs})
    return float(f1_macro(pairs)) if pairs else 0.0


def handler(event, context):
    """
    HTTP-триггер для оценки CSV в оффлайн режиме.

    Ожидает JSON тело:
    {
      "run_csv_id": int,
      "s3_bucket": str,
      "s3_pred_key": str,
      "s3_gold_key": str,
    }
    """
    logger.info("EVENT", extra=event)

    try:
        body = event.get("body") if isinstance(event, dict) else None
        if body and event.get("isBase64Encoded"):
            body = base64.b64decode(body).decode("utf-8")
        payload = json.loads(body or "{}")
        logger.info("PAYLOAD", extra=payload)
    except Exception as e:
        logger.error("BAD_REQUEST", extra={"error": str(e)})
        return {"statusCode": 400, "body": json.dumps({"error": "invalid json"})}

    async def _run(m: dict):
        engine = create_async_engine(
            _db_url(),
            pool_pre_ping=True,
            pool_size=1,
            max_overflow=1,
        )
        SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
        try:
            run_csv_id = int(m.get("run_csv_id"))
            bucket = str(m.get("s3_bucket"))
            key_gold = str(m.get("s3_gold_key"))
            key_pred = str(m.get("s3_pred_key"))

            s3 = _s3_client()
            gold_obj = s3.get_object(Bucket=bucket, Key=key_gold)
            pred_obj = s3.get_object(Bucket=bucket, Key=key_pred)
            gold_bytes = gold_obj["Body"].read()
            pred_bytes = pred_obj["Body"].read()

            f1_val = _compute_f1_from_s3_bytes(gold_bytes, pred_bytes)

            async with SessionLocal() as db:
                row = (await db.execute(select(RunCSV).where(RunCSV.id == run_csv_id))).scalar_one_or_none()
                if row is not None:
                    row.f1 = float(f1_val)
                    await db.commit()
        finally:
            await engine.dispose()

    try:
        asyncio.run(_run(payload))
        return {"statusCode": 200, "headers": {"content-type": "application/json"}, "body": json.dumps({"processed": 1})}
    except Exception as e:
        logger.error("PROCESS_ERROR", extra={"error": str(e), "payload": payload})
        return {"statusCode": 500, "headers": {"content-type": "application/json"}, "body": json.dumps({"error": "processing failed"})}
