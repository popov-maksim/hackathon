import csv
import time
import asyncio
from datetime import datetime, timezone

import httpx
from celery import Celery
from sqlalchemy import select

from common.config import (
    REDIS_URL,
    REQUEST_CONNECT_TIMEOUT,
    REQUEST_READ_TIMEOUT,
    RATE_LIMIT_SECONDS,
    DATASETS_DIR,
    N_CSV_ROWS,
    RUN_TIME_LIMIT_SECONDS,
)
from common.db import AsyncSessionLocal
from common.constants import RunStatus
from common.models import Team, Phase, Run, Prediction
from common.utils import f1_macro, parse_annotation_literal


celery_app = Celery("tasks", broker=REDIS_URL)
celery_app.conf.task_routes = {"tasks.*": {"queue": "runs"}}


@celery_app.task(name="tasks.start_run")
def start_run(run_id: int):
    asyncio.run(run(run_id))


async def run(run_id: int):
    """Проверка решения участника бомбежкой эндпоинта"""
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Run).where(Run.id == run_id))
        run: Run | None = result.scalar_one_or_none()
        if run is None:
            return

        team = (await db.execute(select(Team).where(Team.id == run.team_id))).scalar_one()
        phase = (await db.execute(select(Phase).where(Phase.id == run.phase_id))).scalar_one()

        run.status = RunStatus.RUNNING
        run.started_at = datetime.now(timezone.utc)
        await db.commit()

        dataset_path = f"{DATASETS_DIR}/{phase.dataset_filename}"

        timeout = httpx.Timeout(connect=REQUEST_CONNECT_TIMEOUT, read=REQUEST_READ_TIMEOUT)

        predictions_to_save: list[Prediction] = []
        gold_pred_pairs = []
        samples_total = 0
        samples_success = 0
        latencies: list[float] = []
        t_start = time.perf_counter()

        async with httpx.AsyncClient(timeout=timeout) as client:
            with open(dataset_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    if time.perf_counter() - t_start > RUN_TIME_LIMIT_SECONDS:
                        break
                    if samples_total >= N_CSV_ROWS:
                        break

                    sample = row.get("sample", "")
                    gold = parse_annotation_literal(row.get("annotation", ""))

                    url = team.endpoint_url.rstrip("/")
                    payload = {"input": sample}
                    t0 = time.perf_counter()
                    ok = False
                    pred_json = None
                    latency_ms = None
                    try:
                        resp = await client.post(url, json=payload)
                        latency_ms = (time.perf_counter() - t0) * 1000.0
                        if resp.status_code == 200:
                            data = resp.json()
                            pred_json = normalize_pred(data)
                            ok = True
                    except Exception:
                        pass

                    samples_total += 1
                    if ok:
                        samples_success += 1
                    if latency_ms is not None:
                        latencies.append(latency_ms)

                    predictions_to_save.append(
                        Prediction(
                            run_id=run.id,
                            latency_ms=latency_ms,
                            ok=ok,
                            gold_json=gold,
                            pred_json=pred_json,
                        )
                    )
                    gold_pred_pairs.append((gold, pred_json or []))

                    if RATE_LIMIT_SECONDS > 0:
                        await asyncio.sleep(RATE_LIMIT_SECONDS)

        if predictions_to_save:
            db.add_all(predictions_to_save)

        avg_latency_ms = (sum(latencies) / len(latencies)) if latencies else None
        f1_val = f1_macro(gold_pred_pairs) if gold_pred_pairs else 0.0

        run.samples_total = samples_total
        run.samples_success = samples_success
        run.avg_latency_ms = avg_latency_ms
        run.f1 = f1_val
        run.finished_at = datetime.now(timezone.utc)
        run.status = RunStatus.DONE

        await db.commit()
