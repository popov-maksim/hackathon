import os
import io
import csv
import json
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import boto3
from sqlalchemy.ext.asyncio import AsyncSession
import httpx
from sqlalchemy import select, func
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from common.db import get_session, async_engine
from common.models import Base, Team, Phase, Run, RunCSV
from common.schemas import (RegisterTeamIn, TeamOut, CreatePhaseOut,
                            StartRunIn, StartRunOut, RunStatusOut, LeaderboardOut, LeaderboardItem,
                            RunCSVStartOut, RunCSVStatusOut)
from common.config import (
    DATASETS_DIR,
    YMQ_ENDPOINT_URL,
    YMQ_REGION,
    YMQ_QUEUE_URL,
    SQS_SEND_BATCH_MAX,
    S3_ENDPOINT_URL,
    S3_REGION,
    S3_OFFLINE_BUCKET,
    ACCESS_KEY,
    SECRET_KEY,
    S3_DATASETS_PREFIX,
    S3_RUNS_CSV_PREFIX,
    OFFLINE_CF_URL,
)
from common.constants import RunStatus
from common.utils import parse_annotation_literal


@asynccontextmanager
async def lifespan(_app: FastAPI):
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title="Hackathon NER API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _sqs_client():
    kwargs = {
        "service_name": "sqs",
        "endpoint_url": YMQ_ENDPOINT_URL,
        "region_name": YMQ_REGION,
    }
    if ACCESS_KEY and SECRET_KEY:
        kwargs.update({
            "aws_access_key_id": ACCESS_KEY,
            "aws_secret_access_key": SECRET_KEY,
        })
    return boto3.client(**kwargs)


def _s3_client():
    if not S3_OFFLINE_BUCKET:
        raise RuntimeError("S3_OFFLINE_BUCKET is not configured")
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


def _publish_run_messages(team: Team, phase: Phase, run: Run) -> int:
    if not YMQ_QUEUE_URL:
        raise RuntimeError("YMQ_QUEUE_URL is not configured")

    dataset_path = f"{DATASETS_DIR}/{phase.dataset_filename}"

    if not os.path.exists(dataset_path):
        raise FileNotFoundError("Dataset file not found")

    client = _sqs_client()

    total = 0
    batch = []

    rows_limit = phase.n_csv_rows

    with open(dataset_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for idx, row in enumerate(reader):
            if rows_limit is not None and idx >= rows_limit:
                break
            sample = row.get("sample", "")
            gold = parse_annotation_literal(row.get("annotation", ""))
            body = json.dumps({
                "run_id": run.id,
                "team_id": team.id,
                "endpoint_url": team.endpoint_url,
                "sample_idx": idx,
                "sample": sample,
                "gold": gold,
            }, ensure_ascii=False)
            batch.append({"Id": f"{run.id}-{idx}", "MessageBody": body})
            total += 1
            if len(batch) >= SQS_SEND_BATCH_MAX:
                client.send_message_batch(QueueUrl=YMQ_QUEUE_URL, Entries=batch)
                batch.clear()
    if batch:
        client.send_message_batch(QueueUrl=YMQ_QUEUE_URL, Entries=batch)
    return total


@app.get("/health")
async def health():
    """Проверка доступности API"""
    return {"status": "ok"}


@app.get("/teams/{tg_chat_id}", response_model=TeamOut)
async def get_team(tg_chat_id: int, db: AsyncSession = Depends(get_session)):
    """Получение команды по ID чата в телеграме"""
    query = select(Team).where(Team.tg_chat_id == tg_chat_id)
    result = await db.execute(query)
    team = result.scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="Команда не найдена")
    return TeamOut(team_id=team.id, name=team.name, endpoint_url=str(team.endpoint_url), github_url=team.github_url)


@app.post("/teams/register", response_model=TeamOut)
async def register_team(payload: RegisterTeamIn, db: AsyncSession = Depends(get_session)):
    """Регистрация команды"""
    query = select(Team).where(Team.tg_chat_id == payload.tg_chat_id)
    result = await db.execute(query)
    team = result.scalar_one_or_none()
    if team is None:
        team = Team(
            tg_chat_id=payload.tg_chat_id,
            name=payload.team_name,
            endpoint_url=str(payload.endpoint_url),
            github_url=payload.github_url,
        )
        db.add(team)
        await db.commit()
        await db.refresh(team)
    else:
        team.endpoint_url = str(payload.endpoint_url)
        if payload.github_url is not None:
            team.github_url = payload.github_url
        await db.commit()
    return TeamOut(team_id=team.id, name=team.name, endpoint_url=str(team.endpoint_url), github_url=team.github_url)


@app.post("/admin/phases", response_model=CreatePhaseOut)
async def create_competition_phase(
    name: str = Form(...),
    file: UploadFile = File(...),
    n_csv_rows: int | None = Form(None),
    db: AsyncSession = Depends(get_session),
):
    """
    Создание нового этапа соревнования с загрузкой датасета.

    Ожидает multipart/form-data:
    - name: str — название этапа (уникально)
    - file: UploadFile — CSV датасет (разделитель ';')
    - n_csv_rows: int | None — максимальное число строк (None/0 = весь датасет)
    """
    query = select(Phase).where(Phase.name == name)
    result = await db.execute(query)
    phase = result.scalar_one_or_none()
    if phase is not None:
        raise HTTPException(status_code=400, detail="Этап с таким названием уже существует")

    filename = f"{name}_{file.filename}"
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Ожидается CSV файл")

    os.makedirs(DATASETS_DIR, exist_ok=True)
    full_path = os.path.join(DATASETS_DIR, filename)
    try:
        with open(full_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось сохранить файл: {e}")

    try:
        if S3_OFFLINE_BUCKET:
            s3 = _s3_client()
            with open(full_path, "rb") as f:
                s3.put_object(Bucket=S3_OFFLINE_BUCKET, Key=f"{S3_DATASETS_PREFIX}{filename}", Body=f.read(), ContentType="text/csv")
    except Exception:
        pass

    phase = Phase(name=name, dataset_filename=filename, n_csv_rows=n_csv_rows)
    db.add(phase)
    await db.commit()
    await db.refresh(phase)
    return CreatePhaseOut(
        phase_id=phase.id,
        name=phase.name,
        dataset_filename=phase.dataset_filename,
        n_csv_rows=phase.n_csv_rows,
    )


@app.get("/phases/current/dataset")
async def download_current_phase_dataset(tg_chat_id: int, db: AsyncSession = Depends(get_session)):
    team = (await db.execute(select(Team).where(Team.tg_chat_id == tg_chat_id))).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="Команда не найдена")

    result = await db.execute(select(Phase).order_by(Phase.created_at.desc()).limit(1))
    phase = result.scalars().first()
    if phase is None:
        raise HTTPException(status_code=404, detail="Нет текущего этапа")

    full_path = f"{DATASETS_DIR}/{phase.dataset_filename}"
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="Файл датасета не найден")

    base_name, _ = os.path.splitext(phase.dataset_filename)
    out_name = f"{base_name}_samples.csv"

    def iter_csv():
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter=";")
        writer.writerow(["sample"])
        yield buf.getvalue().encode("utf-8")
        buf.seek(0)
        buf.truncate(0)

        with open(full_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                writer.writerow([row.get("sample", "")])
                yield buf.getvalue().encode("utf-8")
                buf.seek(0)
                buf.truncate(0)

    headers = {"Content-Disposition": f'attachment; filename="{out_name}"'}
    return StreamingResponse(iter_csv(), media_type="text/csv", headers=headers)


@app.post("/runs_csv/upload", response_model=RunCSVStartOut)
async def upload_run_csv(
    tg_chat_id: int = Form(...),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_session),
):
    """Загрузить CSV предсказаний в S3 и вызвать функцию оценки через Cloud Functions."""
    if not S3_OFFLINE_BUCKET:
        raise HTTPException(status_code=500, detail="S3 bucket is not configured")
    if not OFFLINE_CF_URL:
        raise HTTPException(status_code=500, detail="OFFLINE_CF_URL is not configured")

    team = (await db.execute(select(Team).where(Team.tg_chat_id == tg_chat_id))).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="Команда не найдена")

    # Запрет параллельных запусков: нельзя запускать оффлайн, если
    # уже есть активный онлайн-запуск или незавершённая оффлайн-оценка
    active_run_query = (
        select(Run)
        .where(Run.team_id == team.id)
        .where(Run.status.in_([RunStatus.QUEUED, RunStatus.RUNNING]))
        .limit(1)
    )
    if (await db.execute(active_run_query)).scalar_one_or_none() is not None:
        raise HTTPException(status_code=409, detail="Нельзя запускать оффлайн-оценку во время активной онлайн-оценки")

    last_csv = (
        await db.execute(
            select(RunCSV)
            .where(RunCSV.team_id == team.id)
            .order_by(RunCSV.created_at.desc())
            .limit(1)
        )
    ).scalars().first()
    if last_csv is not None and last_csv.f1 is None:
        raise HTTPException(status_code=409, detail="У команды уже есть активная оффлайн-оценка")

    phase = (await db.execute(select(Phase).order_by(Phase.created_at.desc()).limit(1))).scalars().first()
    if phase is None:
        raise HTTPException(status_code=404, detail="Соревнование не стартовало")

    try:
        pred_bytes = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Не удалось прочитать файл: {e}")

    run_csv = RunCSV(team_id=team.id, phase_id=phase.id, precision=None, recall=None, f1=None)
    db.add(run_csv)
    await db.commit()
    await db.refresh(run_csv)

    s3 = _s3_client()
    filename = f"{phase.name}_{file.filename}"
    gold_key = f"{S3_DATASETS_PREFIX}{filename}"
    try:
        s3.head_object(Bucket=S3_OFFLINE_BUCKET, Key=gold_key)
    except Exception:
        local_path = os.path.join(DATASETS_DIR, filename)
        if not os.path.exists(local_path):
            raise HTTPException(status_code=404, detail="Файл датасета не найден для выгрузки в S3")
        with open(local_path, "rb") as f:
            s3.put_object(Bucket=S3_OFFLINE_BUCKET, Key=gold_key, Body=f.read(), ContentType="text/csv")

    pred_key = f"{S3_RUNS_CSV_PREFIX}{run_csv.id}/predictions.csv"
    s3.put_object(Bucket=S3_OFFLINE_BUCKET, Key=pred_key, Body=pred_bytes, ContentType="text/csv")

    payload = {
        "run_csv_id": run_csv.id,
        "s3_bucket": S3_OFFLINE_BUCKET,
        "s3_pred_key": pred_key,
        "s3_gold_key": gold_key,
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(OFFLINE_CF_URL, json=payload)
            resp.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Не удалось вызвать функцию оценки для вашего файла: {e}")

    return RunCSVStartOut(run_csv_id=run_csv.id, status="queued")


@app.get("/teams/{tg_chat_id}/last_csv", response_model=RunCSVStatusOut)
async def get_last_csv_status(tg_chat_id: int, db: AsyncSession = Depends(get_session)):
    team = (await db.execute(select(Team).where(Team.tg_chat_id == tg_chat_id))).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="Команда не найдена")

    last = (
        await db.execute(
            select(RunCSV)
            .where(RunCSV.team_id == team.id)
            .order_by(RunCSV.created_at.desc())
            .limit(1)
        )
    ).scalars().first()
    if last is None:
        raise HTTPException(status_code=404, detail="Нет оффлайн-оценок для команды")
    status = "done" if last.f1 is not None else "running"
    return RunCSVStatusOut(run_csv_id=last.id, status=status, f1=last.f1, precision=last.precision, recall=last.recall)


@app.get("/teams/{tg_chat_id}/best_csv", response_model=RunCSVStatusOut)
async def get_best_csv_status(tg_chat_id: int, db: AsyncSession = Depends(get_session)):
    """Лучший оффлайн-результат команды (по максимальному F1)."""
    team = (await db.execute(select(Team).where(Team.tg_chat_id == tg_chat_id))).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="Команда не найдена")
    best = (
        await db.execute(
            select(RunCSV)
            .where(RunCSV.team_id == team.id, RunCSV.f1.isnot(None))
            .order_by(RunCSV.f1.desc(), RunCSV.created_at.asc())
            .limit(1)
        )
    ).scalars().first()
    if best is None:
        raise HTTPException(status_code=404, detail="Нет завершённых оффлайн-оценок для команды")
    return RunCSVStatusOut(run_csv_id=best.id, status="done", f1=best.f1, precision=best.precision, recall=best.recall)


@app.post("/runs/start", response_model=StartRunOut)
async def start_run(payload: StartRunIn, db: AsyncSession = Depends(get_session)):
    """Запустить оценку через Yandex Message Queue и Cloud Functions."""
    team = (await db.execute(select(Team).where(Team.tg_chat_id == payload.tg_chat_id))).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="Команда не найдена")

    active_run_query = (
        select(Run)
        .where(Run.team_id == team.id)
        .where(Run.status.in_([RunStatus.QUEUED, RunStatus.RUNNING]))
        .limit(1)
    )
    if (await db.execute(active_run_query)).scalar_one_or_none() is not None:
        raise HTTPException(status_code=409, detail="У команды уже есть активный запуск")

    # Запрет параллельных запусков: нельзя запускать онлайн, если есть незавершённая оффлайн-оценка
    last_csv = (
        await db.execute(
            select(RunCSV)
            .where(RunCSV.team_id == team.id)
            .order_by(RunCSV.created_at.desc())
            .limit(1)
        )
    ).scalars().first()
    if last_csv is not None and last_csv.f1 is None:
        raise HTTPException(status_code=409, detail="Нельзя запускать онлайн-оценку во время активной оффлайн-оценки")

    result = await db.execute(select(Phase).order_by(Phase.created_at.desc()).limit(1))
    phase = result.scalars().first()
    if phase is None:
        raise HTTPException(status_code=404, detail="Соревнование не стартовало")

    run = Run(
        team_id=team.id,
        phase_id=phase.id,
        status=RunStatus.RUNNING,
        started_at=datetime.now(timezone.utc),
        samples_total=0,
        samples_processed=0,
        samples_success=0,
    )
    db.add(run)
    await db.commit()
    await db.refresh(run)

    try:
        total = _publish_run_messages(team, phase, run)
    except Exception as e:
        res = await db.execute(select(Run).where(Run.id == run.id))
        r = res.scalar_one()
        r.status = RunStatus.QUEUED
        await db.commit()
        raise HTTPException(status_code=500, detail=f"Не удалось поставить задачи в очередь: {e}")

    res = await db.execute(select(Run).where(Run.id == run.id))
    r = res.scalar_one()
    r.samples_total = total
    await db.commit()

    return StartRunOut(run_id=run.id, status=run.status)


@app.get("/runs/{run_id}/status", response_model=RunStatusOut)
async def run_status(run_id: int, db: AsyncSession = Depends(get_session)):
    """Получение статуса запуска"""
    run = (await db.execute(select(Run).where(Run.id == run_id))).scalar_one_or_none()
    if run is None:
        raise HTTPException(status_code=404, detail="Запуск теста с таким ID не найден")
    return RunStatusOut(
        run_id=run.id,
        status=run.status,
        samples_processed=run.samples_processed,
        samples_success=run.samples_success,
        samples_total=run.samples_total,
        avg_latency_ms=run.avg_latency_ms,
        f1=run.f1,
    )


@app.get("/teams/{tg_chat_id}/last_run", response_model=RunStatusOut)
async def get_last_run_status(tg_chat_id: int, db: AsyncSession = Depends(get_session)):
    """Получение статуса последнего запуска командой"""
    team = (await db.execute(select(Team).where(Team.tg_chat_id == tg_chat_id))).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="Команда не найдена")

    last_run_query = (
        select(Run)
        .where(Run.team_id == team.id)
        .order_by(Run.created_at.desc())
        .limit(1)
    )
    last_run = (await db.execute(last_run_query)).scalars().first()
    if last_run is None:
        raise HTTPException(status_code=404, detail="У данной команды еще не было запусков")

    return RunStatusOut(
        run_id=last_run.id,
        status=last_run.status,
        samples_processed=last_run.samples_processed,
        samples_success=last_run.samples_success,
        samples_total=last_run.samples_total,
        avg_latency_ms=last_run.avg_latency_ms,
        f1=last_run.f1,
    )


@app.get("/leaderboard", response_model=LeaderboardOut)
async def leaderboard(phase_id: int | None = None, db: AsyncSession = Depends(get_session)):
    """
    Лидерборд по текущему этапу. По умолчанию — по текущему (последнему) этапу.
    Для каждой команды берётся лучший F1 на выбранном этапе. При равенстве F1 берём меньшую задержку.
    """
    if phase_id is None:
        res = await db.execute(select(Phase).order_by(Phase.created_at.desc()).limit(1))
        phase = res.scalars().first()
        if phase is None:
            raise HTTPException(status_code=404, detail="Нет этапов")
        pid = phase.id
    else:
        res = await db.execute(select(Phase).where(Phase.id == phase_id))
        phase = res.scalar_one_or_none()
        if phase is None:
            raise HTTPException(status_code=404, detail="Этап не найден")
        pid = phase.id

    rn = func.row_number().over(
        partition_by=Run.team_id,
        order_by=(func.coalesce(Run.f1, 0.0).desc(), func.coalesce(Run.avg_latency_ms, 1e9).asc())
    )

    subq = (
        select(
            Run.team_id.label("team_id"),
            Run.f1.label("f1"),
            Run.avg_latency_ms.label("lat"),
            rn.label("rn"),
        )
        .where(Run.phase_id == pid, Run.status == RunStatus.DONE)
        .subquery()
    )

    res = await db.execute(
        select(Team.name, subq.c.f1, subq.c.lat)
        .join(Team, Team.id == subq.c.team_id)
        .where(subq.c.rn == 1)
        .order_by(func.coalesce(subq.c.f1, 0.0).desc(), func.coalesce(subq.c.lat, 1e9).asc(), Team.name.asc())
    )
    rows = res.all()

    return LeaderboardOut(
        phase_id=pid,
        items=[
            LeaderboardItem(
                team_name=name,
                avg_latency_ms=float(lat or 0.0),
                f1=float(f1 or 0.0)
            )
            for (name, f1, lat) in rows
        ]
    )
