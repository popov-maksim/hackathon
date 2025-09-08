import os
import io
import csv
import json
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import boto3
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from common.db import get_session, async_engine
from common.models import Base, Team, Phase, Run
from common.schemas import (RegisterTeamIn, TeamOut, CreatePhaseOut,
                            StartRunIn, StartRunOut, RunStatusOut, LeaderboardOut, LeaderboardItem)
from common.config import (
    DATASETS_DIR,
    YMQ_ENDPOINT_URL,
    YMQ_REGION,
    YMQ_QUEUE_URL,
    YMQ_ACCESS_KEY,
    YMQ_SECRET_KEY,
    YMQ_SESSION_TOKEN,
    SQS_SEND_BATCH_MAX,
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
    if YMQ_ACCESS_KEY and YMQ_SECRET_KEY:
        kwargs.update({
            "aws_access_key_id": YMQ_ACCESS_KEY,
            "aws_secret_access_key": YMQ_SECRET_KEY,
        })
        if YMQ_SESSION_TOKEN:
            kwargs["aws_session_token"] = YMQ_SESSION_TOKEN
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
    rows_limit = int(phase.n_csv_rows) if getattr(phase, "n_csv_rows", None) not in (None, 0) else None
    with open(dataset_path, newline="", encoding="utf-8") as f:
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
    return TeamOut(team_id=team.id, name=team.name)


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
            endpoint_url=str(payload.endpoint_url)
        )
        db.add(team)
        await db.commit()
        await db.refresh(team)
    else:
        team.endpoint_url = str(payload.endpoint_url)
        await db.commit()
    return TeamOut(team_id=team.id, name=team.name)


@app.post("/admin/phases", response_model=CreatePhaseOut)
async def create_phase(
    name: str = Form(...),
    file: UploadFile = File(...),
    n_csv_rows: int | None = Form(None),
    db: AsyncSession = Depends(get_session),
):
    """Создание нового этапа соревнования с загрузкой датасета.

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
    # Validate file
    filename = file.filename
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Ожидается CSV файл")

    # Save file
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

    # Create phase
    nr = int(n_csv_rows) if n_csv_rows not in (None, 0) else None
    phase = Phase(name=name, dataset_filename=filename, n_csv_rows=nr)
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
        buf.seek(0); buf.truncate(0)

        with open(full_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                writer.writerow([row.get("sample", "")])
                yield buf.getvalue().encode("utf-8")
                buf.seek(0); buf.truncate(0)

    headers = {"Content-Disposition": f'attachment; filename="{out_name}"'}
    return StreamingResponse(iter_csv(), media_type="text/csv", headers=headers)


@app.post("/runs/start", response_model=StartRunOut)
async def start_run(payload: StartRunIn, db: AsyncSession = Depends(get_session)):
    """Запустить оценку через Yandex Message Queue и Cloud Functions."""
    # 1) Validate team and no active run
    query = select(Team).where(Team.tg_chat_id == payload.tg_chat_id)
    result = await db.execute(query)
    team = result.scalar_one_or_none()
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

    # 2) Current phase
    result = await db.execute(select(Phase).order_by(Phase.created_at.desc()).limit(1))
    phase = result.scalars().first()
    if phase is None:
        raise HTTPException(status_code=404, detail="Нет текущего этапа")

    # 3) Create run (RUNNING), compute samples_total, publish messages
    run = Run(
        team_id=team.id,
        phase_id=phase.id,
        status=RunStatus.RUNNING,
        started_at=datetime.now(timezone.utc),
        samples_total=0,
        samples_success=0,
    )
    async with db.begin():
        db.add(run)

    try:
        total = _publish_run_messages(team, phase, run)
    except Exception as e:
        # Best-effort: mark run back to queued on failure
        async with db.begin():
            res = await db.execute(select(Run).where(Run.id == run.id))
            r = res.scalar_one()
            r.status = RunStatus.QUEUED
        raise HTTPException(status_code=500, detail=f"Не удалось поставить задачи в очередь: {e}")

    async with db.begin():
        res = await db.execute(select(Run).where(Run.id == run.id))
        r = res.scalar_one()
        r.samples_total = total

    return StartRunOut(run_id=run.id, status=run.status)


@app.get("/runs/{run_id}/status", response_model=RunStatusOut)
async def run_status(run_id: int, db: AsyncSession = Depends(get_session)):
    """Получение статуса запуска"""
    result = await db.execute(select(Run).where(Run.id == run_id))
    run = result.scalar_one_or_none()
    if run is None:
        raise HTTPException(status_code=404, detail="Запуск теста с таким ID не найден")
    return RunStatusOut(
        run_id=run.id,
        status=run.status,
        samples_success=run.samples_success,
        samples_total=run.samples_total,
        avg_latency_ms=run.avg_latency_ms,
        f1=run.f1,
    )


@app.get("/teams/{tg_chat_id}/last_run", response_model=RunStatusOut)
async def get_last_run_status(tg_chat_id: int, db: AsyncSession = Depends(get_session)):
    """Получение статуса последнего запуска командой"""
    result = await db.execute(select(Team).where(Team.tg_chat_id == tg_chat_id))
    team = result.scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="Команда не найдена")
    last_run_query = (
        select(Run)
        .where(Run.team_id == team.id)
        .order_by(Run.created_at.desc())
        .limit(1)
    )
    result = await db.execute(last_run_query)
    last_run = result.scalars().first()
    if last_run is None:
        raise HTTPException(status_code=404, detail="У данной команды еще не было запусков")
    return RunStatusOut(
        run_id=last_run.id,
        status=last_run.status,
        samples_success=last_run.samples_success,
        samples_total=last_run.samples_total,
        avg_latency_ms=last_run.avg_latency_ms,
        f1=last_run.f1,
    )


@app.get("/leaderboard", response_model=LeaderboardOut)
async def leaderboard(phase_id: int | None = None, db: AsyncSession = Depends(get_session)):
    """Лидерборд по этапу. По умолчанию — по текущему (последнему) этапу.
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

    items = [
        LeaderboardItem(team_name=name, avg_latency_ms=float(lat or 0.0), f1=float(f1 or 0.0))
        for (name, f1, lat) in rows
    ]
    return LeaderboardOut(phase_id=pid, items=items)
