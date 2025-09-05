import os
import io
import csv
from contextlib import asynccontextmanager

from celery import Celery
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from common.db import get_session, async_engine
from common.models import Base, Team, Phase, Run
from common.schemas import (RegisterTeamIn, TeamOut, CreatePhaseIn, CreatePhaseOut,
                            StartRunIn, StartRunOut, RunStatusOut, LeaderboardOut, LeaderboardItem)
from common.config import REDIS_URL, DATASETS_DIR
from common.constants import RunStatus


@asynccontextmanager
async def lifespan(app: FastAPI):
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

celery_app = Celery(broker=REDIS_URL)


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
async def create_phase(payload: CreatePhaseIn, db: AsyncSession = Depends(get_session)):
    """Создание нового этапа соревнования"""
    query = select(Phase).where(Phase.name == payload.name)
    result = await db.execute(query)
    phase = result.scalar_one_or_none()
    if phase is not None:
        raise HTTPException(status_code=400, detail="Этап с таким названием уже существует")
    phase = Phase(name=payload.name, dataset_filename=payload.dataset_filename)
    db.add(phase)
    await db.commit()
    await db.refresh(phase)
    return CreatePhaseOut(
        phase_id=phase.id,
        name=phase.name,
        dataset_filename=phase.dataset_filename
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
    """Запустить бомбардировку участника на данных текущего этапа"""
    async with db.begin():
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
        current_phase_query = (
            select(Phase)
            .order_by(Phase.created_at.desc())
            .limit(1)
        )
        result = await db.execute(current_phase_query)
        phase = result.scalars().first()
        if phase is None:
            raise HTTPException(status_code=404, detail="Нет текущего этапа")
        run = Run(
            team_id=team.id,
            phase_id=phase.id,
            status=RunStatus.QUEUED,
            started_at=None,
            samples_total=0,
            samples_success=0,
        )
        db.add(run)
        await db.flush()
    celery_app.send_task("tasks.start_run", args=[run.id], queue="runs")
    return StartRunOut(run_id=run.id, status=run.status)


@app.get("/runs/{run_id}/status", response_model=RunStatusOut)
async def run_status(run_id: int, db: AsyncSession = Depends(get_session)):
    """Получение статуса запуска"""
    query = select(Run).where(Run.id == run_id)
    result = await db.execute(query)
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
    query = select(Team).where(Team.tg_chat_id == tg_chat_id)
    result = await db.execute(query)
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
