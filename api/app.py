import os
import io
import csv
import logging
import asyncio
import aiofiles
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import httpx
import boto3
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Form

from common.db import get_session, async_engine, AsyncSessionLocal
from common.models import Base, Team, Phase, Run, RunCSV
from common.schemas import (RegisterTeamIn, TeamOut, CreatePhaseOut,
                            StartRunIn, StartRunOut, RunStatusOut, LeaderboardOut, LeaderboardItem,
                            RunCSVStartOut, RunCSVStatusOut, TeamWithEndpointOut)
from common.config import (
    DATASETS_DIR,
    S3_ENDPOINT_URL,
    S3_REGION,
    S3_OFFLINE_BUCKET,
    ACCESS_KEY,
    SECRET_KEY,
    S3_DATASETS_PREFIX,
    S3_RUNS_CSV_PREFIX,
    OFFLINE_CF_URL,
    PREDICT_CF_URL,
)
from common.constants import RunStatus, NULL_LAT_MS_RANK
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

logger = logging.getLogger(__name__)


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


async def _build_run_items(phase: Phase) -> list[dict]:
    """Асинхронно собирает элементы запуска из CSV."""
    dataset_path = f"{DATASETS_DIR}/{phase.dataset_filename}"
    if not os.path.exists(dataset_path):
        raise FileNotFoundError("Dataset file not found")

    items: list[dict] = []
    async with aiofiles.open(dataset_path, newline="", encoding="utf-8-sig") as f:
        content = await f.read()
        reader = csv.DictReader(io.StringIO(content), delimiter=";")
        for idx, row in enumerate(reader):
            if phase.n_csv_rows is not None and idx >= phase.n_csv_rows:
                break
            sample = row.get("sample", "")
            gold = parse_annotation_literal(row.get("annotation", ""))
            items.append({"sample_idx": idx, "sample": sample, "gold": gold})

    return items


@app.get("/health")
async def health():
    """Проверка доступности API"""
    return {"status": "ok"}


@app.get("/teams/with_endpoint", response_model=list[TeamWithEndpointOut])
async def list_teams_with_endpoint(db: AsyncSession = Depends(get_session)):
    """Список команд, у которых указан непустой endpoint_url."""
    res = await db.execute(
        select(Team)
        .where(Team.endpoint_url.isnot(None))
        .where(Team.endpoint_url != "")
        .order_by(Team.id.asc())
    )
    teams = res.scalars().all()
    return [
        TeamWithEndpointOut(
            tg_chat_id=int(t.tg_chat_id),
            name=t.name,
            endpoint_url=t.endpoint_url or "",
        )
        for t in teams
    ]


@app.post("/teams/register", response_model=TeamOut)
async def register_team(payload: RegisterTeamIn, db: AsyncSession = Depends(get_session)):
    """Регистрация команды с валидацией и проверкой уникальности имени."""
    # Нормализация и базовая валидация данных
    team_name = (payload.team_name or "").strip()
    tg_username = (payload.tg_username or "").strip()
    endpoint_url = str(payload.endpoint_url) if payload.endpoint_url is not None else None
    github_url = str(payload.github_url) if payload.github_url is not None else None

    if not team_name or len(team_name) > 128:
        raise HTTPException(status_code=422, detail="Некорректное имя команды (1-128 символов)")
    if not tg_username or len(tg_username) > 128:
        raise HTTPException(status_code=422, detail="Некорректный tg_username (1-128 символов)")
    if endpoint_url is not None and len(endpoint_url) > 512:
        raise HTTPException(status_code=422, detail="Слишком длинный endpoint_url (макс 512)")
    if github_url is not None and len(github_url) > 512:
        raise HTTPException(status_code=422, detail="Слишком длинный github_url (макс 512)")

    logger.info(
        "Register team request: tg_chat_id=%s, team_name=%s, tg_username=%s",
        payload.tg_chat_id,
        team_name,
        tg_username,
    )

    try:
        # Ищем команду по tg_chat_id
        query = select(Team).where(Team.tg_chat_id == payload.tg_chat_id)
        result = await db.execute(query)
        team = result.scalar_one_or_none()

        # Проверка занятости имени другой командой
        name_q = select(Team).where(Team.name == team_name)
        name_res = await db.execute(name_q)
        name_owner = name_res.scalar_one_or_none()
        if name_owner is not None and (team is None or name_owner.id != team.id):
            logger.warning("Team name already in use: %s", team_name)
            raise HTTPException(status_code=409, detail="Имя команды уже занято")

        if team is None:
            # Создание новой команды
            team = Team(
                tg_chat_id=payload.tg_chat_id,
                name=team_name,
                tg_username=tg_username,
                endpoint_url=endpoint_url,
                github_url=github_url,
            )
            db.add(team)
            await db.commit()
            await db.refresh(team)
            logger.info("Team created id=%s", team.id)
        else:
            changed_fields: list[str] = []
            if endpoint_url is not None and team.endpoint_url != endpoint_url:
                team.endpoint_url = endpoint_url
                changed_fields.append("endpoint_url")
            if github_url is not None and team.github_url != github_url:
                team.github_url = github_url
                changed_fields.append("github_url")

            if changed_fields:
                await db.commit()
                logger.info("Team updated id=%s fields=%s", team.id, ",".join(changed_fields))
            else:
                logger.info("No changes for team id=%s", team.id)

        return TeamOut(
            team_id=team.id,
            name=team.name,
            tg_username=team.tg_username,
            endpoint_url=team.endpoint_url,
            github_url=team.github_url
        )
    except HTTPException:
        await db.rollback()
        raise
    except IntegrityError:
        await db.rollback()
        logger.exception("Integrity error during team registration")
        raise HTTPException(status_code=409, detail="Конфликт уникальности (tg_chat_id или name уже заняты)")
    except SQLAlchemyError:
        await db.rollback()
        logger.exception("Database error during team registration")
        raise HTTPException(status_code=500, detail="Ошибка базы данных при регистрации команды")
    except Exception:
        await db.rollback()
        logger.exception("Unexpected error during team registration")
        raise HTTPException(status_code=500, detail="Ошибка при регистрации команды")


@app.get("/teams/{tg_chat_id}", response_model=TeamOut)
async def get_team(tg_chat_id: int, db: AsyncSession = Depends(get_session)):
    """Получение команды по ID чата в телеграме"""
    query = select(Team).where(Team.tg_chat_id == tg_chat_id)
    result = await db.execute(query)
    team = result.scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="Команда не найдена")
    return TeamOut(
        team_id=team.id,
        name=team.name,
        tg_username=team.tg_username,
        endpoint_url=team.endpoint_url,
        github_url=team.github_url
    )


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
        if os.path.exists(full_path):
            os.remove(full_path)
        raise HTTPException(status_code=500, detail=f"Не удалось сохранить файл: {e}")

    try:
        phase = Phase(name=name, dataset_filename=filename, n_csv_rows=n_csv_rows)
        db.add(phase)
        await db.commit()
        await db.refresh(phase)
    except Exception:
        await db.rollback()
        if os.path.exists(full_path):
            os.remove(full_path)
        raise HTTPException(status_code=500, detail="Ошибка при создании этапа в БД")

    try:
        if S3_OFFLINE_BUCKET:
            s3 = _s3_client()
            with open(full_path, "rb") as f:
                s3.put_object(Bucket=S3_OFFLINE_BUCKET, Key=f"{S3_DATASETS_PREFIX}{filename}", Body=f.read(), ContentType="text/csv")
    except Exception:
        pass

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

    try:
        # Блокируем команду для предотвращения race conditions
        team_query = select(Team).where(Team.tg_chat_id == tg_chat_id).with_for_update()
        team = (await db.execute(team_query)).scalar_one_or_none()
        if team is None:
            raise HTTPException(status_code=404, detail="Команда не найдена")

        # Проверяем активные запуски под блокировкой
        active_run_query = (
            select(Run)
            .where(Run.team_id == team.id)
            .where(Run.status.in_([RunStatus.QUEUED, RunStatus.RUNNING]))
            .limit(1)
        )
        if (await db.execute(active_run_query)).scalar_one_or_none() is not None:
            raise HTTPException(status_code=409, detail="Нельзя запускать оффлайн-оценку во время активной онлайн-оценки")

        # Проверяем активные CSV запуски
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

        # Получаем текущий этап
        phase = (await db.execute(select(Phase).order_by(Phase.created_at.desc()).limit(1))).scalars().first()
        if phase is None:
            raise HTTPException(status_code=404, detail="Соревнование не стартовало")

        # Читаем файл
        try:
            pred_bytes = await file.read()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Не удалось прочитать файл: {e}")

        # Создаем запись под блокировкой
        run_csv = RunCSV(team_id=team.id, phase_id=phase.id, f1=None)
        db.add(run_csv)
        await db.commit()
        await db.refresh(run_csv)

    except HTTPException:
        await db.rollback()
        raise
    except Exception:
        await db.rollback()
        raise HTTPException(status_code=500, detail="Ошибка при создании оффлайн-оценки")

    # Работа с S3 и вызов функции (вне транзакции)
    try:
        s3 = _s3_client()
        gold_key = f"{S3_DATASETS_PREFIX}{phase.dataset_filename}"

        # Загружаем предсказания
        pred_key = f"{S3_RUNS_CSV_PREFIX}{run_csv.id}/predictions.csv"
        s3.put_object(Bucket=S3_OFFLINE_BUCKET, Key=pred_key, Body=pred_bytes, ContentType="text/csv")

        # Вызываем Cloud Function
        payload = {
            "run_csv_id": run_csv.id,
            "s3_bucket": S3_OFFLINE_BUCKET,
            "s3_pred_key": pred_key,
            "s3_gold_key": gold_key,
        }

        async def _invoke_offline_cf(data: dict):
            try:
                async with httpx.AsyncClient(timeout=50.0) as client:
                    resp = await client.post(OFFLINE_CF_URL, json=data)
                    resp.raise_for_status()
            except Exception:
                logger.exception("OFFLINE_CF invocation failed", extra={"run_csv_id": data.get("run_csv_id")})

        asyncio.create_task(_invoke_offline_cf(payload))
        return RunCSVStartOut(run_csv_id=run_csv.id, status="queued")
    except Exception as e:
        # Если ошибка в S3/CF, помечаем запуск как неудачный
        try:
            async with AsyncSessionLocal() as cleanup_db:
                cleanup_run = await cleanup_db.get(RunCSV, run_csv.id)
                if cleanup_run:
                    await cleanup_db.delete(cleanup_run)
                    await cleanup_db.commit()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Ошибка при работе с S3: {e}")


@app.get("/teams/{tg_chat_id}/last_csv", response_model=RunCSVStatusOut)
async def get_last_csv_status(
    tg_chat_id: int,
    phase_id: int | None = None,
    db: AsyncSession = Depends(get_session),
):
    team = (await db.execute(select(Team).where(Team.tg_chat_id == tg_chat_id))).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="Команда не найдена")

    # Определяем этап: указанный или последний созданный
    if phase_id is None:
        phase = (await db.execute(select(Phase).order_by(Phase.created_at.desc()).limit(1))).scalars().first()
        if phase is None:
            raise HTTPException(status_code=404, detail="Нет этапов")
        pid = phase.id
    else:
        phase = (await db.execute(select(Phase).where(Phase.id == phase_id))).scalar_one_or_none()
        if phase is None:
            raise HTTPException(status_code=404, detail="Этап не найден")
        pid = phase.id

    last = (
        await db.execute(
            select(RunCSV)
            .where(RunCSV.team_id == team.id, RunCSV.phase_id == pid)
            .order_by(RunCSV.created_at.desc())
            .limit(1)
        )
    ).scalars().first()
    if last is None:
        raise HTTPException(status_code=404, detail="Нет оффлайн-оценок для команды на этом этапе")
    status = "done" if last.f1 is not None else "running"
    return RunCSVStatusOut(run_csv_id=last.id, status=status, f1=last.f1)


@app.get("/teams/{tg_chat_id}/best_csv", response_model=RunCSVStatusOut)
async def get_best_csv_status(
    tg_chat_id: int,
    phase_id: int | None = None,
    db: AsyncSession = Depends(get_session),
):
    """Лучший оффлайн-результат команды (по максимальному F1)."""
    team = (await db.execute(select(Team).where(Team.tg_chat_id == tg_chat_id))).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="Команда не найдена")
    # Определяем этап: указанный или последний
    if phase_id is None:
        phase = (await db.execute(select(Phase).order_by(Phase.created_at.desc()).limit(1))).scalars().first()
        if phase is None:
            raise HTTPException(status_code=404, detail="Нет этапов")
        pid = phase.id
    else:
        phase = (await db.execute(select(Phase).where(Phase.id == phase_id))).scalar_one_or_none()
        if phase is None:
            raise HTTPException(status_code=404, detail="Этап не найден")
        pid = phase.id

    best = (
        await db.execute(
            select(RunCSV)
            .where(RunCSV.team_id == team.id, RunCSV.phase_id == pid, RunCSV.f1.isnot(None))
            .order_by(RunCSV.f1.desc(), RunCSV.created_at.asc())
            .limit(1)
        )
    ).scalars().first()
    if best is None:
        raise HTTPException(status_code=404, detail="Нет завершённых оффлайн-оценок для команды на этом этапе")
    return RunCSVStatusOut(run_csv_id=best.id, status="done", f1=best.f1)


@app.post("/runs/start", response_model=StartRunOut)
async def start_run(payload: StartRunIn, db: AsyncSession = Depends(get_session)):
    """Запустить оценку через Cloud Functions."""
    try:
        # Блокируем команду
        team_query = select(Team).where(Team.tg_chat_id == payload.tg_chat_id).with_for_update()
        team = (await db.execute(team_query)).scalar_one_or_none()
        if team is None:
            raise HTTPException(status_code=404, detail="Команда не найдена")
        if team.endpoint_url is None:
            raise HTTPException(status_code=400, detail="Не указан URL сервиса")
        if not PREDICT_CF_URL:
            raise HTTPException(status_code=500, detail="PREDICT_CF_URL is not configured")

        # Проверяем активные запуски
        active_run_query = (
            select(Run)
            .where(Run.team_id == team.id)
            .where(Run.status.in_([RunStatus.QUEUED, RunStatus.RUNNING]))
            .limit(1)
        )
        if (await db.execute(active_run_query)).scalar_one_or_none() is not None:
            raise HTTPException(status_code=409, detail="У команды уже есть активный запуск")

        # Проверяем активные CSV запуски
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

        # Получаем этап
        result = await db.execute(select(Phase).order_by(Phase.created_at.desc()).limit(1))
        phase = result.scalars().first()
        if phase is None:
            raise HTTPException(status_code=404, detail="Соревнование не стартовало")

        # Создаем запуск
        run = Run(
            team_id=team.id,
            phase_id=phase.id,
            status=RunStatus.QUEUED,  # Начинаем с QUEUED
            started_at=datetime.now(timezone.utc),
            samples_total=0,
            samples_processed=0,
            samples_success=0,
        )
        db.add(run)
        await db.commit()
        await db.refresh(run)

    except HTTPException:
        await db.rollback()
        raise
    except Exception:
        await db.rollback()
        raise HTTPException(status_code=500, detail="Ошибка при создании запуска")

    # Подготовка данных и запуск CF (вне транзакции)
    try:
        # Подготавливаем items
        items = await _build_run_items(phase)

        # Обновляем количество samples и статус на RUNNING
        async with AsyncSessionLocal() as update_db:
            update_run = await update_db.get(Run, run.id)
            if update_run:
                update_run.samples_total = len(items)
                update_run.status = RunStatus.RUNNING
                await update_db.commit()

        # Запускаем Cloud Function
        async def _call_predict_cf_http(team: Team, run: Run, items: list[dict]) -> None:
            try:
                payload = {
                    "run_id": run.id,
                    "team_id": team.id,
                    "endpoint_url": team.endpoint_url,
                    "items": items,
                }
                async with httpx.AsyncClient(timeout=50.0) as client:
                    resp = await client.post(PREDICT_CF_URL.rstrip("/"), json=payload)
                    resp.raise_for_status()
            except Exception:
                # При ошибке CF помечаем запуск как failed
                async with AsyncSessionLocal() as error_db:
                    error_run = await error_db.get(Run, run.id)
                    if error_run:
                        error_run.status = RunStatus.FAILED
                        await error_db.commit()
                logger.exception("PREDICT_CF invocation failed", extra={"run_id": run.id})

        asyncio.create_task(_call_predict_cf_http(team, run, items))
        return StartRunOut(run_id=run.id, status=RunStatus.RUNNING.value)

    except Exception as e:
        # При ошибке подготовки помечаем запуск как failed
        try:
            async with AsyncSessionLocal() as error_db:
                error_run = await error_db.get(Run, run.id)
                if error_run:
                    error_run.status = RunStatus.FAILED
                    await error_db.commit()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Не удалось подготовить запуск: {e}")


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
async def get_last_run_status(
    tg_chat_id: int,
    phase_id: int | None = None,
    db: AsyncSession = Depends(get_session),
):
    """Получение статуса последнего запуска командой"""
    team = (await db.execute(select(Team).where(Team.tg_chat_id == tg_chat_id))).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="Команда не найдена")

    # Определяем этап: указанный или последний
    if phase_id is None:
        phase = (await db.execute(select(Phase).order_by(Phase.created_at.desc()).limit(1))).scalars().first()
        if phase is None:
            raise HTTPException(status_code=404, detail="Нет этапов")
        pid = phase.id
    else:
        phase = (await db.execute(select(Phase).where(Phase.id == phase_id))).scalar_one_or_none()
        if phase is None:
            raise HTTPException(status_code=404, detail="Этап не найден")
        pid = phase.id

    last_run_query = (
        select(Run)
        .where(Run.team_id == team.id, Run.phase_id == pid)
        .order_by(Run.created_at.desc())
        .limit(1)
    )
    last_run = (await db.execute(last_run_query)).scalars().first()
    if last_run is None:
        raise HTTPException(status_code=404, detail="У данной команды ещё не было запусков на этом этапе")

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
        order_by=(
            func.coalesce(Run.f1, 0.0).desc(),
            func.coalesce(Run.avg_latency_ms, NULL_LAT_MS_RANK).asc(),
            Run.id.asc(),
        ),
    )

    subq = (
        select(
            Run.team_id.label("team_id"),
            Run.f1.label("f1"),
            Run.avg_latency_ms.label("lat"),
            rn.label("rn"),
        )
        .where(
            Run.phase_id == pid,
            Run.status == RunStatus.DONE,
            Run.f1.isnot(None),
        )
        .subquery()
    )

    res = await db.execute(
        select(Team.name, subq.c.f1, subq.c.lat)
        .join(Team, Team.id == subq.c.team_id)
        .where(subq.c.rn == 1)
        .order_by(
            func.coalesce(subq.c.f1, 0.0).desc(),
            func.coalesce(subq.c.lat, NULL_LAT_MS_RANK).asc(),
            Team.name.asc(),
        )
    )
    rows = res.all()

    return LeaderboardOut(
        phase_id=pid,
        items=[
            LeaderboardItem(
                team_name=name,
                avg_latency_ms=float(lat) if lat is not None else None,
                f1=float(f1) if f1 is not None else None
            )
            for (name, f1, lat) in rows
        ]
    )
