from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    ForeignKey,
    DateTime,
    Float,
    JSON,
    BigInteger,
    Enum,
    func,
    UniqueConstraint,
    Index,
)

from common.constants import RunStatus


Base = declarative_base()


class Team(Base):
    """Таблица с командами"""

    __tablename__ = "teams"

    id = Column(Integer, primary_key=True)
    tg_chat_id = Column(BigInteger, unique=True, nullable=False)
    tg_username = Column(String(128), nullable=False)
    name = Column(String(128), unique=True, nullable=False)
    endpoint_url = Column(String(512), nullable=True)
    github_url = Column(String(512), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class Phase(Base):
    """Таблица с этапами, от этапа зависит набор данных для оценки: паблик/прайват"""

    __tablename__ = "phases"

    id = Column(Integer, primary_key=True)
    name = Column(String(128), unique=True, nullable=False)
    dataset_filename = Column(String(256), nullable=False)
    # Максимальное количество строк из CSV для оценки на этапе
    n_csv_rows = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_phases_created_at", "created_at"),
    )


class Run(Base):
    """Таблица со статистикой по запускам с пингом участников"""

    __tablename__ = "runs"

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("teams.id", ondelete="CASCADE"), nullable=False)
    phase_id = Column(Integer, ForeignKey("phases.id", ondelete="CASCADE"), nullable=False)
    status = Column(Enum(RunStatus), default=RunStatus.QUEUED, nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    samples_total = Column(Integer, default=0)
    samples_processed = Column(Integer, default=0)
    samples_success = Column(Integer, default=0)
    avg_latency_ms = Column(Float, nullable=True)
    f1 = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        # Для лидерборда: фильтр по (phase_id, status), разбиение по team_id и сортировка по f1/avg_latency
        Index(
            "ix_runs_phase_status_team_f1_lat_id",
            "phase_id",
            "status",
            "team_id",
            "f1",
            "avg_latency_ms",
            "id",
        ),
        # Для последнего запуска команды на этапе
        Index(
            "ix_runs_team_phase_created",
            "team_id",
            "phase_id",
            "created_at",
        ),
        # Для проверки активных запусков
        Index("ix_runs_team_status", "team_id", "status"),
    )


class Prediction(Base):
    """Таблица с предикшенами для пингов"""

    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("runs.id", ondelete="CASCADE"), nullable=False)
    sample_idx = Column(Integer, nullable=False, default=0)
    latency_ms = Column(Float, nullable=True)
    ok = Column(Boolean, default=False)
    gold_json = Column(JSON, nullable=False)
    pred_json = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("run_id", "sample_idx", name="ux_predictions_run_sample"),
    )


class RunCSV(Base):
    """Таблица со статистикой по запускам на csv файлах"""

    __tablename__ = "runs_csv"

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("teams.id", ondelete="CASCADE"), nullable=False)
    phase_id = Column(Integer, ForeignKey("phases.id", ondelete="CASCADE"), nullable=False)
    f1 = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        # Для получения последнего CSV-запуска на этапе
        Index("ix_runs_csv_team_phase_created", "team_id", "phase_id", "created_at"),
        # Для выбора лучшего CSV-запуска по f1 (и тай-брейк по created_at)
        Index("ix_runs_csv_team_phase_f1_created", "team_id", "phase_id", "f1", "created_at"),
    )
