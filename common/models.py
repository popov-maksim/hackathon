from datetime import datetime, timezone

from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, DateTime, Float, Text, JSON, BigInteger, Enum

from common.constants import RunStatus


Base = declarative_base()


class Team(Base):
    """Таблица с командами"""

    __tablename__ = "teams"

    id = Column(Integer, primary_key=True)
    tg_chat_id = Column(BigInteger, unique=True, nullable=False)
    name = Column(String(128), unique=True, nullable=False)
    endpoint_url = Column(String(512), nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)

    runs = relationship("Run", back_populates="team")
    runs_csv = relationship("RunCSV", back_populates="team")


class Phase(Base):
    """Таблица с этапами, от этапа зависит набор данных для оценки: паблик/прайват"""

    __tablename__ = "phases"

    id = Column(Integer, primary_key=True)
    name = Column(String(128), unique=True, nullable=False)
    dataset_filename = Column(String(256), nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)

    runs = relationship("Run", back_populates="phase")
    runs_csv = relationship("RunCSV", back_populates="phase")


class Run(Base):
    """Таблица со статистикой по запускам с пингом участников"""

    __tablename__ = "runs"

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    phase_id = Column(Integer, ForeignKey("phases.id"), nullable=False)
    status = Column(Enum(RunStatus), default=RunStatus.QUEUED, nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    samples_total = Column(Integer, default=0)
    samples_success = Column(Integer, default=0)
    avg_latency_ms = Column(Float, nullable=True)
    f1 = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)

    team = relationship("Team", back_populates="runs")
    phase = relationship("Phase", back_populates="runs")
    predictions = relationship("Prediction", back_populates="run")


class Prediction(Base):
    """Таблица с предикшенами для пингов"""

    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("runs.id"), nullable=False)
    latency_ms = Column(Float, nullable=True)
    ok = Column(Boolean, default=False)
    gold_json = Column(JSON, nullable=False)
    pred_json = Column(JSON, nullable=True)

    run = relationship("Run", back_populates="predictions")


class RunCSV(Base):
    """Таблица со статистикой по запускам на csv файлах"""

    __tablename__ = "runs_csv"

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    phase_id = Column(Integer, ForeignKey("phases.id"), nullable=False)
    precision = Column(Float, nullable=True)
    recall = Column(Float, nullable=True)
    f1 = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)

    team = relationship("Team", back_populates="runs_csv")
    phase = relationship("Phase", back_populates="runs_csv")
