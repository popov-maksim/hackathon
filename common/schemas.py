from pydantic import BaseModel, AnyHttpUrl
from typing import List, Optional


class RegisterTeamIn(BaseModel):
    tg_chat_id: int
    team_name: str
    endpoint_url: AnyHttpUrl


class TeamOut(BaseModel):
    team_id: int
    name: str
    endpoint_url: str


class CreatePhaseIn(BaseModel):
    name: str
    dataset_filename: str
    n_csv_rows: int | None = None


class CreatePhaseOut(BaseModel):
    phase_id: int
    name: str
    dataset_filename: str
    n_csv_rows: int | None = None


class StartRunIn(BaseModel):
    tg_chat_id: int


class StartRunOut(BaseModel):
    run_id: int
    status: str


class RunStatusOut(BaseModel):
    run_id: int
    status: str
    samples_processed: int
    samples_success: int
    samples_total: int
    avg_latency_ms: Optional[float] = None
    f1: Optional[float] = None


class LeaderboardItem(BaseModel):
    team_name: str
    avg_latency_ms: float
    f1: float


class LeaderboardOut(BaseModel):
    phase_id: int
    items: List[LeaderboardItem]


class RunCSVStartOut(BaseModel):
    run_csv_id: int
    status: str


class RunCSVStatusOut(BaseModel):
    run_csv_id: int
    status: str
    f1: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
