from enum import Enum


class RunStatus(Enum):
    """Статусы запуска"""

    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
