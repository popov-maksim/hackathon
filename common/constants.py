from enum import Enum


class RunStatus(Enum):
    """Статусы запуска"""

    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"

# Значение для ранжирования NULL задержек как максимально больших
NULL_LAT_MS_RANK = 1_000_000_000
