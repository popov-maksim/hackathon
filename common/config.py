import os


DATASETS_DIR = os.getenv("DATASETS_DIR", "/data/datasets")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

REQUEST_CONNECT_TIMEOUT = float(os.getenv("REQUEST_CONNECT_TIMEOUT", "2.0"))
REQUEST_READ_TIMEOUT = float(os.getenv("REQUEST_READ_TIMEOUT", "3.0"))
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "1.0"))
RUN_TIME_LIMIT_SECONDS = float(os.getenv("RUN_TIME_LIMIT_SECONDS", "1200"))  # 20 minutes

API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000")

N_CSV_ROWS = int(os.getenv("N_CSV_ROWS", "20"))
