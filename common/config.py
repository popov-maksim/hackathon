import os


DATASETS_DIR = os.getenv("DATASETS_DIR", "/data/datasets")

REQUEST_CONNECT_TIMEOUT = float(os.getenv("REQUEST_CONNECT_TIMEOUT", "2.0"))
REQUEST_READ_TIMEOUT = float(os.getenv("REQUEST_READ_TIMEOUT", "3.0"))
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "1.0"))
RUN_TIME_LIMIT_SECONDS = float(os.getenv("RUN_TIME_LIMIT_SECONDS", "1200"))  # 20 minutes

API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000")


# S3 / Object Storage (e.g., Yandex Object Storage)
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "https://storage.yandexcloud.net")
S3_REGION = os.getenv("S3_REGION", "ru-central1")
S3_OFFLINE_BUCKET = os.getenv("S3_OFFLINE_BUCKET", "")
S3_DATASETS_PREFIX = os.getenv("S3_DATASETS_PREFIX", "datasets/")
S3_RUNS_CSV_PREFIX = os.getenv("S3_RUNS_CSV_PREFIX", "runs_csv/")

# Public Cloud Function endpoint for offline CSV scoring (HTTP trigger)
OFFLINE_CF_URL = os.getenv("OFFLINE_CF_URL", "")

# Public Cloud Function endpoint for online scoring (HTTP trigger)
PREDICT_CF_URL = os.getenv("PREDICT_CF_URL", "")

ACCESS_KEY = os.getenv("ACCESS_KEY", "")
SECRET_KEY = os.getenv("SECRET_KEY", "")
