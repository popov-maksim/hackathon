import os


DATASETS_DIR = os.getenv("DATASETS_DIR", "/data/datasets")

REQUEST_CONNECT_TIMEOUT = float(os.getenv("REQUEST_CONNECT_TIMEOUT", "2.0"))
REQUEST_READ_TIMEOUT = float(os.getenv("REQUEST_READ_TIMEOUT", "3.0"))
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "1.0"))
RUN_TIME_LIMIT_SECONDS = float(os.getenv("RUN_TIME_LIMIT_SECONDS", "1200"))  # 20 minutes

API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000")

# Yandex Message Queue (SQS-compatible)
YMQ_ENDPOINT_URL = os.getenv("YMQ_ENDPOINT_URL", "https://message-queue.api.cloud.yandex.net")
YMQ_REGION = os.getenv("YMQ_REGION", "ru-central1")
YMQ_QUEUE_URL = os.getenv("YMQ_QUEUE_URL", "")  # Full QueueUrl

# Optional static credentials (prefer SA/IAM in YC)
YMQ_ACCESS_KEY = os.getenv("YMQ_ACCESS_KEY", "")
YMQ_SECRET_KEY = os.getenv("YMQ_SECRET_KEY", "")
YMQ_SESSION_TOKEN = os.getenv("YMQ_SESSION_TOKEN", "")

# Publish options
RUN_CHUNK_SIZE = int(os.getenv("RUN_CHUNK_SIZE", "1"))  # 1 = per-sample messages
SQS_SEND_BATCH_MAX = 10  # SQS/YMQ limit
