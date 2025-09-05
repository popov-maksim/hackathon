FROM python:3.11-slim
WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app

CMD ["celery", "-A", "tasks.celery_app", "worker", "-Q", "runs", "-l", "INFO", "-c", "1"]
