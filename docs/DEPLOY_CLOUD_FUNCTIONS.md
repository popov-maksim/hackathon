Развёртывание в Yandex Cloud

1) Предварительные условия

- Установите и настройте yc CLI (облако и каталог).
- Подготовьте Managed PostgreSQL (порт по умолчанию 6432) и доступ к нему из функций: публичный FQDN или привязка функций к сети VPC.
- Создайте очереди Yandex Message Queue: основную (например, scoring_q) и DLQ (например, scoring_q_dlq). Сохраните полный Queue URL (нужен API) и/или ARN (может использоваться в триггере).

2) Сервисные аккаунты и роли

- Минимум один SA, рекомендовано два:
  - sa-api: роль на каталог ymq.writer (публиковать сообщения в очередь из API).
  - sa-worker: роли на каталог ymq.reader (читать очередь триггером) и возможность вызывать функции serverless.functions.invoker.
- Если БД доступна только в приватной сети, добавьте sa-worker роль vpc.user и привяжите сеть к функциям.

3) Настройка .env

Для API и функций укажите:

- POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT (обычно 6432)
- REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT, RUN_TIME_LIMIT_SECONDS (например, 2.0 / 3.0 / 1200)

Только для API (публикация в очередь):

- YMQ_QUEUE_URL — полный URL очереди (обязательно для API)
- (опционально) YMQ_ENDPOINT_URL, YMQ_REGION — уже имеют дефолты
- (если без IAM‑метадаты) YMQ_ACCESS_KEY, YMQ_SECRET_KEY — статические ключи sa-api

Для predict‑worker (опционально): WORKER_MAX_CONCURRENCY — параллелизм HTTP внутри одной инвокации (по умолчанию 10).

4) Деплой функций

- Скрипт упакует общий модуль common/ внутрь функций.
- Выполните:
  - export YC_SA_ID=<id сервисного аккаунта sa‑worker>
  - ./scripts/deploy_functions.sh
- Скрипт создаст/обновит функции predict-worker и run-finalizer, привяжет их к сети default и задаст окружение.

5) Триггеры (создаются скриптом автоматически)

- MQ‑триггер для predict-worker:
  - Источник: YMQ_QUEUE_URL или YMQ_QUEUE_ARN из .env.
  - SA для чтения очереди и вызова функции: $YC_SA_ID (sa-worker).
  - Параметры по умолчанию: batch size 10, batch cutoff 2s, visibility timeout 90s.
- Таймер для run-finalizer:
  - Quartz‑cron (6 полей) по умолчанию: "0 0/1 * * * ?" — каждую минуту.
  - Можно переопределить: export TRIGGER_TIMER_CRON="0 */5 * * * ?".

6) Рекомендации по настройке очереди YMQ

- Тип: Standard.
- Visibility timeout: ~90 сек (должен быть больше максимального времени обработки одного батча).
- Wait time при получении: 20 сек.
- DLQ: включить, очередь — scoring_q_dlq, Max receive count: 5.

7) Доступ к БД

- Публичная БД: дополнительные сетевые настройки не требуются.
- Приватная БД: привяжите функции к сети VPC "default" (скрипт это делает), откройте в Security Group PG порт 6432 для подсетей этой сети.

8) Проверка

- yc serverless function list — список функций.
- yc serverless trigger list — список триггеров.
- Логи функций — в карточке функций или через Cloud Logging.
- Роли: sa-api — ymq.writer; sa-worker — ymq.reader и invoker на функциях (скрипт добавляет автоматически).
