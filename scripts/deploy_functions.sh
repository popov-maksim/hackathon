#!/usr/bin/env bash
set -euo pipefail

# This script deploys Cloud Functions using yc CLI.
# Requirements:
#  - yc CLI configured (cloud/folder set)
#  - Service Account with roles to invoke functions and access DB
#  - Environment variables for DB/timeouts exported or present in .env

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
BUILD_DIR="$ROOT_DIR/build"

: "${YC_SA_ID:?Set YC_SA_ID to your Service Account ID}"

FN_PREDICT_NAME="${FN_PREDICT_NAME:-predict-worker}"
FN_FINALIZER_NAME="${FN_FINALIZER_NAME:-run-finalizer}"

# Load .env if present to pick DB/TIMEOUT vars
if [[ -f "$ROOT_DIR/.env" ]]; then
  while IFS= read -r line; do
    case "$line" in
      POSTGRES_USER=*|POSTGRES_PASSWORD=*|POSTGRES_DB=*|POSTGRES_HOST=*|POSTGRES_PORT=*|REQUEST_CONNECT_TIMEOUT=*|REQUEST_READ_TIMEOUT=*|RUN_TIME_LIMIT_SECONDS=*|YMQ_QUEUE_URL=*|YMQ_QUEUE_ARN=*)
        key="${line%%=*}"
        val="${line#*=}"
        # strip inline comments only if preceded by whitespace (preserves '#' inside values)
        val="$(printf '%s' "$val" | sed -E 's/[[:space:]]+#.*$//')"
        # trim surrounding whitespace and enclosing quotes
        val="$(printf '%s' "$val" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//; s/^"(.*)"$/\1/')"
        export "$key"="$val"
        ;;
    esac
  done < "$ROOT_DIR/.env"
fi

# Validate required envs
req_vars=(POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB POSTGRES_HOST POSTGRES_PORT REQUEST_CONNECT_TIMEOUT REQUEST_READ_TIMEOUT RUN_TIME_LIMIT_SECONDS)
for v in "${req_vars[@]}"; do
  if [[ -z "${!v:-}" ]]; then
    echo "[!] Missing env var: $v" >&2
    exit 1
  fi
done

echo "[i] Building function sources..."
"$ROOT_DIR/scripts/package_functions.sh"

echo "[i] Ensuring functions exist..."
yc serverless function create --name "$FN_PREDICT_NAME" >/dev/null 2>&1 || true
yc serverless function create --name "$FN_FINALIZER_NAME" >/dev/null 2>&1 || true

echo "[i] Deploying version: $FN_PREDICT_NAME"
yc serverless function version create \
  --function-name "$FN_PREDICT_NAME" \
  --runtime python311 \
  --entrypoint main.handler \
  --memory 512MB \
  --execution-timeout 120s \
  --network-name default \
  --service-account-id "$YC_SA_ID" \
  --source-path "$BUILD_DIR/predict_worker" \
  --environment POSTGRES_USER="$POSTGRES_USER" \
  --environment POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --environment POSTGRES_DB="$POSTGRES_DB" \
  --environment POSTGRES_HOST="$POSTGRES_HOST" \
  --environment POSTGRES_PORT="$POSTGRES_PORT" \
  --environment REQUEST_CONNECT_TIMEOUT="$REQUEST_CONNECT_TIMEOUT" \
  --environment REQUEST_READ_TIMEOUT="$REQUEST_READ_TIMEOUT" \
  --environment RUN_TIME_LIMIT_SECONDS="$RUN_TIME_LIMIT_SECONDS"

echo "[i] Deploying version: $FN_FINALIZER_NAME"
yc serverless function version create \
  --function-name "$FN_FINALIZER_NAME" \
  --runtime python311 \
  --entrypoint main.handler \
  --memory 512MB \
  --execution-timeout 120s \
  --service-account-id "$YC_SA_ID" \
  --source-path "$BUILD_DIR/run_finalizer" \
  --network-name default \
  --environment POSTGRES_USER="$POSTGRES_USER" \
  --environment POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --environment POSTGRES_DB="$POSTGRES_DB" \
  --environment POSTGRES_HOST="$POSTGRES_HOST" \
  --environment POSTGRES_PORT="$POSTGRES_PORT" \
  --environment RUN_TIME_LIMIT_SECONDS="$RUN_TIME_LIMIT_SECONDS"

echo "[✓] Deployed Cloud Functions. Create triggers in YC console or with yc CLI."

# --- Triggers registration ---
echo "[i] Ensuring triggers exist..."

# Predict-worker MQ trigger
TRIGGER_MQ_NAME="${TRIGGER_MQ_NAME:-predict-worker-trigger}"
TRIGGER_BATCH_SIZE="${TRIGGER_BATCH_SIZE:-10}"
TRIGGER_BATCH_CUTOFF="${TRIGGER_BATCH_CUTOFF:-2s}"
TRIGGER_VISIBILITY_TIMEOUT="${TRIGGER_VISIBILITY_TIMEOUT:-90s}"

# Accept either ARN or Queue URL
QUEUE_IDENT="${YMQ_QUEUE_ARN:-}"
if [[ -z "$QUEUE_IDENT" || "$QUEUE_IDENT" == "" ]]; then
  QUEUE_IDENT="${YMQ_QUEUE_URL:-}"
fi

if [[ -n "$QUEUE_IDENT" ]]; then
  # Delete existing MQ trigger to avoid CLI update quirks
  if yc serverless trigger get --name "$TRIGGER_MQ_NAME" >/dev/null 2>&1; then
    yc serverless trigger delete --name "$TRIGGER_MQ_NAME"
  fi
  echo "[i] Creating MQ trigger '$TRIGGER_MQ_NAME'"
  MQ_FLAGS=(--name "$TRIGGER_MQ_NAME" --queue "$QUEUE_IDENT" --queue-service-account-id "$YC_SA_ID" --invoke-function-name "$FN_PREDICT_NAME" --invoke-function-service-account-id "$YC_SA_ID")
  if yc serverless trigger create message-queue --help 2>/dev/null | grep -q -- '--batch-size'; then
    MQ_FLAGS+=(--batch-size "$TRIGGER_BATCH_SIZE")
  fi
  if yc serverless trigger create message-queue --help 2>/dev/null | grep -q -- '--batch-cutoff'; then
    MQ_FLAGS+=(--batch-cutoff "$TRIGGER_BATCH_CUTOFF")
  fi
  yc serverless trigger create message-queue "${MQ_FLAGS[@]}"
else
  echo "[!] YMQ_QUEUE_URL or YMQ_QUEUE_ARN not set; skipping MQ trigger creation" >&2
fi

# Run-finalizer timer trigger
TRIGGER_TIMER_NAME="${TRIGGER_TIMER_NAME:-run-finalizer-trigger}"
# Yandex Cloud timer expects Quartz-style 6-field cron
CRON_EXPR="${TRIGGER_TIMER_CRON:-* * * * ? *}"

if yc serverless trigger get --name "$TRIGGER_TIMER_NAME" >/dev/null 2>&1; then
  yc serverless trigger delete --name "$TRIGGER_TIMER_NAME"
fi
echo "[i] Creating timer trigger '$TRIGGER_TIMER_NAME'"
yc serverless trigger create timer \
  --name "$TRIGGER_TIMER_NAME" \
  --cron-expression "$CRON_EXPR" \
  --invoke-function-name "$FN_FINALIZER_NAME" \
  --invoke-function-service-account-id "$YC_SA_ID"

echo "[✓] Triggers ensured: $TRIGGER_MQ_NAME, $TRIGGER_TIMER_NAME"
