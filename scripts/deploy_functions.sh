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
      POSTGRES_USER=*|POSTGRES_PASSWORD=*|POSTGRES_DB=*|POSTGRES_HOST=*|POSTGRES_PORT=*|REQUEST_CONNECT_TIMEOUT=*|REQUEST_READ_TIMEOUT=*|RUN_TIME_LIMIT_SECONDS=*)
        key="${line%%=*}"
        val="${line#*=}"
        # strip inline comments
        val="${val%%#*}"
        # trim whitespace
        val="${val%% }"; val="${val## }"
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
  --memory 512M \
  --execution-timeout 120s \
  --service-account-id "$YC_SA_ID" \
  --source-path "$BUILD_DIR/predict_worker" \
  --env POSTGRES_USER="$POSTGRES_USER" \
  --env POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --env POSTGRES_DB="$POSTGRES_DB" \
  --env POSTGRES_HOST="$POSTGRES_HOST" \
  --env POSTGRES_PORT="$POSTGRES_PORT" \
  --env REQUEST_CONNECT_TIMEOUT="$REQUEST_CONNECT_TIMEOUT" \
  --env REQUEST_READ_TIMEOUT="$REQUEST_READ_TIMEOUT" \
  --env RUN_TIME_LIMIT_SECONDS="$RUN_TIME_LIMIT_SECONDS"

echo "[i] Deploying version: $FN_FINALIZER_NAME"
yc serverless function version create \
  --function-name "$FN_FINALIZER_NAME" \
  --runtime python311 \
  --entrypoint main.handler \
  --memory 512M \
  --execution-timeout 120s \
  --service-account-id "$YC_SA_ID" \
  --source-path "$BUILD_DIR/run_finalizer" \
  --env POSTGRES_USER="$POSTGRES_USER" \
  --env POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --env POSTGRES_DB="$POSTGRES_DB" \
  --env POSTGRES_HOST="$POSTGRES_HOST" \
  --env POSTGRES_PORT="$POSTGRES_PORT" \
  --env RUN_TIME_LIMIT_SECONDS="$RUN_TIME_LIMIT_SECONDS"

echo "[✓] Deployed Cloud Functions. Create triggers in YC console or with yc CLI."
