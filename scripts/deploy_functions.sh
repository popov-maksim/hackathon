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
FN_OFFLINE_CSV_NAME="${FN_OFFLINE_CSV_NAME:-offline-csv-worker}"

# Load .env if present to pick DB/TIMEOUT vars
if [[ -f "$ROOT_DIR/.env" ]]; then
  while IFS= read -r line; do
    case "$line" in
      POSTGRES_USER=*|POSTGRES_PASSWORD=*|POSTGRES_DB=*|POSTGRES_HOST=*|POSTGRES_PORT=*|REQUEST_CONNECT_TIMEOUT=*|REQUEST_READ_TIMEOUT=*|RUN_TIME_LIMIT_SECONDS=*|YMQ_QUEUE_URL=*|YMQ_QUEUE_ARN=*|S3_ENDPOINT_URL=*|S3_REGION=*|S3_OFFLINE_BUCKET=*|ACCESS_KEY=*|SECRET_KEY=*)
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
req_vars=(POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB POSTGRES_HOST POSTGRES_PORT REQUEST_CONNECT_TIMEOUT REQUEST_READ_TIMEOUT RUN_TIME_LIMIT_SECONDS S3_ENDPOINT_URL S3_REGION S3_OFFLINE_BUCKET ACCESS_KEY SECRET_KEY)
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
yc serverless function create --name "$FN_OFFLINE_CSV_NAME" >/dev/null 2>&1 || true

echo "[i] Deploying version: $FN_PREDICT_NAME"
yc serverless function version create \
  --function-name "$FN_PREDICT_NAME" \
  --runtime python311 \
  --entrypoint main.handler \
  --memory 512MB \
  --execution-timeout 600s \
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

echo "[i] Deploying version: $FN_OFFLINE_CSV_NAME"
yc serverless function version create \
  --function-name "$FN_OFFLINE_CSV_NAME" \
  --runtime python311 \
  --entrypoint main.handler \
  --memory 512MB \
  --execution-timeout 600s \
  --service-account-id "$YC_SA_ID" \
  --source-path "$BUILD_DIR/offline_csv_worker" \
  --network-name default \
  --environment POSTGRES_USER="$POSTGRES_USER" \
  --environment POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --environment POSTGRES_DB="$POSTGRES_DB" \
  --environment POSTGRES_HOST="$POSTGRES_HOST" \
  --environment POSTGRES_PORT="$POSTGRES_PORT" \
  --environment S3_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  --environment S3_REGION="$S3_REGION" \
  --environment ACCESS_KEY="$ACCESS_KEY" \
  --environment SECRET_KEY="$SECRET_KEY"

echo "[✓] Deployed Cloud Functions."
