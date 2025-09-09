#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
BUILD_DIR="$ROOT_DIR/build"

echo "[i] Packaging Cloud Functions into $BUILD_DIR"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/predict_worker" "$BUILD_DIR/run_finalizer" "$BUILD_DIR/offline_csv_worker"

# Copy predict_worker
cp -R "$ROOT_DIR/functions/predict_worker/." "$BUILD_DIR/predict_worker/"
cp -R "$ROOT_DIR/common" "$BUILD_DIR/predict_worker/common"
find "$BUILD_DIR/predict_worker" -name "__pycache__" -type d -exec rm -rf {} + || true
find "$BUILD_DIR/predict_worker" -name "*.pyc" -delete || true

# Copy run_finalizer
cp -R "$ROOT_DIR/functions/run_finalizer/." "$BUILD_DIR/run_finalizer/"
cp -R "$ROOT_DIR/common" "$BUILD_DIR/run_finalizer/common"
find "$BUILD_DIR/run_finalizer" -name "__pycache__" -type d -exec rm -rf {} + || true
find "$BUILD_DIR/run_finalizer" -name "*.pyc" -delete || true

# Copy offline_csv_worker
cp -R "$ROOT_DIR/functions/offline_csv_worker/." "$BUILD_DIR/offline_csv_worker/"
cp -R "$ROOT_DIR/common" "$BUILD_DIR/offline_csv_worker/common"
find "$BUILD_DIR/offline_csv_worker" -name "__pycache__" -type d -exec rm -rf {} + || true
find "$BUILD_DIR/offline_csv_worker" -name "*.pyc" -delete || true

echo "[✓] Packaged into:"
echo " - $BUILD_DIR/predict_worker"
echo " - $BUILD_DIR/run_finalizer"
echo " - $BUILD_DIR/offline_csv_worker"
