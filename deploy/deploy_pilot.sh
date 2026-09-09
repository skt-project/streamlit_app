#!/usr/bin/env bash
# Shared build+deploy script for every migration pilot. Run from the repo root.
#
# Usage:
#   deploy/deploy_pilot.sh <app_slug> <entrypoint_file> <requirements_path> [memory] [cpu]
#
# Example:
#   deploy/deploy_pilot.sh skt-top-20-store-list-stock skt_top_20_store_list_stock.py \
#     deploy/skt_top_20_store_list_stock/requirements.txt 512Mi 1
#
# Fixed, non-negotiable settings across every pilot (see docs/migration/
# DEPLOYMENT_TEMPLATE.md for why): region asia-southeast1, the
# streamlit-migration-runtime service account, --no-allow-unauthenticated
# (internal-testing phase only, per MIGRATION_PLAN.md's WebSocket/IAM-auth
# finding), and concurrency=1 (Streamlit's session model does not support
# more than one session per container instance).
set -euo pipefail

APP_SLUG="$1"
ENTRYPOINT_FILE="$2"
REQUIREMENTS_PATH="$3"
MEMORY="${4:-512Mi}"
CPU="${5:-1}"

PROJECT="skintific-data-warehouse"
REGION="asia-southeast1"
REPO="asia-southeast1-docker.pkg.dev/${PROJECT}/streamlit-migration"
IMAGE="${REPO}/${APP_SLUG}:pilot-1"
SERVICE="${APP_SLUG}-migration"
SA="streamlit-migration-runtime@${PROJECT}.iam.gserviceaccount.com"

echo "=== Building ${APP_SLUG} (entrypoint: ${ENTRYPOINT_FILE}) ==="
gcloud builds submit \
  --config=deploy/cloudbuild-template.yaml \
  --substitutions="_ENTRYPOINT_FILE=${ENTRYPOINT_FILE},_REQUIREMENTS_PATH=${REQUIREMENTS_PATH},_IMAGE=${IMAGE}" \
  --project="${PROJECT}" \
  .

echo "=== Deploying ${SERVICE} ==="
gcloud run deploy "${SERVICE}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --platform=managed \
  --service-account="${SA}" \
  --no-allow-unauthenticated \
  --memory="${MEMORY}" --cpu="${CPU}" \
  --concurrency=1 --min-instances=0 --max-instances=3 \
  --session-affinity \
  --timeout=300 \
  --project="${PROJECT}"

echo "=== Done. Service URL above. Validate via: ==="
echo "gcloud run services proxy ${SERVICE} --region=${REGION} --project=${PROJECT} --port=<local-port>"
