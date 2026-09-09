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
# DEPLOYMENT_TEMPLATE.md section 10 for why): region asia-southeast1, the
# streamlit-migration-runtime service account, --no-allow-unauthenticated
# (internal-testing phase only, per MIGRATION_PLAN.md's WebSocket/IAM-auth
# finding), --session-affinity (keeps one session's WebSocket + file-upload
# + static-asset requests on the same container instance), and a real
# concurrency value (80, Cloud Run's own default) - NOT 1. concurrency=1
# was an earlier mistake in this template: it forced Cloud Run to serve
# only one HTTP request at a time per instance, which starved the dozens
# of small static JS chunks a Streamlit page fetches in parallel on first
# load, surfacing as real HTTP 500s on those requests (confirmed live on
# noo-detector-migration, 2026-09-09) - not a proxy artifact as first
# assumed. Session affinity (not a low concurrency number) is the correct
# mechanism for "one session's requests stay on one instance."
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
  --concurrency=80 --min-instances=0 --max-instances=3 \
  --session-affinity \
  --update-env-vars="STREAMLIT_THEME_BASE=light,STREAMLIT_THEME_BACKGROUND_COLOR=#FFFFFF,STREAMLIT_THEME_SECONDARY_BACKGROUND_COLOR=#F5F7FA,STREAMLIT_THEME_TEXT_COLOR=#262730" \
  --timeout=300 \
  --project="${PROJECT}"

echo "=== Done. Service URL above. Validate via: ==="
echo "gcloud run services proxy ${SERVICE} --region=${REGION} --project=${PROJECT} --port=<local-port>"
