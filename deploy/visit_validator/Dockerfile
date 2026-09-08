# Build context: repo root (visit_validator/ is already a self-contained app
# with zero external service dependencies - no BigQuery, no GCS, no auth -
# see docs/migration/FEATURE_MIGRATION_MATRIX.md for why this was chosen as
# the first migration pilot).
FROM python:3.11-slim

WORKDIR /app

COPY visit_validator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY visit_validator/app.py .
COPY visit_validator/utils ./utils
COPY visit_validator/components ./components
COPY visit_validator/.streamlit ./.streamlit

ENV PORT=8080
EXPOSE 8080

CMD streamlit run app.py \
    --server.port=${PORT} \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false
