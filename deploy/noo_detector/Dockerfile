# Build context: repo root. Scoped requirements.txt, not the shared one.
# NOTE: this app returns NIK/NPWP (PII) with no authentication in its
# current form - deployed with --no-allow-unauthenticated for the pilot
# (see docs/migration/GCP_REQUIREMENTS.md and the scorecard notes). Do not
# make this service publicly invokable before an auth layer is added.
FROM python:3.11-slim

WORKDIR /app

COPY deploy/noo_detector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY noo_detector.py .

ENV PORT=8080
EXPOSE 8080

CMD streamlit run noo_detector.py \
    --server.port=${PORT} \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false
