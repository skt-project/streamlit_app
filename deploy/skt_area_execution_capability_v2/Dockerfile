# Custom Dockerfile (not the shared _template one) - this app imports two
# local modules (assessment_logic.py, assessment_email.py) and reads local
# assets (assets/, User_Guide*.html, screenshots/) that a single-entrypoint
# COPY can't cover. See DEPLOYMENT_TEMPLATE.md section 2 for when to
# deviate from the shared template.
FROM python:3.11-slim

WORKDIR /app

COPY deploy/skt_area_execution_capability_v2/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY skt_area_execution_capability_v2.py .
COPY assessment_logic.py .
COPY assessment_email.py .
COPY assets ./assets
COPY screenshots ./screenshots
COPY User_Guide.html .
COPY User_Guide_ID.html .

ENV PORT=8080
EXPOSE 8080

CMD streamlit run skt_area_execution_capability_v2.py \
    --server.port=${PORT} \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false
