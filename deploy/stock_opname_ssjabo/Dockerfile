# Build context: repo root. Scoped requirements.txt, not the shared one.
FROM python:3.11-slim

WORKDIR /app

COPY deploy/stock_opname_ssjabo/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY stock_opname_ssjabo.py .

ENV PORT=8080
EXPOSE 8080

CMD streamlit run stock_opname_ssjabo.py \
    --server.port=${PORT} \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false
