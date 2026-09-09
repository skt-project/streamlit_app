# Custom Dockerfile - this app registers two local TTF fonts unconditionally
# at import time (pdfmetrics.registerFont), which crashes without them; a
# single-entrypoint COPY can't cover that. logo.png is NOT copied - both
# usages already gracefully fall back (a remote URL, or an os.path.exists
# guard) when it's absent, confirmed by reading the source.
FROM python:3.11-slim

WORKDIR /app

COPY deploy/po_simulator_v2/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY po_simulator_v2.py .
COPY trebuc.ttf .
COPY trebucbd.ttf .

ENV PORT=8080
EXPOSE 8080

CMD streamlit run po_simulator_v2.py \
    --server.port=${PORT} \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false
