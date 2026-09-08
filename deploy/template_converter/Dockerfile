# Build context: repo root. Scoped requirements.txt (not the shared
# repo-wide one) - this app doesn't need geopandas/folium/matplotlib/
# reportlab/gspread/bcrypt/etc. See docs/migration/MIGRATION_PLAN.md
# section 5 (M-7) for why per-app dependency isolation matters.
FROM python:3.11-slim

WORKDIR /app

COPY deploy/template_converter/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY template_converter.py .

ENV PORT=8080
EXPOSE 8080

CMD streamlit run template_converter.py \
    --server.port=${PORT} \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false
