# Shared Dockerfile template for every Streamlit-on-Cloud-Run migration pilot.
# Do not copy/fork this per app - pass ENTRYPOINT_FILE and REQUIREMENTS_PATH as
# --build-arg instead (see deploy/cloudbuild-template.yaml and
# deploy/deploy_pilot.sh). One template, many apps - see docs/migration/
# DEPLOYMENT_TEMPLATE.md for the full pattern and when to deviate from it.
FROM python:3.11-slim

WORKDIR /app

ARG ENTRYPOINT_FILE
ARG REQUIREMENTS_PATH
ENV ENTRYPOINT_FILE=${ENTRYPOINT_FILE}

COPY ${REQUIREMENTS_PATH} ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ${ENTRYPOINT_FILE} .

ENV PORT=8080
EXPOSE 8080

CMD streamlit run ${ENTRYPOINT_FILE} \
    --server.port=${PORT} \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false
