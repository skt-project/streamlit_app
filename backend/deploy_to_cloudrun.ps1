# Deploy STEP Backend to Cloud Run
# Run this after installing Google Cloud SDK: https://cloud.google.com/sdk/docs/install
#
# One-time setup:
#   gcloud auth login
#   gcloud config set project skintific-data-warehouse
#
# Then run this script:
#   .\deploy_to_cloudrun.ps1

$PROJECT   = "skintific-data-warehouse"
$REGION    = "asia-southeast1"
$SERVICE   = "step-api"
$IMAGE     = "gcr.io/$PROJECT/$SERVICE"

Write-Host "=== Building Docker image ===" -ForegroundColor Cyan
docker build -t "$IMAGE`:latest" "D:\GitHub\skintific-step\backend"

if ($LASTEXITCODE -ne 0) { Write-Error "Docker build failed"; exit 1 }

Write-Host "`n=== Authenticating Docker to GCR ===" -ForegroundColor Cyan
gcloud auth configure-docker --quiet

Write-Host "`n=== Pushing image to GCR ===" -ForegroundColor Cyan
docker push "$IMAGE`:latest"

if ($LASTEXITCODE -ne 0) { Write-Error "Docker push failed"; exit 1 }

Write-Host "`n=== Deploying to Cloud Run ===" -ForegroundColor Cyan
gcloud run deploy $SERVICE `
  --image "$IMAGE`:latest" `
  --region $REGION `
  --project $PROJECT `
  --platform managed `
  --quiet

Write-Host "`n=== Done! ===" -ForegroundColor Green
Write-Host "Service URL: https://$SERVICE-141828905128.$REGION.run.app/api/v1/auth/login"
