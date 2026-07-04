"""
STEP Backend API — FastAPI app entrypoint
Run locally: uvicorn main:app --reload --port 8000
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from routers import (
    auth, dashboard, photo, route, salesman, schedule, sku, stock, visit,
    # Web app routers
    dashboard_web, announcement, approval, target_web, evaluate_web,
    route_planner, report_web, salesman_web, outlet_web, notification, admin_web,
    store_opportunity,
)

app = FastAPI(
    title="STEP API",
    description="Skintific Territory & Execution Platform backend",
    version="1.2.0",
    docs_url="/docs",
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mobile app routers
app.include_router(auth.router,      prefix="/api/v1")
app.include_router(salesman.router,  prefix="/api/v1")
app.include_router(route.router,     prefix="/api/v1")
app.include_router(visit.router,     prefix="/api/v1")
app.include_router(schedule.router,  prefix="/api/v1")
app.include_router(photo.router,     prefix="/api/v1")
app.include_router(sku.router,       prefix="/api/v1")
app.include_router(stock.router,     prefix="/api/v1")
app.include_router(dashboard.router, prefix="/api/v1")

# Web app routers
app.include_router(dashboard_web.router,  prefix="/api/v1")
app.include_router(announcement.router,   prefix="/api/v1")
app.include_router(approval.router,       prefix="/api/v1")
app.include_router(target_web.router,     prefix="/api/v1")
app.include_router(evaluate_web.router,   prefix="/api/v1")
app.include_router(route_planner.router,  prefix="/api/v1")
app.include_router(report_web.router,     prefix="/api/v1")
app.include_router(salesman_web.router,   prefix="/api/v1")
app.include_router(outlet_web.router,     prefix="/api/v1")
app.include_router(notification.router,   prefix="/api/v1")
app.include_router(admin_web.router,          prefix="/api/v1")
app.include_router(store_opportunity.router,  prefix="/api/v1")


@app.get("/health")
def health():
    return {"status": "ok", "version": "1.2.0"}
