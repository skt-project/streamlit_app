"""
STEP Backend API — FastAPI app entrypoint
Run locally: uvicorn main:app --reload --port 8000
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from config import settings
from routers import (
    auth, dashboard, photo, product, route, salesman, schedule, sku, stock, visit,
    skipped_store, weekly_cleanup,
    # Web app routers
    dashboard_web, announcement, approval, target_web, evaluate_web,
    route_planner, report_web, salesman_web, outlet_web, notification, admin_web,
    store_opportunity, pjp_upload, import_export,
)

limiter = Limiter(key_func=get_remote_address, default_limits=["200/minute"])

app = FastAPI(
    title="STEP API",
    description="Skintific Territory & Execution Platform backend",
    version="1.4.0",
    docs_url="/docs",
    redoc_url=None,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mobile app routers
app.include_router(auth.router,      prefix="/api/v1")
# salesman_web must be included BEFORE salesman so that static segments
# (/salesman/list, /salesman/search, /salesman/360/...) take priority
# over the dynamic /salesman/{sk} route in FastAPI's route list.
app.include_router(salesman_web.router,   prefix="/api/v1")
app.include_router(salesman.router,  prefix="/api/v1")
app.include_router(route.router,     prefix="/api/v1")
app.include_router(visit.router,     prefix="/api/v1")
app.include_router(schedule.router,  prefix="/api/v1")
app.include_router(photo.router,     prefix="/api/v1")
app.include_router(sku.router,       prefix="/api/v1")
app.include_router(product.router,   prefix="/api/v1")
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
app.include_router(outlet_web.router,     prefix="/api/v1")
app.include_router(notification.router,   prefix="/api/v1")
app.include_router(admin_web.router,      prefix="/api/v1")
app.include_router(store_opportunity.router, prefix="/api/v1")
app.include_router(pjp_upload.router,     prefix="/api/v1")
app.include_router(skipped_store.router,   prefix="/api/v1")
app.include_router(weekly_cleanup.router,  prefix="/api/v1")
app.include_router(import_export.router,   prefix="/api/v1")


@app.get("/health")
def health():
    return {"status": "ok", "version": "1.4.0"}
