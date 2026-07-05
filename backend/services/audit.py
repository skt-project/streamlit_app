"""
Audit log writer — inserts into sfa_web.audit_log.
Fire-and-forget: exceptions are swallowed so audit failures never break API calls.
"""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from typing import Any

from config import settings


def log_event(
    action: str,
    entity_type: str = "",
    entity_id: str = "",
    performed_by: str = "",
    dist_code: str = "",
    payload: dict[str, Any] | None = None,
) -> None:
    try:
        from services.bq import BQClient
        bq = BQClient.get()
        now = datetime.now(timezone.utc)
        bq.execute(
            f"""
            INSERT INTO `{settings.bq_project}.{settings.bq_dataset}.audit_log`
              (event_id, event_ts, event_date, dist_code, session_id,
               entity_type, action, entity_id, payload_json, performed_by)
            VALUES
              (@eid, @ts, @dt, @dc, @sid, @et, @act, @entid, @pj, @by)
            """,
            [
                bq.p("eid",   "STRING",    str(uuid.uuid4())),
                bq.p("ts",    "TIMESTAMP", now.isoformat()),
                bq.p("dt",    "DATE",      now.date().isoformat()),
                bq.p("dc",    "STRING",    dist_code or ""),
                bq.p("sid",   "STRING",    ""),
                bq.p("et",    "STRING",    entity_type),
                bq.p("act",   "STRING",    action),
                bq.p("entid", "STRING",    entity_id),
                bq.p("pj",    "STRING",    json.dumps(payload or {})),
                bq.p("by",    "STRING",    performed_by),
            ],
        )
    except Exception:
        pass
