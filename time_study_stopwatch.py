import os
import json
import logging
import sys
import hashlib
import tempfile
import pathlib
from datetime import datetime, timezone, timedelta
from typing import Tuple, List, Dict, Optional

import streamlit as st
import streamlit.components.v1 as components
from google.oauth2 import service_account
from google.cloud import bigquery

# ============================================================
# CONFIGURATION
# ============================================================
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

class Config:
    BQ_PROJECT       = "skintific-data-warehouse"
    BQ_DATASET       = "gt_schema"
    BQ_TABLE         = "sales_activity_records"
    CREDENTIALS_PATH = r"C:\Users\Bella Chelsea\Documents\skintific-data-warehouse-ea77119e2e7a.json"
    TZ_OFFSET        = timedelta(hours=7)   # WIB = UTC+7

st.set_page_config(
    page_title="Sales Activity Timer",
    page_icon="⏱️",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.markdown("""
<style>
  #MainMenu{visibility:hidden;}footer{visibility:hidden;}header{visibility:hidden;}
  .block-container{padding:0!important;max-width:100%!important;}
</style>
""", unsafe_allow_html=True)

# ============================================================
# BigQuery READ query
# ============================================================
BQ_QUERY = """
    SELECT upper(spv_g2g) as spv,
    CASE WHEN region_g2g='' THEN upper(region) ELSE upper(region_g2g) END region,
    upper(distributor_g2g) as distributor, cust_id AS store_id, store_name
    FROM `gt_schema.master_store_database_basis`
    WHERE spv_g2g <> '' AND spv_g2g <> '-' AND distributor_g2g<>'-'
    ORDER BY spv, region, distributor, store_name
"""

# ============================================================
# BigQuery DDL
# ============================================================
BQ_DDL = f"""
CREATE TABLE IF NOT EXISTS `{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}` (
    event_id             STRING    NOT NULL,
    spv                  STRING    NOT NULL,
    region               STRING    NOT NULL,
    distributor          STRING    NOT NULL,
    activity_key         STRING    NOT NULL,
    activity_label       STRING    NOT NULL,
    logged_at            TIMESTAMP NOT NULL,
    store_id             STRING,
    store_name           STRING,
    duration_seconds     INT64,
    started_at           TIMESTAMP,
    ended_at             TIMESTAMP,
    latitude             FLOAT64,
    longitude            FLOAT64,
    location_accuracy_m  INT64,
    created_at           TIMESTAMP,
    device_id            STRING
)
PARTITION BY DATE(logged_at)
CLUSTER BY spv, distributor, store_id
"""

# ============================================================
# CREDENTIAL LOADERS
# ============================================================
def _load_from_streamlit_secrets(key: str) -> Tuple:
    if key == "connections":
        d = dict(st.secrets["connections"]["bigquery"])
    else:
        d = dict(st.secrets[key])
    if "private_key" in d:
        d["private_key"] = d["private_key"].replace("\\n", "\n")
    creds = service_account.Credentials.from_service_account_info(d)
    project = d.get("project_id") or d.get("project") or Config.BQ_PROJECT
    return creds, project

def _load_from_local_file() -> Tuple:
    if not os.path.exists(Config.CREDENTIALS_PATH):
        raise FileNotFoundError(f"Not found: {Config.CREDENTIALS_PATH}")
    creds = service_account.Credentials.from_service_account_file(Config.CREDENTIALS_PATH)
    return creds, Config.BQ_PROJECT

@st.cache_resource
def get_bq_client():
    for name, fn in [
        ("local JSON",           _load_from_local_file),
        ("gcp_service_account",  lambda: _load_from_streamlit_secrets("gcp_service_account")),
        ("connections.bigquery", lambda: _load_from_streamlit_secrets("connections")),
    ]:
        try:
            creds, project = fn()
            client = bigquery.Client(credentials=creds, project=project)
            logger.info(f"BQ client OK via {name}")
            return client
        except Exception as e:
            logger.warning(f"{name} failed: {e}")
    try:
        client = bigquery.Client()
        logger.info("BQ client OK via ADC")
        return client
    except Exception as e:
        logger.error(f"ADC failed: {e}")
        return None

def ensure_table(client: bigquery.Client):
    try:
        client.query(BQ_DDL).result()
        logger.info(f"Table {Config.BQ_TABLE} ready.")
    except Exception as e:
        logger.error(f"DDL failed: {e}")

# ============================================================
# LOAD MASTER DATA
# ============================================================
@st.cache_data(ttl=3600)
def load_master_data() -> dict:
    client = get_bq_client()
    if client is None:
        return {"spv_list": [], "by_spv": {}, "error": "Cannot connect to BigQuery."}
    try:
        rows = list(client.query(BQ_QUERY).result())
    except Exception as e:
        logger.error(f"BQ query error: {e}")
        return {"spv_list": [], "by_spv": {}, "error": str(e)}

    by_spv: dict = {}
    for row in rows:
        spv   = (row.spv         or "").strip()
        reg   = (row.region      or "").strip()
        dist  = (row.distributor or "").strip()
        sid   = str(row.store_id or "").strip()
        sname = (row.store_name  or "").strip()
        if not spv or not reg or not dist:
            continue
        bs = by_spv.setdefault(spv, {"regions": [], "by_region": {}})
        if reg not in bs["regions"]:
            bs["regions"].append(reg)
        br = bs["by_region"].setdefault(reg, {"distributors": [], "by_dist": {}})
        if dist not in br["distributors"]:
            br["distributors"].append(dist)
        br["by_dist"].setdefault(dist, [])
        existing = {s["store_id"] for s in br["by_dist"][dist]}
        if sid not in existing:
            br["by_dist"][dist].append({"store_id": sid, "store_name": sname})
    return {"spv_list": sorted(by_spv.keys()), "by_spv": by_spv}

# ============================================================
# BQ WRITE
# ============================================================
def _parse_ts(iso: Optional[str]) -> Optional[datetime]:
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except Exception:
        return None

def _to_wib(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.astimezone(timezone(Config.TZ_OFFSET))

def _make_event_id(spv: str, activity_key: str, store_id: Optional[str], logged_at) -> str:
    ts_str = logged_at.isoformat() if isinstance(logged_at, datetime) else str(logged_at or "")
    raw = f"{spv}|{activity_key}|{store_id or ''}|{ts_str}"
    return hashlib.sha256(raw.encode()).hexdigest()

def build_rows(payload: dict) -> List[Dict]:
    spv         = payload.get("spv", "")
    region      = payload.get("region", "")
    distributor = payload.get("distributor", "")
    device_id   = payload.get("device_id")
    created_at  = datetime.now(timezone.utc)
    rows = []

    for entry in payload.get("p1Log", []):
        logged_at = _to_wib(_parse_ts(
            datetime.fromtimestamp(entry["time"] / 1000, tz=timezone.utc).isoformat()
        ))
        event_id = _make_event_id(spv, entry.get("key", ""), None, logged_at)
        rows.append({
            "event_id": event_id, "spv": spv, "region": region, "distributor": distributor,
            "activity_key": entry.get("key", ""), "activity_label": entry.get("label", ""),
            "logged_at": logged_at, "store_id": None, "store_name": None,
            "duration_seconds": None, "started_at": None, "ended_at": None,
            "latitude": None, "longitude": None, "location_accuracy_m": None,
            "created_at": created_at, "device_id": device_id,
        })

    for entry in payload.get("history", []):
        ended_at   = _to_wib(_parse_ts(entry.get("endedAt")))
        duration_s = int(entry.get("duration", 0) / 1000)
        started_at = (ended_at - timedelta(seconds=duration_s)) if ended_at and duration_s else None
        loc        = entry.get("location") or {}
        store_id   = entry.get("storeId") or None
        event_id   = _make_event_id(spv, entry.get("activity", ""), store_id, ended_at)
        rows.append({
            "event_id": event_id, "spv": spv, "region": region, "distributor": distributor,
            "activity_key": entry.get("activity", ""), "activity_label": entry.get("label", ""),
            "logged_at": ended_at, "store_id": store_id, "store_name": entry.get("store") or None,
            "duration_seconds": duration_s, "started_at": started_at, "ended_at": ended_at,
            "latitude":  loc.get("lat") if "lat" in loc else None,
            "longitude": loc.get("lng") if "lng" in loc else None,
            "location_accuracy_m": loc.get("acc") if "acc" in loc else None,
            "created_at": created_at, "device_id": device_id,
        })

    return rows

def get_existing_event_ids(client: bigquery.Client, event_ids: List[str]) -> set:
    if not event_ids:
        return set()
    id_list = ", ".join(f"'{eid}'" for eid in event_ids)
    try:
        result = client.query(f"""
            SELECT event_id FROM `{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}`
            WHERE event_id IN ({id_list})
        """).result()
        return {row.event_id for row in result}
    except Exception as e:
        logger.warning(f"Dedup check failed (will insert all): {e}")
        return set()

def write_to_bq(payload: dict) -> Tuple[bool, str]:
    client = get_bq_client()
    if client is None:
        return False, "No BQ client."
    rows = build_rows(payload)
    if not rows:
        return False, "No data to save."
    all_ids  = [r["event_id"] for r in rows]
    existing = get_existing_event_ids(client, all_ids)
    new_rows = [r for r in rows if r["event_id"] not in existing]
    if not new_rows:
        return True, f"ℹ️ {len(rows)} records already saved — no duplicates inserted."
    skipped   = len(rows) - len(new_rows)
    table_ref = f"{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}"
    try:
        errors = client.insert_rows_json(table_ref, new_rows,
                                         row_ids=[r["event_id"] for r in new_rows])
        if errors:
            return False, f"BQ insert errors: {errors}"
        msg = f"✅ {len(new_rows)} records saved to BigQuery."
        if skipped:
            msg += f" ({skipped} duplicates skipped)"
        return True, msg
    except Exception as e:
        return False, str(e)

# ============================================================
# LOAD DATA + ENSURE TABLE
# ============================================================
with st.spinner("⏳ Memuat data dari BigQuery…"):
    MASTER  = load_master_data()
    _client = get_bq_client()
    if _client:
        ensure_table(_client)

if "error" in MASTER:
    st.error(f"⚠️ BigQuery error: {MASTER['error']}")
elif not MASTER["spv_list"]:
    st.warning("⚠️ Data kosong. Periksa query / kredensial BigQuery.")

MASTER_JSON = json.dumps(MASTER, ensure_ascii=False)

# ============================================================
# HANDLE PENDING SAVE
# Runs at the top of every rerun. When the bridge component fires,
# it stores the BQ payload in session_state["bq_payload"] and calls
# st.rerun(). We process it here and show a result banner.
# ============================================================
_save_banner = st.empty()

if st.session_state.get("bq_payload"):
    _payload = st.session_state.pop("bq_payload")
    try:
        _ok, _msg = write_to_bq(_payload)
        _save_banner.success(_msg) if _ok else _save_banner.error(f"❌ {_msg}")
    except Exception as _e:
        _save_banner.error(f"❌ Exception: {_e}")

# ============================================================
# BRIDGE COMPONENT  (invisible, height=0)
# ============================================================
# Architecture:
#   1. UI iframe (components.html below) does:
#        window.parent.postMessage({type:"bq_save", payload:{...}}, "*")
#      This sends data to the PARENT Streamlit page, not another iframe.
#
#   2. Bridge iframe (declare_component below) also lives in the same
#      parent page. It listens for that postMessage and calls:
#        Streamlit.setComponentValue(payload)
#      which triggers a Python rerun with the payload as return value.
#
#   3. Python stores it in session_state and calls st.rerun() so the
#      save handler above runs and writes to BigQuery.
#
# Why this works:
#   - components.html() renders the full UI reliably (no Streamlit JS needed)
#   - declare_component() provides the setComponentValue channel
#   - postMessage between siblings via the shared parent is standard browser API
#   - The bridge HTML is tiny and has no UI — it just relays messages
#
# Why declare_component(path=) is safe here:
#   - The bridge HTML does NOT use window.location or streamlit-component-lib.js
#   - It only needs the Streamlit object, which IS injected by Streamlit's
#     component server when the iframe loads index.html from the path= dir
# ============================================================

# In BRIDGE_HTML — listen on parent, which DOES receive the postMessage:
BRIDGE_HTML = """<!DOCTYPE html>
<html><head><meta charset="UTF-8"/></head>
<body><script>
(function init() {
  if (!window.Streamlit) { setTimeout(init, 20); return; }
  Streamlit.setComponentReady();
  Streamlit.setFrameHeight(0);
  
  // Listen on the PARENT window (we have access to it via window.parent)
  window.parent.addEventListener("message", function(e) {
    if (e.data && e.data.type === "bq_save") {
      Streamlit.setComponentValue(e.data.payload);
    }
  });
})();
</script></body></html>"""

# Write bridge to a stable temp dir (same content = same path across reruns)
_bridge_dir = pathlib.Path(tempfile.gettempdir()) / "st_bq_bridge_v1"
_bridge_dir.mkdir(exist_ok=True)
(_bridge_dir / "index.html").write_text(BRIDGE_HTML, encoding="utf-8")

_bridge      = components.declare_component("bq_bridge", path=str(_bridge_dir))
_bridge_val  = _bridge(key="bq_bridge", default=None)

if _bridge_val is not None:
    st.session_state["bq_payload"] = _bridge_val
    st.rerun()

# ============================================================
# MAIN APP  —  components.html (no Streamlit JS API needed, always works)
# ============================================================
APP_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;700&display=swap');
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
:root{{
  --bg:#0a0c10;--surface:#12151c;--surface2:#1a1e28;
  --border:rgba(255,255,255,0.08);--accent:#f5a623;--accent2:#e05c5c;
  --green:#4ade80;--blue:#60a5fa;--purple:#a78bfa;--teal:#2dd4bf;
  --text:#e8eaf0;--muted:#6b7280;--radius:14px;
}}
body{{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;}}
.page{{display:none;flex-direction:column;min-height:100vh;padding:20px;max-width:880px;margin:0 auto;gap:16px;padding-bottom:40px;}}
.page.active{{display:flex;}}
.topbar{{display:flex;align-items:center;justify-content:space-between;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px 18px;position:relative;overflow:hidden;flex-shrink:0;}}
.topbar::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--accent),#f97316,var(--accent2));}}
.topbar-title{{font-family:'Bebas Neue',sans-serif;font-size:1.5rem;letter-spacing:2px;color:var(--accent);}}
.topbar-sub{{font-size:0.7rem;color:var(--muted);letter-spacing:1px;text-transform:uppercase;margin-top:2px;}}
.topbar-badge{{background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:6px 14px;font-size:0.76rem;color:var(--text);text-align:right;line-height:1.7;max-width:60%;}}
.topbar-badge strong{{color:var(--accent);display:block;font-size:0.82rem;}}
.nav{{display:flex;gap:4px;background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:4px;}}
.nav-tab{{flex:1;text-align:center;padding:10px 6px;border-radius:9px;font-size:0.78rem;font-weight:600;letter-spacing:0.5px;cursor:pointer;transition:all 0.18s;color:var(--muted);border:none;background:none;font-family:'DM Sans',sans-serif;}}
.nav-tab.active{{background:var(--accent);color:#0a0c10;}}
.nav-tab:not(.active):hover{{background:var(--surface2);color:var(--text);}}
.section-label{{font-size:0.68rem;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--muted);margin-bottom:10px;}}
.field-label{{font-size:0.68rem;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--muted);margin-bottom:6px;}}
.search-wrap{{position:relative;margin-bottom:8px;}}
.search-wrap input{{width:100%;background:var(--surface2);border:1.5px solid var(--border);border-radius:10px;padding:12px 16px;font-size:0.9rem;color:var(--text);font-family:'DM Sans',sans-serif;outline:none;transition:border-color 0.18s;}}
.search-wrap input:focus{{border-color:var(--accent);}}
.search-wrap input::placeholder{{color:var(--muted);}}
.dropdown-list{{background:var(--surface2);border:1.5px solid var(--accent);border-radius:10px;margin-top:4px;max-height:200px;overflow-y:auto;display:none;z-index:50;position:absolute;width:100%;left:0;}}
.dropdown-list.open{{display:block;}}
.dropdown-item{{padding:10px 16px;font-size:0.88rem;cursor:pointer;transition:background 0.15s;border-bottom:1px solid var(--border);}}
.dropdown-item:last-child{{border-bottom:none;}}
.dropdown-item:hover,.dropdown-item.selected{{background:rgba(245,166,35,0.12);color:var(--accent);}}
.selected-badge{{display:inline-flex;align-items:center;gap:8px;background:rgba(245,166,35,0.12);border:1px solid rgba(245,166,35,0.3);border-radius:20px;padding:4px 14px;font-size:0.78rem;color:var(--accent);margin-top:4px;}}
.select-card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:28px;}}
.select-card h2{{font-family:'Bebas Neue',sans-serif;font-size:2rem;letter-spacing:2px;color:var(--text);margin-bottom:4px;}}
.select-card p{{font-size:0.83rem;color:var(--muted);margin-bottom:24px;}}
.btn-proceed{{width:100%;margin-top:24px;background:var(--accent);color:#0a0c10;border:none;border-radius:10px;padding:14px;font-size:1rem;font-weight:700;font-family:'DM Sans',sans-serif;cursor:pointer;transition:all 0.18s;letter-spacing:0.5px;}}
.btn-proceed:hover{{filter:brightness(1.1);}}
.btn-proceed:disabled{{opacity:0.35;cursor:not-allowed;}}
select.act-sel{{width:100%;background:var(--surface2);border:1.5px solid var(--border);border-radius:10px;padding:12px 16px;font-size:0.9rem;color:var(--text);font-family:'DM Sans',sans-serif;outline:none;cursor:pointer;transition:border-color 0.18s;}}
select.act-sel:focus{{border-color:var(--accent);}}
.clock-card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:26px 24px 20px;text-align:center;position:relative;overflow:hidden;}}
.clock-card::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,var(--accent),#f97316,var(--accent2));border-radius:3px 3px 0 0;}}
.act-label-sm{{font-size:0.68rem;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:var(--muted);margin-bottom:6px;}}
.act-name-big{{font-size:1.05rem;font-weight:600;color:var(--accent);margin-bottom:16px;min-height:26px;}}
.time-display{{font-family:'JetBrains Mono',monospace;font-size:4.8rem;font-weight:700;color:var(--text);letter-spacing:-2px;line-height:1;transition:color 0.3s;}}
.time-display.running{{color:var(--green);text-shadow:0 0 40px rgba(74,222,128,0.25);}}
.time-display.paused{{color:var(--accent);text-shadow:0 0 30px rgba(245,166,35,0.2);}}
.status-row2{{display:flex;align-items:center;justify-content:center;gap:8px;margin-top:10px;}}
.sdot{{width:8px;height:8px;border-radius:50%;background:var(--muted);transition:background 0.3s;}}
.sdot.running{{background:var(--green);animation:blink 1.2s infinite;}}
.sdot.paused{{background:var(--accent);}}
@keyframes blink{{0%,100%{{opacity:1;}}50%{{opacity:0.3;}}}}
.stext{{font-size:0.72rem;color:var(--muted);letter-spacing:1px;text-transform:uppercase;}}
.ctrl-row{{display:flex;gap:10px;justify-content:center;margin-top:18px;flex-wrap:wrap;}}
.btn-ctrl{{font-family:'DM Sans',sans-serif;font-size:0.88rem;font-weight:700;border:none;border-radius:10px;padding:12px 26px;cursor:pointer;transition:all 0.18s;letter-spacing:0.5px;}}
.btn-ctrl:active{{transform:scale(0.96);}}
.btn-start{{background:var(--green);color:#0a0c10;}}
.btn-pause{{background:var(--accent);color:#0a0c10;}}
.btn-stop{{background:var(--accent2);color:#fff;}}
.btn-save{{background:var(--blue);color:#0a0c10;font-size:0.9rem;padding:12px 20px;}}
.btn-start:hover,.btn-pause:hover,.btn-stop:hover,.btn-save:hover{{filter:brightness(1.1);}}
.btn-ctrl:disabled{{opacity:0.4;cursor:not-allowed;filter:none;}}
.totals-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(115px,1fr));gap:8px;}}
.total-chip{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:10px 12px;}}
.chip-icon{{font-size:1.05rem;}}
.chip-label{{font-size:0.65rem;color:var(--muted);font-weight:600;margin-top:2px;line-height:1.3;}}
.chip-time{{font-family:'JetBrains Mono',monospace;font-size:0.8rem;color:var(--text);margin-top:3px;}}
.history-box{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;}}
.history-hdr{{padding:12px 16px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border);}}
.history-hdr span{{font-size:0.68rem;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--muted);}}
.clr-btn{{font-family:'DM Sans',sans-serif;font-size:0.72rem;font-weight:500;color:var(--accent2);background:none;border:none;cursor:pointer;opacity:0.7;}}
.clr-btn:hover{{opacity:1;}}
table{{width:100%;border-collapse:collapse;font-size:0.79rem;}}
thead tr{{background:var(--surface2);}}
th{{padding:9px 12px;text-align:left;font-size:0.64rem;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:var(--muted);}}
td{{padding:9px 12px;border-top:1px solid var(--border);vertical-align:middle;}}
.tag{{display:inline-block;padding:2px 9px;border-radius:20px;font-size:0.7rem;font-weight:600;background:rgba(245,166,35,0.15);color:var(--accent);}}
.mono{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;}}
.no-hist{{padding:28px;text-align:center;color:var(--muted);font-size:0.82rem;}}
.back-btn{{font-family:'DM Sans',sans-serif;font-size:0.72rem;font-weight:700;background:var(--surface2);border:1px solid var(--border);color:var(--muted);border-radius:8px;padding:5px 12px;cursor:pointer;letter-spacing:0.5px;transition:all 0.18s;}}
.back-btn:hover{{border-color:var(--accent2);color:var(--accent2);}}
.save-banner{{background:var(--surface);border:1px solid rgba(96,165,250,0.3);border-radius:var(--radius);padding:14px 18px;display:flex;align-items:center;justify-content:space-between;gap:12px;}}
.save-banner-txt{{font-size:0.8rem;color:var(--muted);line-height:1.5;}}
.save-banner-txt strong{{color:var(--blue);display:block;font-size:0.88rem;margin-bottom:2px;}}
</style>
</head>
<body>

<!-- ═══ PAGE 0 ═══ -->
<div class="page active" id="page0">
  <div class="topbar">
    <div><div class="topbar-title">⏱ Sales Timer</div><div class="topbar-sub">Activity Tracking System</div></div>
  </div>
  <div class="select-card">
    <h2>Selamat Datang</h2>
    <p>Pilih SPV, Region, dan Distributor sebelum memulai kunjungan.</p>
    <div class="field-label">👤 Supervisor (SPV)</div>
    <div class="search-wrap">
      <input type="text" id="spvInput" placeholder="Cari nama SPV…" autocomplete="off"
        oninput="p0filter('spv')" onfocus="p0open('spv')" onblur="p0close('spv')"/>
      <div class="dropdown-list" id="spvDrop"></div>
    </div>
    <div id="spvBadge"></div>
    <div style="height:18px"></div>
    <div class="field-label">🗺️ Region</div>
    <div class="search-wrap">
      <input type="text" id="regionInput" placeholder="Pilih SPV dulu…" autocomplete="off"
        oninput="p0filter('region')" onfocus="p0open('region')" onblur="p0close('region')" disabled/>
      <div class="dropdown-list" id="regionDrop"></div>
    </div>
    <div id="regionBadge"></div>
    <div style="height:18px"></div>
    <div class="field-label">🏭 Distributor</div>
    <div class="search-wrap">
      <input type="text" id="distInput" placeholder="Pilih Region dulu…" autocomplete="off"
        oninput="p0filter('dist')" onfocus="p0open('dist')" onblur="p0close('dist')" disabled/>
      <div class="dropdown-list" id="distDrop"></div>
    </div>
    <div id="distBadge"></div>
    <button class="btn-proceed" id="proceedBtn" disabled onclick="proceedToApp()">Mulai Kunjungan →</button>
  </div>
</div>

<!-- ═══ PAGE 1 ═══ -->
<div class="page" id="page1">
  <div class="topbar">
    <div><div class="topbar-title">⏱ Sales Timer</div><div class="topbar-sub">Check In / Out & ISHOMA</div></div>
    <div style="display:flex;flex-direction:column;align-items:flex-end;gap:8px;">
      <button class="back-btn" onclick="goHome()">← Kembali</button>
      <div class="topbar-badge" id="badge1">—</div>
    </div>
  </div>
  <div class="nav">
    <button class="nav-tab active" onclick="showPage(1)">📍 Check In/Out</button>
    <button class="nav-tab" onclick="showPage(2)">📋 Activities</button>
  </div>
  <div>
    <div class="section-label">📋 Pilih Aktivitas</div>
    <select class="act-sel" id="p1Select">
      <option value="">— Pilih Aktivitas —</option>
      <option value="dist_in">📥 Check In Distributor</option>
      <option value="dist_out">📤 Check Out Distributor</option>
      <option value="ish_in">🍽️ Check In ISHOMA</option>
      <option value="ish_out">🍽️ Check Out ISHOMA</option>
    </select>
  </div>
  <button onclick="logP1Action()" style="width:100%;background:var(--accent);color:#0a0c10;border:none;border-radius:10px;padding:14px;font-size:1rem;font-weight:700;font-family:'DM Sans',sans-serif;cursor:pointer;letter-spacing:0.5px;">
    ✅ Catat Waktu Sekarang
  </button>
  <div class="history-box">
    <div class="history-hdr">
      <span>📜 Log Aktivitas</span>
      <button class="clr-btn" onclick="clearP1Log()">Clear</button>
    </div>
    <div id="p1LogBody"><div class="no-hist">Belum ada aktivitas tercatat.</div></div>
  </div>
  <div class="save-banner">
    <div class="save-banner-txt">
      <strong>💾 Simpan Check In/Out ke BigQuery</strong>
      Simpan semua log hari ini
    </div>
    <button class="btn-ctrl btn-save" id="saveBtn1" onclick="saveAll()">Simpan</button>
  </div>
</div>

<!-- ═══ PAGE 2 ═══ -->
<div class="page" id="page2">
  <div class="topbar">
    <div><div class="topbar-title">⏱ Sales Timer</div><div class="topbar-sub">Activity Stopwatch</div></div>
    <div style="display:flex;flex-direction:column;align-items:flex-end;gap:8px;">
      <button class="back-btn" onclick="goHome()">← Kembali</button>
      <div class="topbar-badge" id="badge2">—</div>
    </div>
  </div>
  <div class="nav">
    <button class="nav-tab" onclick="showPage(1)">📍 Check In/Out</button>
    <button class="nav-tab active" onclick="showPage(2)">📋 Activities</button>
  </div>
  <div>
    <div class="section-label">🏪 Pilih Toko</div>
    <div class="search-wrap">
      <input type="text" id="actStoreInput" placeholder="Cari nama toko…" autocomplete="off"
        oninput="filterActStore()" onfocus="openActStore()" onblur="delayCloseActStore()"/>
      <div class="dropdown-list" id="actStoreDrop"></div>
    </div>
    <div id="actStoreBadge"></div>
  </div>
  <div>
    <div class="section-label">🗂 Pilih Aktivitas</div>
    <select class="act-sel" id="actSelect" onchange="onActivityChange()">
      <option value="">— Pilih Aktivitas —</option>
      <option value="preparation">🗂️  Preparation</option>
      <option value="greetings">👋  Greetings</option>
      <option value="stock_check">📦  Stock Check</option>
      <option value="merchandise">🛍️  Merchandise</option>
      <option value="payment">💳  Payment</option>
      <option value="sales_presentation">📊  Sales Presentation</option>
      <option value="taking_order">📝  Taking Order</option>
    </select>
  </div>
  <div class="clock-card">
    <div class="act-label-sm">Current Activity</div>
    <div class="act-name-big" id="actNameBig">— None Selected —</div>
    <div class="time-display" id="timerDisplay">00:00:00</div>
    <div class="status-row2"><div class="sdot" id="sDot"></div><div class="stext" id="sTxt">Idle</div></div>
    <div class="ctrl-row">
      <button class="btn-ctrl btn-start" id="btnStart" onclick="startTimer()">▶ Start</button>
      <button class="btn-ctrl btn-pause" id="btnPause" onclick="pauseTimer()" style="display:none">⏸ Pause</button>
      <button class="btn-ctrl btn-stop"  id="btnStop"  onclick="stopTimer()">⏹ Stop</button>
    </div>
  </div>
  <div>
    <div class="section-label">📊 Total Waktu Per Aktivitas</div>
    <div class="totals-grid" id="totalsGrid"></div>
  </div>
  <div class="history-box">
    <div class="history-hdr">
      <span>📜 Session Log</span>
      <button class="clr-btn" onclick="clearHistory()">Clear All</button>
    </div>
    <div id="histBody"><div class="no-hist">No sessions recorded yet.</div></div>
  </div>
  <div id="storeSubmitPanel" style="display:none;">
    <div style="background:var(--surface);border:1.5px solid rgba(74,222,128,0.3);border-radius:var(--radius);padding:16px 18px;">
      <div style="font-size:0.68rem;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--green);margin-bottom:8px;">✅ Submit Audit Toko</div>
      <div id="storeSubmitStatus" style="font-size:0.8rem;color:var(--muted);margin-bottom:12px;line-height:1.6;"></div>
      <button class="btn-ctrl" id="storeSubmitBtn" onclick="saveStore(S.actStoreId)"
        style="width:100%;background:var(--green);color:#0a0c10;">Submit Toko Ini</button>
    </div>
  </div>
</div>

<script>
// ═══════════════════════════════════════════════════
// MASTER DATA
// ═══════════════════════════════════════════════════
const MASTER = {MASTER_JSON};

const ACTIVITIES = [
  {{id:'preparation',        label:'Preparation',        icon:'🗂️'}},
  {{id:'greetings',          label:'Greetings',          icon:'👋'}},
  {{id:'stock_check',        label:'Stock Check',        icon:'📦'}},
  {{id:'merchandise',        label:'Merchandise',        icon:'🛍️'}},
  {{id:'payment',            label:'Payment',            icon:'💳'}},
  {{id:'sales_presentation', label:'Sales Presentation', icon:'📊'}},
  {{id:'taking_order',       label:'Taking Order',       icon:'📝'}},
];

// ═══════════════════════════════════════════════════
// STATE
// ═══════════════════════════════════════════════════
const LS_KEY = 'salesTimerV6';

function genDeviceId() {{
  const raw = [navigator.userAgent, screen.width+'x'+screen.height,
               navigator.language, Intl.DateTimeFormat().resolvedOptions().timeZone].join('|');
  let h = 0;
  for (let i = 0; i < raw.length; i++) {{ h = Math.imul(31, h) + raw.charCodeAt(i) | 0; }}
  return 'dev_' + Math.abs(h).toString(16);
}}

function defState() {{
  return {{
    spv:'', region:'', distributor:'', device_id: genDeviceId(),
    p1Log:[], p2Location:null, actStore:'', actStoreId:'',
    activity:null, running:false, elapsed:0, startedAt:null,
    history:[], totals:{{}}, submittedStores:[],
  }};
}}

let S = defState();
try {{
  const r = localStorage.getItem(LS_KEY);
  if (r) S = {{...defState(), ...JSON.parse(r)}};
}} catch(e) {{}}
if (!S.device_id) S.device_id = genDeviceId();

function save() {{ localStorage.setItem(LS_KEY, JSON.stringify(S)); }}

// ═══════════════════════════════════════════════════
// SAVE TO BIGQUERY
// Sends a postMessage to the Streamlit parent window.
// The bridge iframe (declare_component) listens there and
// relays it to Python via Streamlit.setComponentValue().
// ═══════════════════════════════════════════════════
function _sendToBQ(payload, btnEl) {{
  if (btnEl) {{
    btnEl.dataset.origLabel = btnEl.textContent;
    btnEl.disabled = true;
    btnEl.textContent = '⏳ Menyimpan…';
    setTimeout(() => {{
      btnEl.disabled = false;
      btnEl.textContent = btnEl.dataset.origLabel || 'Simpan';
    }}, 5000);
  }}
  window.parent.frames[0] && window.parent.postMessage({type: 'bq_save', payload: payload}, '*');
}}

function saveStore(storeId) {{
  if (!storeId) {{ alert('Pilih toko terlebih dahulu.'); return; }}
  const storeRows = (S.history||[]).filter(h => h.storeId === storeId);
  if (!storeRows.length) {{ alert('Belum ada aktivitas untuk toko ini.'); return; }}
  const already = (S.submittedStores||[]).includes(storeId);
  if (already && !confirm('Toko ini sudah pernah disubmit. Submit ulang?')) return;
  S.submittedStores = S.submittedStores || [];
  if (!S.submittedStores.includes(storeId)) S.submittedStores.push(storeId);
  save();
  _sendToBQ({{
    spv:S.spv, region:S.region, distributor:S.distributor, device_id:S.device_id,
    p1Log:[], history:storeRows,
  }}, document.getElementById('storeSubmitBtn'));
  renderStoreSubmitBanner();
}}

function saveAll() {{
  if (!(S.p1Log||[]).length) {{ alert('Belum ada data Check In/Out untuk disimpan.'); return; }}
  _sendToBQ({{
    spv:S.spv, region:S.region, distributor:S.distributor, device_id:S.device_id,
    p1Log:S.p1Log||[], history:[],
  }}, document.getElementById('saveBtn1'));
}}

// ═══════════════════════════════════════════════════
// PAGE 0
// ═══════════════════════════════════════════════════
let p0sel = {{spv:'',region:'',dist:''}};

function p0getList(t) {{
  if (t==='spv')    return MASTER.spv_list||[];
  if (t==='region') return MASTER.by_spv[p0sel.spv]?.regions||[];
  return MASTER.by_spv[p0sel.spv]?.by_region[p0sel.region]?.distributors||[];
}}
function p0filter(t) {{
  const q = document.getElementById(t==='dist'?'distInput':t+'Input').value.toLowerCase();
  p0renderDrop(t, p0getList(t).filter(x=>x.toLowerCase().includes(q)));
  document.getElementById(t==='dist'?'distDrop':t+'Drop').classList.add('open');
}}
function p0open(t) {{
  const q = document.getElementById(t==='dist'?'distInput':t+'Input').value.toLowerCase();
  p0renderDrop(t, p0getList(t).filter(x=>x.toLowerCase().includes(q)||!q));
  document.getElementById(t==='dist'?'distDrop':t+'Drop').classList.add('open');
}}
function p0close(t) {{
  setTimeout(()=>document.getElementById(t==='dist'?'distDrop':t+'Drop').classList.remove('open'),220);
}}
function p0renderDrop(t, list) {{
  const dropId = t==='dist'?'distDrop':t+'Drop';
  const cur    = t==='dist'?p0sel.dist:p0sel[t];
  document.getElementById(dropId).innerHTML = list.length
    ? list.map(x=>`<div class="dropdown-item${{x===cur?' selected':''}}" onmousedown="p0select('${{t}}','${{x.replace(/'/g,"\\\\'")}}')">${{x}}</div>`).join('')
    : `<div class="dropdown-item" style="color:var(--muted)">Tidak ditemukan</div>`;
}}
function p0select(t, val) {{
  const icons={{spv:'👤',region:'🗺️',dist:'🏭'}};
  if (t==='spv') {{
    p0sel={{spv:val,region:'',dist:''}};
    ['region','dist'].forEach(k=>{{
      document.getElementById(k==='dist'?'distInput':k+'Input').value='';
      document.getElementById(k==='dist'?'distBadge':k+'Badge').innerHTML='';
    }});
    document.getElementById('regionInput').disabled=false;
    document.getElementById('regionInput').placeholder='Cari region…';
    document.getElementById('distInput').disabled=true;
    document.getElementById('distInput').placeholder='Pilih Region dulu…';
  }} else if (t==='region') {{
    p0sel.region=val; p0sel.dist='';
    document.getElementById('distInput').value='';
    document.getElementById('distBadge').innerHTML='';
    document.getElementById('distInput').disabled=false;
    document.getElementById('distInput').placeholder='Cari distributor…';
  }} else {{ p0sel.dist=val; }}
  document.getElementById(t==='dist'?'distInput':t+'Input').value=val;
  document.getElementById(t==='dist'?'distBadge':t+'Badge').innerHTML=`<div class="selected-badge">${{icons[t]}} ${{val}}</div>`;
  document.getElementById(t==='dist'?'distDrop':t+'Drop').classList.remove('open');
  document.getElementById('proceedBtn').disabled=!(p0sel.spv&&p0sel.region&&p0sel.dist);
}}

function hasCheckedIn() {{ return (S.p1Log||[]).some(e=>e.key==='dist_in'); }}

function proceedToApp() {{
  S.spv=p0sel.spv; S.region=p0sel.region; S.distributor=p0sel.dist; save();
  showActivePage(1); updateBadges(); renderPage1(); renderTimer();
  if (!hasCheckedIn()) setTimeout(()=>alert('⚠️ Jangan lupa Check In Distributor terlebih dahulu.'),300);
}}

// ═══════════════════════════════════════════════════
// NAVIGATION
// ═══════════════════════════════════════════════════
function showActivePage(n) {{
  [0,1,2].forEach(i=>document.getElementById('page'+i).classList.remove('active'));
  document.getElementById('page'+n).classList.add('active');
}}
function showPage(n) {{
  if (n===2 && !hasCheckedIn()) {{ alert('⚠️ Wajib Check In Distributor terlebih dahulu.'); return; }}
  showActivePage(n);
  document.querySelectorAll('#page'+n+' .nav-tab').forEach((tab,i)=>tab.classList.toggle('active',i===n-1));
  if (n===2) {{ renderTotals(); renderHistory(); renderStoreSubmitBanner(); }}
}}
function updateBadges() {{
  const html=`<strong>${{S.spv||'—'}}</strong>${{S.distributor||'—'}} · ${{S.region||'—'}}`;
  ['badge1','badge2'].forEach(id=>document.getElementById(id).innerHTML=html);
}}
function goHome() {{
  if (!confirm('Kembali ke halaman awal?')) return;
  if (S.running) pauseTimer();
  S.spv=''; S.region=''; S.distributor='';
  S.activity=null; S.elapsed=0; S.startedAt=null; S.running=false;
  if (rafId) {{ cancelAnimationFrame(rafId); rafId=null; }}
  save();
  p0sel={{spv:'',region:'',dist:''}};
  ['spv','region'].forEach(t=>{{
    document.getElementById(t+'Input').value='';
    document.getElementById(t+'Badge').innerHTML='';
  }});
  document.getElementById('distInput').value='';
  document.getElementById('distBadge').innerHTML='';
  document.getElementById('regionInput').disabled=true;
  document.getElementById('regionInput').placeholder='Pilih SPV dulu…';
  document.getElementById('distInput').disabled=true;
  document.getElementById('distInput').placeholder='Pilih Region dulu…';
  document.getElementById('proceedBtn').disabled=true;
  showActivePage(0);
}}

// ═══════════════════════════════════════════════════
// PAGE 1
// ═══════════════════════════════════════════════════
const P1_ACTIONS={{
  dist_in: {{label:'Check In Distributor', icon:'📥',color:'var(--green)'}},
  dist_out:{{label:'Check Out Distributor',icon:'📤',color:'var(--accent2)'}},
  ish_in:  {{label:'Check In ISHOMA',      icon:'🍽️',color:'var(--purple)'}},
  ish_out: {{label:'Check Out ISHOMA',     icon:'🍽️',color:'var(--purple)'}},
}};
function logP1Action() {{
  const sel=document.getElementById('p1Select'), val=sel.value;
  if (!val) {{ alert('Pilih aktivitas terlebih dahulu!'); return; }}
  const act=P1_ACTIONS[val];
  (S.p1Log=S.p1Log||[]).push({{key:val,label:act.label,icon:act.icon,time:Date.now()}});
  sel.value=''; save(); renderP1Log();
}}
function clearP1Log() {{ S.p1Log=[]; save(); renderP1Log(); }}
function renderP1Log() {{
  const body=document.getElementById('p1LogBody'), log=S.p1Log||[];
  if (!log.length) {{ body.innerHTML='<div class="no-hist">Belum ada aktivitas tercatat.</div>'; return; }}
  const warn=!log.some(e=>e.key==='dist_in')
    ?`<div style="background:rgba(245,166,35,0.1);border:1px solid rgba(245,166,35,0.3);border-radius:10px;padding:10px 14px;margin-bottom:10px;font-size:0.78rem;color:var(--accent);">⚠️ Belum Check In Distributor.</div>`:'';
  body.innerHTML=warn+`<table><thead><tr><th>#</th><th>Aktivitas</th><th>Waktu</th></tr></thead><tbody>`
    +log.map((e,i)=>{{
      const col=P1_ACTIONS[e.key]?.color||'var(--accent)';
      return `<tr><td class="mono" style="color:var(--muted)">${{i+1}}</td>
        <td><span class="tag" style="background:${{col}}22;color:${{col}}">${{e.icon}} ${{e.label}}</span></td>
        <td class="mono">${{fmtClock(e.time)}}</td></tr>`;
    }}).join('')+`</tbody></table>`;
}}
function renderPage1() {{ renderP1Log(); }}

// ═══════════════════════════════════════════════════
// PAGE 2 — STORE
// ═══════════════════════════════════════════════════
function getStoreList() {{
  return MASTER.by_spv?.[S.spv]?.by_region?.[S.region]?.by_dist?.[S.distributor]||[];
}}
function filterActStore() {{
  const q=document.getElementById('actStoreInput').value.toLowerCase();
  renderActStoreDrop(getStoreList().filter(x=>x.store_name.toLowerCase().includes(q)));
  document.getElementById('actStoreDrop').classList.add('open');
}}
function openActStore() {{
  const q=document.getElementById('actStoreInput').value.toLowerCase();
  renderActStoreDrop(getStoreList().filter(x=>x.store_name.toLowerCase().includes(q)||!q));
  document.getElementById('actStoreDrop').classList.add('open');
}}
function delayCloseActStore() {{ setTimeout(()=>document.getElementById('actStoreDrop').classList.remove('open'),220); }}
function renderActStoreDrop(list) {{
  document.getElementById('actStoreDrop').innerHTML=list.length
    ?list.map(x=>`<div class="dropdown-item${{x.store_id===S.actStoreId?' selected':''}}" onmousedown="selectActStore('${{x.store_id}}','${{x.store_name.replace(/'/g,"\\\\'")}}')">${{x.store_name}}</div>`).join('')
    :`<div class="dropdown-item" style="color:var(--muted)">Tidak ditemukan</div>`;
}}
function _currentStoreHasUnsubmitted() {{
  if (!S.actStoreId||(S.submittedStores||[]).includes(S.actStoreId)) return false;
  return (S.history||[]).some(h=>h.storeId===S.actStoreId);
}}
function selectActStore(id, name) {{
  if (S.actStoreId&&S.actStoreId!==id&&_currentStoreHasUnsubmitted()) {{
    alert('⚠️ Submit data toko "'+S.actStore+'" terlebih dahulu.'); return;
  }}
  if (S.actStoreId!==id) S.p2Location=null;
  S.actStoreId=id; S.actStore=name; save();
  document.getElementById('actStoreInput').value=name;
  document.getElementById('actStoreBadge').innerHTML=`<div class="selected-badge">🏪 ${{name}}</div>`;
  document.getElementById('actStoreDrop').classList.remove('open');
  renderStoreSubmitBanner();
}}
function restoreActStore() {{
  if (S.actStore) {{
    document.getElementById('actStoreInput').value=S.actStore;
    document.getElementById('actStoreBadge').innerHTML=`<div class="selected-badge">🏪 ${{S.actStore}}</div>`;
  }}
}}

// ═══════════════════════════════════════════════════
// STOPWATCH
// ═══════════════════════════════════════════════════
function getLiveElapsed() {{
  return (!S.running||!S.startedAt) ? S.elapsed : S.elapsed+(Date.now()-S.startedAt);
}}
function onActivityChange() {{
  if (S.running) pauseTimer();
  S.activity=document.getElementById('actSelect').value||null;
  S.elapsed=0; S.startedAt=null; S.running=false; save(); renderTimer();
}}
function startTimer() {{
  if (!S.activity) {{ alert('Pilih aktivitas terlebih dahulu!'); return; }}
  if (S.running) return;
  S.running=true; S.startedAt=Date.now(); save(); timerTick();
}}
function pauseTimer() {{
  if (!S.running) return;
  S.elapsed=getLiveElapsed(); S.running=false; S.startedAt=null;
  save(); cancelAnimationFrame(rafId); rafId=null; renderTimer();
}}
function stopTimer() {{
  const elapsed=getLiveElapsed();
  if (elapsed<=0||!S.activity) {{
    S.elapsed=0; S.running=false; S.startedAt=null; S.activity=null;
    document.getElementById('actSelect').value='';
    save(); cancelAnimationFrame(rafId); rafId=null; renderTimer(); return;
  }}
  const act=ACTIVITIES.find(a=>a.id===S.activity);
  const entry={{activity:S.activity,label:act?act.label:S.activity,icon:act?act.icon:'⏱',
    store:S.actStore||'—',storeId:S.actStoreId||'',duration:elapsed,
    endedAt:new Date().toISOString(),location:null}};
  S.totals[S.activity]=(S.totals[S.activity]||0)+elapsed;
  S.elapsed=0; S.running=false; S.startedAt=null; S.activity=null;
  cancelAnimationFrame(rafId); rafId=null;
  document.getElementById('actSelect').value='';
  const isFirst=!S.p2Location;
  if (isFirst&&navigator.geolocation) {{
    const btn=document.getElementById('btnStop');
    if(btn){{btn.disabled=true;btn.textContent='📡 Lokasi…';}}
    navigator.geolocation.getCurrentPosition(
      pos=>{{S.p2Location={{lat:pos.coords.latitude,lng:pos.coords.longitude,acc:Math.round(pos.coords.accuracy)}};
        entry.location=S.p2Location;S.history.unshift(entry);save();renderTimer();
        if(btn){{btn.disabled=false;btn.textContent='⏹ Stop';}}}},
      err=>{{S.p2Location={{error:err.message}};entry.location=S.p2Location;
        S.history.unshift(entry);save();renderTimer();
        if(btn){{btn.disabled=false;btn.textContent='⏹ Stop';}}}},
      {{enableHighAccuracy:true,timeout:8000,maximumAge:0}}
    );
  }} else {{ S.history.unshift(entry); save(); renderTimer(); }}
}}
function clearHistory() {{
  if (!confirm('Hapus semua session log dan reset totals?')) return;
  S.history=[]; S.totals={{}}; S.p2Location=null; S.submittedStores=[];
  save(); renderTotals(); renderHistory(); renderStoreSubmitBanner();
}}
function renderStoreSubmitBanner() {{
  const panel=document.getElementById('storeSubmitPanel');
  const status=document.getElementById('storeSubmitStatus');
  const btn=document.getElementById('storeSubmitBtn');
  if (!panel||!S.actStoreId) {{ if(panel) panel.style.display='none'; return; }}
  const rows=(S.history||[]).filter(h=>h.storeId===S.actStoreId);
  if (!rows.length) {{ panel.style.display='none'; return; }}
  panel.style.display='block';
  const done=(S.submittedStores||[]).includes(S.actStoreId);
  status.innerHTML=done
    ?`<span style="color:var(--green)">✔ Sudah disubmit (${{rows.length}} sesi).</span><br>Submit ulang jika ada tambahan.`
    :`Terdapat <strong>${{rows.length}} sesi</strong>.<br>Klik tombol untuk mengirim ke BigQuery.`;
  btn.textContent=done?'Submit Ulang':'Submit Toko Ini';
  btn.style.background=done?'var(--teal)':'var(--green)';
  btn.disabled=false;
}}
let rafId=null;
function timerTick() {{ renderTimer(); if(S.running) rafId=requestAnimationFrame(timerTick); }}
function renderTimer() {{
  const elapsed=getLiveElapsed(), act=ACTIVITIES.find(a=>a.id===S.activity);
  document.getElementById('actNameBig').textContent=act?act.icon+'  '+act.label:'— None Selected —';
  const disp=document.getElementById('timerDisplay');
  disp.textContent=fmtMs(elapsed);
  disp.className='time-display'+(S.running?' running':elapsed>0?' paused':'');
  const dot=document.getElementById('sDot'),txt=document.getElementById('sTxt');
  if(S.running){{dot.className='sdot running';txt.textContent='Recording';}}
  else if(elapsed>0){{dot.className='sdot paused';txt.textContent='Paused';}}
  else{{dot.className='sdot';txt.textContent='Idle';}}
  document.getElementById('btnStart').style.display=S.running?'none':'';
  document.getElementById('btnPause').style.display=S.running?'':'none';
  renderTotals(); renderHistory(); renderStoreSubmitBanner();
}}
function renderTotals() {{
  const elapsed=getLiveElapsed();
  document.getElementById('totalsGrid').innerHTML=ACTIVITIES.map(a=>{{
    const t=(S.totals[a.id]||0)+(S.activity===a.id?elapsed:0);
    return `<div class="total-chip"><div class="chip-icon">${{a.icon}}</div><div class="chip-label">${{a.label}}</div><div class="chip-time">${{fmtMs(t)}}</div></div>`;
  }}).join('');
}}
function renderHistory() {{
  const hb=document.getElementById('histBody');
  if (!S.history.length) {{ hb.innerHTML='<div class="no-hist">No sessions recorded yet.</div>'; return; }}
  hb.innerHTML=`<table><thead><tr><th>#</th><th>Toko</th><th>Activity</th><th>Duration</th><th>Selesai</th></tr></thead><tbody>`
    +S.history.map((h,i)=>{{
      const num=S.history.length-i, loc=h.location;
      const locRow=loc&&!loc.error
        ?`<tr style="background:rgba(96,165,250,0.05)"><td></td><td colspan="4" style="font-size:0.7rem;color:var(--blue);padding:4px 12px 8px">
            📍 ${{loc.lat.toFixed(6)}}, ${{loc.lng.toFixed(6)}} <span style="color:var(--muted)">±${{loc.acc}}m</span>
            <a href="https://maps.google.com/?q=${{loc.lat}},${{loc.lng}}" target="_blank" style="margin-left:8px;color:var(--blue);font-size:0.68rem">🗺 Maps</a>
          </td></tr>`
        :loc?.error?`<tr><td></td><td colspan="4" style="font-size:0.7rem;color:var(--accent2);padding:4px 12px 8px">⚠️ ${{loc.error}}</td></tr>`:'';
      return `<tr>
        <td class="mono" style="color:var(--muted)">${{num}}</td>
        <td style="font-size:0.74rem">${{h.store||'—'}}</td>
        <td><span class="tag">${{h.icon}} ${{h.label}}</span></td>
        <td class="mono">${{fmtMs(h.duration)}}</td>
        <td style="color:var(--muted);font-size:0.74rem">${{fmtClock(new Date(h.endedAt).getTime())}}</td>
      </tr>${{locRow}}`;
    }}).join('')+`</tbody></table>`;
}}

// ═══════════════════════════════════════════════════
// UTILS
// ═══════════════════════════════════════════════════
function pad(n) {{ return String(n).padStart(2,'0'); }}
function fmtMs(ms) {{
  const t=Math.floor(Math.max(ms,0)/1000);
  return `${{pad(Math.floor(t/3600))}}:${{pad(Math.floor((t%3600)/60))}}:${{pad(t%60)}}`;
}}
function fmtClock(epoch) {{
  if(!epoch) return '—';
  return new Date(epoch).toLocaleTimeString([],{{hour:'2-digit',minute:'2-digit',second:'2-digit'}});
}}

document.addEventListener('visibilitychange',()=>{{
  if(!document.hidden){{
    try{{const r=localStorage.getItem(LS_KEY);if(r)S={{...defState(),...JSON.parse(r)}};}}catch(e){{}}
    if(S.running&&!rafId) timerTick();
    renderPage1(); restoreActStore(); renderStoreSubmitBanner();
  }}
}});

// ── INIT ──
if (S.spv&&S.region&&S.distributor) {{
  p0sel={{spv:S.spv,region:S.region,dist:S.distributor}};
  showActivePage(1); updateBadges(); renderPage1();
  if(S.activity) document.getElementById('actSelect').value=S.activity;
  restoreActStore(); renderStoreSubmitBanner();
  if(S.running) timerTick(); else renderTimer();
}}
</script>
</body>
</html>"""

components.html(APP_HTML, height=1150, scrolling=True)
