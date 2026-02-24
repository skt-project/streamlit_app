import os
import json
import logging
import sys
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Tuple

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery

# ============================================================
# CONFIGURATION
# ============================================================
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


class Config:
    BQ_PROJECT = "skintific-data-warehouse"
    BQ_DATASET = "gt_schema"
    BQ_TABLE = "sales_activity_records"
    CREDENTIALS_PATH = r"C:\Users\Bella Chelsea\Documents\skintific-data-warehouse-ea77119e2e7a.json"
    TZ_OFFSET = timedelta(hours=7)  # WIB = UTC+7


st.set_page_config(
    page_title="Sales Activity Timer",
    page_icon="⏱️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ────────────────────────────────────────────────
st.markdown("""
<style>
#MainMenu{visibility:hidden;}footer{visibility:hidden;}header{visibility:hidden;}
.block-container{padding-top:1rem!important;}

/* ── palette ── */
:root{
  --accent:#f5a623;--accent2:#e05c5c;--green:#4ade80;
  --blue:#60a5fa;--purple:#a78bfa;--teal:#2dd4bf;
  --muted:#6b7280;
}

/* ── nav tabs ── */
div[data-testid="stHorizontalBlock"] button{
  border-radius:9px!important;font-weight:600!important;
}

/* ── metric cards ── */
div[data-testid="metric-container"]{
  background:#12151c;border:1px solid rgba(255,255,255,0.08);
  border-radius:12px;padding:12px 16px;
}

/* ── tag badges ── */
.tag{
  display:inline-block;padding:3px 10px;border-radius:20px;
  font-size:0.75rem;font-weight:600;
  background:rgba(245,166,35,0.15);color:var(--accent);
}
.tag-green{background:rgba(74,222,128,0.15);color:var(--green);}
.tag-red  {background:rgba(224, 92, 92,0.15);color:var(--accent2);}
.tag-blue {background:rgba(96,165,250,0.15);color:var(--blue);}
.tag-purple{background:rgba(167,139,250,0.15);color:var(--purple);}

/* ── section divider ── */
.section-label{
  font-size:0.65rem;font-weight:700;letter-spacing:2px;
  text-transform:uppercase;color:var(--muted);margin-bottom:4px;
}
</style>
""", unsafe_allow_html=True)

# ============================================================
# BigQuery queries / DDL
# ============================================================
BQ_QUERY = """
    SELECT upper(spv_g2g) as spv,
    CASE WHEN region_g2g='' THEN upper(region) ELSE upper(region_g2g) END region,
    upper(distributor_g2g) as distributor, cust_id AS store_id, store_name
    FROM `gt_schema.master_store_database_basis`
    WHERE spv_g2g <> '' AND spv_g2g <> '-' AND distributor_g2g<>'-'
    ORDER BY spv, region, distributor, store_name
"""

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
# BigQuery CLIENT
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
        ("local JSON", _load_from_local_file),
        ("gcp_service_account", lambda: _load_from_streamlit_secrets("gcp_service_account")),
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
    except Exception as e:
        logger.error(f"DDL failed: {e}")


# ============================================================
# MASTER DATA
# ============================================================

@st.cache_data(ttl=3600)
def load_master_data() -> dict:
    client = get_bq_client()
    if client is None:
        return {"spv_list": [], "by_spv": {}, "error": "Cannot connect to BigQuery."}
    try:
        rows = list(client.query(BQ_QUERY).result())
    except Exception as e:
        return {"spv_list": [], "by_spv": {}, "error": str(e)}

    by_spv: dict = {}
    for row in rows:
        spv = (row.spv or "").strip()
        reg = (row.region or "").strip()
        dist = (row.distributor or "").strip()
        sid = str(row.store_id or "").strip()
        sname = (row.store_name or "").strip()
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
# BIGQUERY WRITE
# ============================================================

def _to_wib(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.astimezone(timezone(Config.TZ_OFFSET))


def _make_event_id(spv: str, activity_key: str, store_id: Optional[str], logged_at) -> str:
    ts_str = logged_at.isoformat() if isinstance(logged_at, datetime) else str(logged_at or "")
    raw = f"{spv}|{activity_key}|{store_id or ''}|{ts_str}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _serialize_row(row: dict) -> dict:
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}


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
        logger.warning(f"Dedup check failed: {e}")
        return set()


def write_p1_log(spv: str, region: str, distributor: str, p1_log: list) -> Tuple[bool, str]:
    """Write Check In/Out log entries to BigQuery."""
    client = get_bq_client()
    if client is None:
        return False, "No BQ client."
    if not p1_log:
        return False, "No data to save."

    created_at = datetime.now(timezone.utc)
    rows = []
    for entry in p1_log:
        logged_at = _to_wib(
            datetime.fromtimestamp(entry["time"] / 1000, tz=timezone.utc)
        )
        event_id = _make_event_id(spv, entry["key"], None, logged_at)
        rows.append({
            "event_id": event_id, "spv": spv, "region": region, "distributor": distributor,
            "activity_key": entry["key"], "activity_label": entry["label"],
            "logged_at": logged_at, "store_id": None, "store_name": None,
            "duration_seconds": None, "started_at": None, "ended_at": None,
            "latitude": None, "longitude": None, "location_accuracy_m": None,
            "created_at": created_at, "device_id": None,
        })

    return _insert_rows(client, rows)


def write_store_sessions(spv: str, region: str, distributor: str, sessions: list) -> Tuple[bool, str]:
    """Write store activity sessions to BigQuery."""
    client = get_bq_client()
    if client is None:
        return False, "No BQ client."
    if not sessions:
        return False, "No sessions to save."

    created_at = datetime.now(timezone.utc)
    rows = []
    for entry in sessions:
        duration_ms = entry.get("duration_ms", 0)
        duration_s = int(duration_ms / 1000)
        ended_at = _to_wib(datetime.fromisoformat(entry["ended_at"].replace("Z", "+00:00")))
        started_at = (ended_at - timedelta(seconds=duration_s)) if ended_at and duration_s else None
        store_id = str(entry.get("store_id", "")) or None
        event_id = _make_event_id(spv, entry["activity_key"], store_id, ended_at)
        rows.append({
            "event_id": event_id, "spv": spv, "region": region, "distributor": distributor,
            "activity_key": entry["activity_key"], "activity_label": entry["activity_label"],
            "logged_at": ended_at, "store_id": store_id, "store_name": entry.get("store_name"),
            "duration_seconds": duration_s, "started_at": started_at, "ended_at": ended_at,
            "latitude": entry.get("latitude"), "longitude": entry.get("longitude"),
            "location_accuracy_m": entry.get("location_accuracy_m"),
            "created_at": created_at, "device_id": None,
        })

    return _insert_rows(client, rows)


def _insert_rows(client: bigquery.Client, rows: list) -> Tuple[bool, str]:
    all_ids = [r["event_id"] for r in rows]
    existing = get_existing_event_ids(client, all_ids)
    new_rows = [r for r in rows if r["event_id"] not in existing]
    skipped = len(rows) - len(new_rows)

    if not new_rows:
        return True, f"ℹ️ {len(rows)} records already saved — no duplicates inserted."

    table_ref = f"{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}"
    try:
        errors = client.insert_rows_json(
            table_ref,
            [_serialize_row(r) for r in new_rows],
            row_ids=[r["event_id"] for r in new_rows],
        )
        if errors:
            return False, f"BQ insert errors: {errors}"
        msg = f"✅ {len(new_rows)} records saved to BigQuery."
        if skipped:
            msg += f" ({skipped} duplicates skipped)"
        return True, msg
    except Exception as e:
        logger.error(f"BQ insert exception: {e}")
        return False, str(e)


# ============================================================
# SESSION STATE INIT
# ============================================================

def init_state():
    defaults = {
        # Setup
        "page": "setup",          # setup | checkin | activities
        "spv": "",
        "region": "",
        "distributor": "",
        # Page 1 — Check In/Out
        "p1_log": [],              # list of {key, label, time_epoch_ms}
        # Page 2 — Stopwatch
        "current_activity_key": "",
        "current_activity_label": "",
        "timer_running": False,
        "timer_elapsed_ms": 0,
        "timer_started_at": None,  # datetime or None
        "selected_store_id": "",
        "selected_store_name": "",
        "sessions": [],            # completed sessions
        "submitted_stores": [],    # store_ids already submitted
        "totals": {},              # {activity_key: total_ms}
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ============================================================
# HELPERS
# ============================================================

ACTIVITIES = [
    {"key": "preparation",        "label": "Preparation",        "icon": "🗂️"},
    {"key": "greetings",          "label": "Greetings",          "icon": "👋"},
    {"key": "stock_check",        "label": "Stock Check",        "icon": "📦"},
    {"key": "merchandise",        "label": "Merchandise",        "icon": "🛍️"},
    {"key": "payment",            "label": "Payment",            "icon": "💳"},
    {"key": "sales_presentation", "label": "Sales Presentation", "icon": "📊"},
    {"key": "taking_order",       "label": "Taking Order",       "icon": "📝"},
]

P1_ACTIONS = {
    "dist_in":  {"label": "Check In Distributor",  "icon": "📥"},
    "dist_out": {"label": "Check Out Distributor", "icon": "📤"},
    "ish_in":   {"label": "Check In ISHOMA",       "icon": "🍽️"},
    "ish_out":  {"label": "Check Out ISHOMA",      "icon": "🍽️"},
}

ACTIVITY_MAP = {a["key"]: a for a in ACTIVITIES}


def fmt_ms(ms: int) -> str:
    s = max(int(ms / 1000), 0)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"


def fmt_clock(epoch_ms: int) -> str:
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone(Config.TZ_OFFSET))
    return dt.strftime("%H:%M:%S")


def has_checked_in() -> bool:
    return any(e["key"] == "dist_in" for e in st.session_state.p1_log)


def get_store_list() -> list:
    spv = st.session_state.spv
    region = st.session_state.region
    dist = st.session_state.distributor
    master = st.session_state.get("master", {})
    return (
        master.get("by_spv", {})
              .get(spv, {})
              .get("by_region", {})
              .get(region, {})
              .get("by_dist", {})
              .get(dist, [])
    )


def get_live_elapsed_ms() -> int:
    if st.session_state.timer_running and st.session_state.timer_started_at:
        delta = datetime.now(timezone.utc) - st.session_state.timer_started_at
        return st.session_state.timer_elapsed_ms + int(delta.total_seconds() * 1000)
    return st.session_state.timer_elapsed_ms


# ============================================================
# PAGES
# ============================================================

def render_setup():
    st.markdown("## ⏱ Sales Timer")
    st.markdown("### Selamat Datang")
    st.markdown("Pilih SPV, Region, dan Distributor sebelum memulai kunjungan.")
    st.divider()

    master = st.session_state.get("master", {})
    spv_list = master.get("spv_list", [])

    col1, col2 = st.columns(2)
    with col1:
        spv = st.selectbox("👤 Supervisor (SPV)", [""] + spv_list, key="sel_spv")

    region_list = []
    if spv:
        region_list = master.get("by_spv", {}).get(spv, {}).get("regions", [])
    with col2:
        region = st.selectbox("🗺️ Region", [""] + region_list, key="sel_region",
                              disabled=not spv)

    dist_list = []
    if spv and region:
        dist_list = (master.get("by_spv", {})
                           .get(spv, {})
                           .get("by_region", {})
                           .get(region, {})
                           .get("distributors", []))

    distributor = st.selectbox("🏭 Distributor", [""] + dist_list, key="sel_dist",
                               disabled=not region)

    st.markdown("")
    ready = bool(spv and region and distributor)
    if st.button("Mulai Kunjungan →", type="primary", disabled=not ready, use_container_width=True):
        st.session_state.spv = spv
        st.session_state.region = region
        st.session_state.distributor = distributor
        st.session_state.page = "checkin"
        st.rerun()


def render_checkin():
    # ── Top bar ──────────────────────────────────────────────
    col_title, col_badge = st.columns([2, 1])
    with col_title:
        st.markdown("## ⏱ Sales Timer")
        st.caption("Check In / Out & ISHOMA")
    with col_badge:
        st.info(f"**{st.session_state.spv}**  \n{st.session_state.distributor} · {st.session_state.region}")

    # ── Tab nav ──────────────────────────────────────────────
    tab_checkin, tab_activities = st.tabs(["📍 Check In/Out", "📋 Activities"])

    with tab_checkin:
        _render_p1_tab()

    with tab_activities:
        if not has_checked_in():
            st.warning("⚠️ Wajib Check In Distributor terlebih dahulu.")
        else:
            _render_p2_tab()

    # ── Back button ──────────────────────────────────────────
    st.divider()
    if st.button("← Kembali ke Halaman Awal"):
        # Stop timer if running
        if st.session_state.timer_running:
            st.session_state.timer_elapsed_ms = get_live_elapsed_ms()
            st.session_state.timer_running = False
            st.session_state.timer_started_at = None
        # Reset identity — keep logs so data isn't lost
        st.session_state.page = "setup"
        st.rerun()


def _render_p1_tab():
    st.markdown("#### 📋 Catat Aktivitas Check In/Out")

    act_options = {f"{v['icon']} {v['label']}": k for k, v in P1_ACTIONS.items()}
    chosen_label = st.selectbox("Pilih Aktivitas", ["— Pilih Aktivitas —"] + list(act_options.keys()),
                                key="p1_sel")

    if st.button("✅ Catat Waktu Sekarang", type="primary", use_container_width=True):
        if chosen_label == "— Pilih Aktivitas —":
            st.warning("Pilih aktivitas terlebih dahulu!")
        else:
            key = act_options[chosen_label]
            meta = P1_ACTIONS[key]
            entry = {
                "key": key,
                "label": meta["label"],
                "icon": meta["icon"],
                "time": int(datetime.now(timezone.utc).timestamp() * 1000),
            }
            st.session_state.p1_log.append(entry)
            st.rerun()

    st.divider()

    # ── Log table ────────────────────────────────────────────
    log = st.session_state.p1_log
    if not log:
        st.info("Belum ada aktivitas tercatat.")
    else:
        if not has_checked_in():
            st.warning("⚠️ Belum Check In Distributor.")
        for i, e in enumerate(log):
            col_n, col_act, col_time, col_del = st.columns([0.5, 3, 2, 0.7])
            col_n.caption(str(i + 1))
            col_act.markdown(f'<span class="tag">{e["icon"]} {e["label"]}</span>', unsafe_allow_html=True)
            col_time.code(fmt_clock(e["time"]))
            if col_del.button("🗑", key=f"del_p1_{i}", help="Hapus entri ini"):
                st.session_state.p1_log.pop(i)
                st.rerun()

    st.divider()

    # ── Save button ──────────────────────────────────────────
    col_save, col_clear = st.columns([3, 1])
    with col_save:
        if st.button("💾 Simpan Check In/Out ke BigQuery", type="primary", use_container_width=True):
            if not log:
                st.warning("Belum ada data Check In/Out untuk disimpan.")
            else:
                with st.spinner("Menyimpan ke BigQuery…"):
                    ok, msg = write_p1_log(
                        st.session_state.spv,
                        st.session_state.region,
                        st.session_state.distributor,
                        log,
                    )
                if ok:
                    st.success(msg)
                else:
                    st.error(f"❌ {msg}")

    with col_clear:
        if st.button("🗑 Clear", use_container_width=True):
            if st.session_state.get("confirm_clear_p1"):
                st.session_state.p1_log = []
                st.session_state.confirm_clear_p1 = False
                st.rerun()
            else:
                st.session_state.confirm_clear_p1 = True
                st.warning("Klik Clear lagi untuk konfirmasi.")


def _render_p2_tab():
    st.markdown("#### 🏪 Pilih Toko")

    stores = get_store_list()
    store_options = {f"{s['store_name']} ({s['store_id']})": s for s in stores}
    current_store_label = ""
    if st.session_state.selected_store_id:
        match = next((k for k, v in store_options.items()
                      if v["store_id"] == st.session_state.selected_store_id), "")
        current_store_label = match

    store_choice = st.selectbox(
        "Cari / Pilih Toko",
        ["— Pilih Toko —"] + list(store_options.keys()),
        index=(list(store_options.keys()).index(current_store_label) + 1
               if current_store_label else 0),
        key="p2_store_sel",
    )

    if store_choice != "— Pilih Toko —":
        chosen = store_options[store_choice]
        if chosen["store_id"] != st.session_state.selected_store_id:
            # Check for unsubmitted sessions before switching
            cur_id = st.session_state.selected_store_id
            unsubmitted = cur_id and cur_id not in st.session_state.submitted_stores and \
                          any(s["store_id"] == cur_id for s in st.session_state.sessions)
            if unsubmitted:
                st.warning(f'⚠️ Submit data toko "{st.session_state.selected_store_name}" terlebih dahulu.')
            else:
                st.session_state.selected_store_id = chosen["store_id"]
                st.session_state.selected_store_name = chosen["store_name"]
                st.rerun()

    st.divider()
    st.markdown("#### 🗂 Stopwatch Aktivitas")

    # ── Activity selector ─────────────────────────────────────
    act_options = ["— Pilih Aktivitas —"] + [f"{a['icon']} {a['label']}" for a in ACTIVITIES]
    act_labels_to_key = {f"{a['icon']} {a['label']}": a["key"] for a in ACTIVITIES}

    current_act_label = ""
    if st.session_state.current_activity_key:
        a = ACTIVITY_MAP.get(st.session_state.current_activity_key)
        if a:
            current_act_label = f"{a['icon']} {a['label']}"

    act_choice = st.selectbox(
        "Pilih Aktivitas",
        act_options,
        index=(act_options.index(current_act_label) if current_act_label in act_options else 0),
        key="p2_act_sel",
    )

    if act_choice != "— Pilih Aktivitas —":
        new_key = act_labels_to_key[act_choice]
        if new_key != st.session_state.current_activity_key:
            # Switching activity — pause first
            if st.session_state.timer_running:
                st.session_state.timer_elapsed_ms = get_live_elapsed_ms()
                st.session_state.timer_running = False
                st.session_state.timer_started_at = None
            st.session_state.current_activity_key = new_key
            a = ACTIVITY_MAP[new_key]
            st.session_state.current_activity_label = f"{a['icon']} {a['label']}"
            st.session_state.timer_elapsed_ms = 0
            st.rerun()

    # ── Clock display ─────────────────────────────────────────
    elapsed = get_live_elapsed_ms()
    act_name = st.session_state.current_activity_label or "— None Selected —"

    st.markdown(f"""
    <div style="background:#12151c;border:1px solid rgba(255,255,255,0.08);border-radius:14px;
                padding:24px;text-align:center;position:relative;overflow:hidden;">
      <div style="position:absolute;top:0;left:0;right:0;height:3px;
                  background:linear-gradient(90deg,#f5a623,#f97316,#e05c5c);"></div>
      <div style="font-size:0.68rem;font-weight:700;letter-spacing:2px;
                  text-transform:uppercase;color:#6b7280;margin-bottom:4px;">Current Activity</div>
      <div style="font-size:1.05rem;font-weight:600;color:#f5a623;margin-bottom:12px;">{act_name}</div>
      <div style="font-family:monospace;font-size:3.5rem;font-weight:700;
                  color:{'#4ade80' if st.session_state.timer_running else '#f5a623' if elapsed>0 else '#e8eaf0'};
                  letter-spacing:-2px;">{fmt_ms(elapsed)}</div>
      <div style="font-size:0.72rem;color:#6b7280;margin-top:8px;letter-spacing:1px;text-transform:uppercase;">
        {'🟢 Recording' if st.session_state.timer_running else '⏸ Paused' if elapsed>0 else '○ Idle'}
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Controls ──────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)

    with col1:
        start_disabled = st.session_state.timer_running or not st.session_state.current_activity_key
        if st.button("▶ Start", type="primary", disabled=start_disabled, use_container_width=True):
            if not st.session_state.selected_store_id:
                st.warning("Pilih toko terlebih dahulu!")
            else:
                st.session_state.timer_running = True
                st.session_state.timer_started_at = datetime.now(timezone.utc)
                st.rerun()

    with col2:
        if st.button("⏸ Pause", disabled=not st.session_state.timer_running, use_container_width=True):
            st.session_state.timer_elapsed_ms = get_live_elapsed_ms()
            st.session_state.timer_running = False
            st.session_state.timer_started_at = None
            st.rerun()

    with col3:
        if st.button("⏹ Stop & Save", type="secondary", use_container_width=True):
            _stop_and_record()

    # ── Auto-refresh while running ────────────────────────────
    if st.session_state.timer_running:
        import time
        time.sleep(1)
        st.rerun()

    # ── Totals ────────────────────────────────────────────────
    st.divider()
    st.markdown('<div class="section-label">📊 Total Waktu Per Aktivitas</div>', unsafe_allow_html=True)
    live_elapsed = get_live_elapsed_ms()
    cols = st.columns(4)
    for i, a in enumerate(ACTIVITIES):
        total = st.session_state.totals.get(a["key"], 0)
        if st.session_state.current_activity_key == a["key"]:
            total += live_elapsed
        with cols[i % 4]:
            st.metric(label=f"{a['icon']} {a['label']}", value=fmt_ms(total))

    # ── Session log ───────────────────────────────────────────
    st.divider()
    st.markdown('<div class="section-label">📜 Session Log</div>', unsafe_allow_html=True)
    sessions = st.session_state.sessions

    if not sessions:
        st.info("No sessions recorded yet.")
    else:
        for i, s in enumerate(sessions):
            tag_color = "tag-green" if s["store_id"] in st.session_state.submitted_stores else "tag"
            loc_text = ""
            if s.get("latitude") and s.get("longitude"):
                loc_text = f"📍 {s['latitude']:.6f}, {s['longitude']:.6f} ±{s.get('location_accuracy_m',0)}m"

            col_n, col_store, col_act, col_dur, col_time = st.columns([0.5, 2.5, 2, 1.5, 1.5])
            col_n.caption(str(len(sessions) - i))
            col_store.markdown(f'<span class="{tag_color}">{s["store_name"]}</span>', unsafe_allow_html=True)
            col_act.markdown(f'<span class="tag">{s["activity_label"]}</span>', unsafe_allow_html=True)
            col_dur.code(fmt_ms(s["duration_ms"]))
            ended_epoch = int(datetime.fromisoformat(s["ended_at"]).timestamp() * 1000)
            col_time.caption(fmt_clock(ended_epoch))
            if loc_text:
                st.caption(loc_text)

    # ── Store submit ──────────────────────────────────────────
    store_id = st.session_state.selected_store_id
    if store_id:
        store_sessions = [s for s in sessions if s["store_id"] == store_id]
        if store_sessions:
            st.divider()
            already = store_id in st.session_state.submitted_stores
            status = f"✔ Sudah disubmit ({len(store_sessions)} sesi). Submit ulang jika ada tambahan." \
                if already else f"Terdapat **{len(store_sessions)} sesi**. Klik tombol untuk mengirim ke BigQuery."
            st.markdown(status)

            btn_label = "🔄 Submit Ulang" if already else "✅ Submit Toko Ini"
            if st.button(btn_label, type="primary", use_container_width=True, key="store_submit_btn"):
                with st.spinner("Menyimpan ke BigQuery…"):
                    ok, msg = write_store_sessions(
                        st.session_state.spv,
                        st.session_state.region,
                        st.session_state.distributor,
                        store_sessions,
                    )
                if ok:
                    st.success(msg)
                    if store_id not in st.session_state.submitted_stores:
                        st.session_state.submitted_stores.append(store_id)
                    st.rerun()
                else:
                    st.error(f"❌ {msg}")

    # ── Clear all ─────────────────────────────────────────────
    st.divider()
    if st.button("🗑 Clear All Sessions", use_container_width=True):
        if st.session_state.get("confirm_clear_sessions"):
            st.session_state.sessions = []
            st.session_state.totals = {}
            st.session_state.submitted_stores = []
            st.session_state.timer_elapsed_ms = 0
            st.session_state.timer_running = False
            st.session_state.timer_started_at = None
            st.session_state.current_activity_key = ""
            st.session_state.current_activity_label = ""
            st.session_state.confirm_clear_sessions = False
            st.rerun()
        else:
            st.session_state.confirm_clear_sessions = True
            st.warning("Klik Clear lagi untuk konfirmasi.")


def _stop_and_record():
    elapsed = get_live_elapsed_ms()
    if elapsed <= 0 or not st.session_state.current_activity_key:
        # Just reset
        st.session_state.timer_elapsed_ms = 0
        st.session_state.timer_running = False
        st.session_state.timer_started_at = None
        st.session_state.current_activity_key = ""
        st.session_state.current_activity_label = ""
        st.rerun()
        return

    key = st.session_state.current_activity_key
    a = ACTIVITY_MAP.get(key, {})
    ended_at = datetime.now(timezone.utc).isoformat()

    session_entry = {
        "activity_key": key,
        "activity_label": st.session_state.current_activity_label,
        "store_id": st.session_state.selected_store_id,
        "store_name": st.session_state.selected_store_name or "—",
        "duration_ms": elapsed,
        "ended_at": ended_at,
        "latitude": None,
        "longitude": None,
        "location_accuracy_m": None,
    }

    # Update totals
    st.session_state.totals[key] = st.session_state.totals.get(key, 0) + elapsed

    # Reset timer
    st.session_state.timer_elapsed_ms = 0
    st.session_state.timer_running = False
    st.session_state.timer_started_at = None
    st.session_state.current_activity_key = ""
    st.session_state.current_activity_label = ""

    st.session_state.sessions.insert(0, session_entry)
    st.rerun()


# ============================================================
# MAIN
# ============================================================

def main():
    init_state()

    # Load master data once
    if "master" not in st.session_state:
        with st.spinner("⏳ Memuat data dari BigQuery…"):
            st.session_state.master = load_master_data()
        client = get_bq_client()
        if client:
            ensure_table(client)

    master = st.session_state.master
    if "error" in master and master.get("spv_list") == []:
        st.error(f"⚠️ BigQuery error: {master['error']}")
    elif not master.get("spv_list"):
        st.warning("⚠️ Data kosong. Periksa query / kredensial BigQuery.")

    page = st.session_state.page
    if page == "setup":
        render_setup()
    else:
        render_checkin()


if __name__ == "__main__":
    main()
