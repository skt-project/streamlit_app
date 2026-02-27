import os
import logging
import sys
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Tuple

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from streamlit_js_eval import get_geolocation

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


class Config:
    BQ_PROJECT = "skintific-data-warehouse"
    BQ_DATASET = "gt_schema"
    BQ_TABLE = "gt_salesman_time_motion"
    CREDENTIALS_PATH = r"C:\Users\Bella Chelsea\Documents\skintific-data-warehouse-ea77119e2e7a.json"
    TZ_OFFSET = timedelta(hours=7)


st.set_page_config(page_title="Sales Activity Timer", page_icon="⏱️",
                   layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
#MainMenu{visibility:hidden;}footer{visibility:hidden;}header{visibility:hidden;}
.block-container{padding-top:1rem!important;}
:root{--accent:#f5a623;--accent2:#e05c5c;--green:#4ade80;
      --blue:#60a5fa;--purple:#a78bfa;--teal:#2dd4bf;--muted:#6b7280;}
div[data-testid="metric-container"]{
  background:#12151c;border:1px solid rgba(255,255,255,0.08);
  border-radius:12px;padding:12px 16px;}
.tag{display:inline-block;padding:3px 10px;border-radius:20px;font-size:0.75rem;
     font-weight:600;background:rgba(245,166,35,0.15);color:var(--accent);}
.geo-ok{display:inline-block;padding:3px 10px;border-radius:20px;font-size:0.75rem;
        font-weight:600;background:rgba(45,212,191,0.12);color:#2dd4bf;
        border:1px solid rgba(45,212,191,0.3);}
.geo-warn{display:inline-block;padding:3px 10px;border-radius:20px;font-size:0.75rem;
          font-weight:600;background:rgba(245,166,35,0.12);color:#f5a623;
          border:1px solid rgba(245,166,35,0.3);}
.section-label{font-size:0.65rem;font-weight:700;letter-spacing:2px;
               text-transform:uppercase;color:var(--muted);margin-bottom:4px;}
</style>
""", unsafe_allow_html=True)

# ============================================================
# BigQuery
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
    event_id STRING NOT NULL, spv STRING NOT NULL, region STRING NOT NULL,
    distributor STRING NOT NULL, activity_key STRING NOT NULL,
    activity_label STRING NOT NULL, logged_at TIMESTAMP NOT NULL,
    store_id STRING, store_name STRING, duration_seconds INT64,
    started_at TIMESTAMP, ended_at TIMESTAMP,
    latitude FLOAT64, longitude FLOAT64, location_accuracy_m INT64,
    created_at TIMESTAMP, device_id STRING
)
PARTITION BY DATE(logged_at)
CLUSTER BY spv, distributor, store_id
"""


def _load_secrets(key):
    d = dict(st.secrets["connections"]["bigquery"] if key == "connections" else st.secrets[key])
    if "private_key" in d:
        d["private_key"] = d["private_key"].replace("\\n", "\n")
    return service_account.Credentials.from_service_account_info(d), \
           d.get("project_id") or d.get("project") or Config.BQ_PROJECT


def _load_local():
    if not os.path.exists(Config.CREDENTIALS_PATH):
        raise FileNotFoundError(Config.CREDENTIALS_PATH)
    return service_account.Credentials.from_service_account_file(Config.CREDENTIALS_PATH), Config.BQ_PROJECT


@st.cache_resource
def get_bq_client():
    for name, fn in [("local", _load_local),
                     ("gcp_service_account", lambda: _load_secrets("gcp_service_account")),
                     ("connections", lambda: _load_secrets("connections"))]:
        try:
            creds, proj = fn()
            c = bigquery.Client(credentials=creds, project=proj)
            logger.info(f"BQ OK via {name}")
            return c
        except Exception as e:
            logger.warning(f"{name}: {e}")
    try:
        return bigquery.Client()
    except Exception as e:
        logger.error(f"ADC: {e}")
        return None


def ensure_table(client):
    try:
        client.query(BQ_DDL).result()
    except Exception as e:
        logger.error(f"DDL: {e}")


@st.cache_data(ttl=3600)
def load_master_data():
    client = get_bq_client()
    if not client:
        return {"spv_list": [], "by_spv": {}, "error": "Cannot connect to BigQuery."}
    try:
        rows = list(client.query(BQ_QUERY).result())
    except Exception as e:
        return {"spv_list": [], "by_spv": {}, "error": str(e)}
    by_spv = {}
    for row in rows:
        spv = (row.spv or "").strip(); reg = (row.region or "").strip()
        dist = (row.distributor or "").strip(); sid = str(row.store_id or "").strip()
        sname = (row.store_name or "").strip()
        if not spv or not reg or not dist:
            continue
        bs = by_spv.setdefault(spv, {"regions": [], "by_region": {}})
        if reg not in bs["regions"]: bs["regions"].append(reg)
        br = bs["by_region"].setdefault(reg, {"distributors": [], "by_dist": {}})
        if dist not in br["distributors"]: br["distributors"].append(dist)
        br["by_dist"].setdefault(dist, [])
        if sid not in {s["store_id"] for s in br["by_dist"][dist]}:
            br["by_dist"][dist].append({"store_id": sid, "store_name": sname})
    return {"spv_list": sorted(by_spv.keys()), "by_spv": by_spv}


# ============================================================
# BQ writes
# ============================================================
def _to_wib(dt):
    return dt.astimezone(timezone(Config.TZ_OFFSET)) if dt else None

def _eid(spv, key, store_id, ts):
    t = ts.isoformat() if isinstance(ts, datetime) else str(ts or "")
    return hashlib.sha256(f"{spv}|{key}|{store_id or ''}|{t}".encode()).hexdigest()

def _ser(row):
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}

def _dedup(client, ids):
    if not ids: return set()
    ql = ", ".join(f"'{i}'" for i in ids)
    try:
        return {r.event_id for r in client.query(
            f"SELECT event_id FROM `{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}` "
            f"WHERE event_id IN ({ql})").result()}
    except Exception as e:
        logger.warning(f"dedup: {e}"); return set()

def _insert(client, rows):
    existing = _dedup(client, [r["event_id"] for r in rows])
    new = [r for r in rows if r["event_id"] not in existing]
    skipped = len(rows) - len(new)
    if not new:
        return True, f"ℹ️ {len(rows)} record sudah tersimpan."
    try:
        errs = client.insert_rows_json(
            f"{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}",
            [_ser(r) for r in new], row_ids=[r["event_id"] for r in new])
        if errs: return False, str(errs)
        msg = f"✅ {len(new)} record tersimpan."
        if skipped: msg += f" ({skipped} duplikat dilewati)"
        return True, msg
    except Exception as e:
        return False, str(e)

def write_p1(spv, region, dist, entry, lat=None, lng=None, acc=None):
    c = get_bq_client()
    if not c: return False, "No BQ client."
    logged_at = _to_wib(datetime.fromtimestamp(entry["time"] / 1000, tz=timezone.utc))
    row = {"event_id": _eid(spv, entry["key"], None, logged_at),
           "spv": spv, "region": region, "distributor": dist,
           "activity_key": entry["key"], "activity_label": entry["label"],
           "logged_at": logged_at, "store_id": None, "store_name": None,
           "duration_seconds": None, "started_at": None, "ended_at": None,
           "latitude": lat, "longitude": lng, "location_accuracy_m": acc,
           "created_at": datetime.now(timezone.utc), "device_id": None}
    return _insert(c, [row])

def write_session(spv, region, dist, entry):
    c = get_bq_client()
    if not c: return False, "No BQ client."
    dur_s = int(entry.get("duration_ms", 0) / 1000)
    ended = _to_wib(datetime.fromisoformat(entry["ended_at"].replace("Z", "+00:00")))
    started = (ended - timedelta(seconds=dur_s)) if ended and dur_s else None
    sid = str(entry.get("store_id", "")) or None
    row = {"event_id": _eid(spv, entry["activity_key"], sid, ended),
           "spv": spv, "region": region, "distributor": dist,
           "activity_key": entry["activity_key"], "activity_label": entry["activity_label"],
           "logged_at": ended, "store_id": sid, "store_name": entry.get("store_name"),
           "duration_seconds": dur_s, "started_at": started, "ended_at": ended,
           "latitude": entry.get("latitude"), "longitude": entry.get("longitude"),
           "location_accuracy_m": entry.get("location_accuracy_m"),
           "created_at": datetime.now(timezone.utc), "device_id": None}
    return _insert(c, [row])


# ============================================================
# Constants
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


def init_state():
    defaults = {
        "page": "setup", "spv": "", "region": "", "distributor": "",
        "act_key": "", "act_label": "",
        "timer_running": False, "timer_elapsed_ms": 0, "timer_started_at": None,
        "store_id": "", "store_name": "",
        "store_geo_done": set(),
        "totals": {},
        # GPS action queues
        "do_dist_in_write": False,
        "pending_dist_in": None,
        "do_store_write": False,
        "pending_store_session": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def fmt_ms(ms):
    s = max(int(ms / 1000), 0)
    h, r = divmod(s, 3600); m, sec = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"

def get_live_ms():
    if st.session_state.timer_running and st.session_state.timer_started_at:
        d = datetime.now(timezone.utc) - st.session_state.timer_started_at
        return st.session_state.timer_elapsed_ms + int(d.total_seconds() * 1000)
    return st.session_state.timer_elapsed_ms

def get_stores():
    m = st.session_state.get("master", {})
    return (m.get("by_spv", {}).get(st.session_state.spv, {})
             .get("by_region", {}).get(st.session_state.region, {})
             .get("by_dist", {}).get(st.session_state.distributor, []))

def _extract_coords(loc):
    try:
        if loc and "coords" in loc:
            c = loc["coords"]
            lat = c.get("latitude"); lng = c.get("longitude"); acc = c.get("accuracy")
            if lat is not None and lng is not None:
                return float(lat), float(lng), int(acc) if acc is not None else None
    except Exception:
        pass
    return None, None, None


# ============================================================
# MAIN
# ============================================================
def main():
    init_state()

    need_geo = st.session_state.do_dist_in_write or st.session_state.do_store_write

    loc = get_geolocation()

    if need_geo and loc is None:
        st.info("📡 Mengambil koordinat GPS… pastikan izin lokasi diaktifkan di browser.")
        import time; time.sleep(0.5); st.rerun()

    lat, lng, acc = _extract_coords(loc) if (need_geo and loc is not None) else (None, None, None)

    if st.session_state.do_dist_in_write and loc is not None:
        entry = st.session_state.pending_dist_in
        st.session_state.do_dist_in_write = False
        st.session_state.pending_dist_in  = None

        ok, msg = write_p1(st.session_state.spv, st.session_state.region,
                           st.session_state.distributor, entry,
                           lat=lat, lng=lng, acc=acc)
        if ok:
            geo_str = f"📍 {lat:.5f}, {lng:.5f} ±{acc}m" if lat else "📍 lokasi tidak tersedia"
            st.session_state._flash = ("success", f"📥 **Check In Distributor** disimpan · {geo_str}")
        else:
            st.session_state._flash = ("error", f"❌ Gagal: {msg}")

    if st.session_state.do_store_write and loc is not None:
        entry = st.session_state.pending_store_session
        st.session_state.do_store_write        = False
        st.session_state.pending_store_session = None

        entry["latitude"] = lat; entry["longitude"] = lng; entry["location_accuracy_m"] = acc
        sid = entry.get("store_id", "")
        if sid: st.session_state.store_geo_done.add(sid)

        ok, msg = write_session(st.session_state.spv, st.session_state.region,
                                st.session_state.distributor, entry)
        if ok:
            geo_str = f"📍 {lat:.5f}, {lng:.5f} ±{acc}m" if lat else "📍 lokasi tidak tersedia"
            st.session_state._flash = ("success",
                f"✅ **{entry['activity_label']}** — {fmt_ms(entry['duration_ms'])} · {geo_str}")
        else:
            st.session_state._flash = ("error", f"❌ Gagal: {msg}")


    if "_flash" in st.session_state:
        kind, text = st.session_state.pop("_flash")
        (st.success if kind == "success" else st.error)(text)

    if "master" not in st.session_state:
        with st.spinner("⏳ Memuat data dari BigQuery…"):
            st.session_state.master = load_master_data()
        c = get_bq_client()
        if c: ensure_table(c)

    master = st.session_state.master
    if "error" in master and not master.get("spv_list"):
        st.error(f"⚠️ BigQuery error: {master['error']}")
    elif not master.get("spv_list"):
        st.warning("⚠️ Data kosong.")

    if st.session_state.page == "setup":
        render_setup()
    else:
        render_checkin()


# ============================================================
# Pages
# ============================================================
def render_setup():
    st.markdown("## ⏱ Time Motion")
    st.markdown("Pilih SPV, Region, dan Distributor sebelum memulai kunjungan.")
    st.divider()

    master = st.session_state.get("master", {})
    spv_list = master.get("spv_list", [])

    c1, c2 = st.columns(2)
    with c1:
        spv = st.selectbox("👤 Supervisor (SPV)", [""] + spv_list, key="sel_spv")
    region_list = master.get("by_spv", {}).get(spv, {}).get("regions", []) if spv else []
    with c2:
        region = st.selectbox("🗺️ Region", [""] + region_list, key="sel_region", disabled=not spv)
    dist_list = (master.get("by_spv", {}).get(spv, {}).get("by_region", {})
                       .get(region, {}).get("distributors", [])) if spv and region else []
    dist = st.selectbox("🏭 Distributor", [""] + dist_list, key="sel_dist", disabled=not region)

    st.markdown("")
    if st.button("Mulai Kunjungan →", type="primary",
                 disabled=not (spv and region and dist), use_container_width=True):
        st.session_state.spv = spv; st.session_state.region = region
        st.session_state.distributor = dist; st.session_state.page = "checkin"
        st.rerun()


def render_checkin():
    c1, c2 = st.columns([2, 1])
    with c1:
        st.markdown("## ⏱ Time Motion")
    with c2:
        st.info(f"**{st.session_state.spv}**  \n{st.session_state.distributor} · {st.session_state.region}")

    tab1, tab2 = st.tabs(["📍 Check In/Out", "📋 Activities"])
    with tab1: _render_p1_tab()
    with tab2: _render_p2_tab()

    st.divider()
    if st.button("← Kembali ke Halaman Awal"):
        if st.session_state.timer_running:
            st.session_state.timer_elapsed_ms = get_live_ms()
            st.session_state.timer_running = False; st.session_state.timer_started_at = None
        st.session_state.page = "setup"; st.rerun()


def _render_p1_tab():
    st.markdown("#### 📋 Catat Aktivitas Check In/Out")

    act_opts = {f"{v['icon']} {v['label']}": k for k, v in P1_ACTIONS.items()}
    chosen   = st.selectbox("Pilih Aktivitas", ["— Pilih Aktivitas —"] + list(act_opts.keys()), key="p1_sel")

    if st.session_state.do_dist_in_write:
        st.info("📡 Mengambil koordinat GPS… tunggu sebentar.")

    if st.button("✅ Catat Waktu", type="primary", use_container_width=True):
        if chosen == "— Pilih Aktivitas —":
            st.warning("Pilih aktivitas terlebih dahulu!")
        else:
            key  = act_opts[chosen]
            meta = P1_ACTIONS[key]
            entry = {"key": key, "label": meta["label"], "icon": meta["icon"],
                     "time": int(datetime.now(timezone.utc).timestamp() * 1000)}

            if key == "dist_in":
                st.session_state.pending_dist_in  = entry
                st.session_state.do_dist_in_write = True
                st.rerun()
            else:
                with st.spinner("Menyimpan…"):
                    ok, msg = write_p1(st.session_state.spv, st.session_state.region,
                                       st.session_state.distributor, entry)
                if ok:
                    st.success(f"{meta['icon']} **{meta['label']}** — {msg}")
                else:
                    st.error(f"❌ {msg}")


def _render_p2_tab():
    st.markdown("#### 🏪 Pilih Toko")
    stores     = get_stores()
    store_opts = {f"{s['store_name']} ({s['store_id']})": s for s in stores}
    cur_label  = next((k for k, v in store_opts.items()
                       if v["store_id"] == st.session_state.store_id), "")

    choice = st.selectbox("Cari / Pilih Toko",
                          ["— Pilih Toko —"] + list(store_opts.keys()),
                          index=(list(store_opts.keys()).index(cur_label) + 1 if cur_label else 0),
                          key="p2_store_sel")
    if choice != "— Pilih Toko —":
        s = store_opts[choice]
        if s["store_id"] != st.session_state.store_id:
            st.session_state.store_id = s["store_id"]
            st.session_state.store_name = s["store_name"]

    if st.session_state.store_id:
        col_geo, col_reset = st.columns([3, 1])
        with col_geo:
            if st.session_state.store_id in st.session_state.store_geo_done:
                st.markdown('<span class="geo-ok">📍 Lokasi toko sudah terekam</span>', unsafe_allow_html=True)
            else:
                st.markdown('<span class="geo-warn">📍 GPS direkam pada aktivitas pertama</span>', unsafe_allow_html=True)
        with col_reset:
            if st.button("🔄 Reset Data", use_container_width=True,
                         help="Reset timer, aktivitas, dan total waktu untuk toko ini"):
                if st.session_state.timer_running:
                    st.session_state.timer_running = False
                    st.session_state.timer_started_at = None
                st.session_state.timer_elapsed_ms = 0
                st.session_state.act_key = ""
                st.session_state.act_label = ""
                st.session_state.totals = {}
                st.session_state.store_geo_done.discard(st.session_state.store_id)
                st.success("✅ Data toko berhasil direset.")

    st.divider()
    st.markdown("#### 🗂 Stopwatch Aktivitas")

    act_list    = ["— Pilih Aktivitas —"] + [f"{a['icon']} {a['label']}" for a in ACTIVITIES]
    act_key_map = {f"{a['icon']} {a['label']}": a["key"] for a in ACTIVITIES}
    cur_act     = ""
    if st.session_state.act_key:
        a = ACTIVITY_MAP.get(st.session_state.act_key)
        if a: cur_act = f"{a['icon']} {a['label']}"

    act_choice = st.selectbox("Pilih Aktivitas", act_list,
                              index=(act_list.index(cur_act) if cur_act in act_list else 0),
                              key="p2_act_sel")
    if act_choice != "— Pilih Aktivitas —":
        nk = act_key_map[act_choice]
        if nk != st.session_state.act_key:
            if st.session_state.timer_running:
                st.session_state.timer_elapsed_ms = get_live_ms()
                st.session_state.timer_running = False; st.session_state.timer_started_at = None
            st.session_state.act_key = nk
            a = ACTIVITY_MAP[nk]; st.session_state.act_label = f"{a['icon']} {a['label']}"
            st.session_state.timer_elapsed_ms = 0

    elapsed  = get_live_ms()
    act_name = st.session_state.act_label or "— None Selected —"
    clr = "#4ade80" if st.session_state.timer_running else ("#f5a623" if elapsed > 0 else "#e8eaf0")
    status = "🟢 Recording" if st.session_state.timer_running else ("🟡 Ready" if elapsed > 0 else "○ Idle")

    st.markdown(f"""
    <div style="background:#12151c;border:1px solid rgba(255,255,255,0.08);border-radius:14px;
                padding:24px;text-align:center;position:relative;overflow:hidden;">
      <div style="position:absolute;top:0;left:0;right:0;height:3px;
                  background:linear-gradient(90deg,#f5a623,#f97316,#e05c5c);"></div>
      <div style="font-size:0.68rem;font-weight:700;letter-spacing:2px;
                  text-transform:uppercase;color:#6b7280;margin-bottom:4px;">Current Activity</div>
      <div style="font-size:1.05rem;font-weight:600;color:#f5a623;margin-bottom:12px;">{act_name}</div>
      <div style="font-family:monospace;font-size:3.5rem;font-weight:700;
                  color:{clr};letter-spacing:-2px;">{fmt_ms(elapsed)}</div>
      <div style="font-size:0.72rem;color:#6b7280;margin-top:8px;letter-spacing:1px;
                  text-transform:uppercase;">{status}</div>
    </div>
    """, unsafe_allow_html=True)

    if st.session_state.do_store_write:
        st.info("📡 Mengambil koordinat GPS toko… tunggu sebentar.")

    c1, c2 = st.columns(2)

    with c1:
        if st.button("▶ Start", type="primary",
                     disabled=st.session_state.timer_running or not st.session_state.act_key,
                     use_container_width=True):
            if not st.session_state.store_id:
                st.warning("Pilih toko terlebih dahulu!")
            else:
                st.session_state.timer_running = True
                st.session_state.timer_started_at = datetime.now(timezone.utc)
                st.rerun()

    with c2:
        if st.button("⏹ Stop & Save", type="secondary", use_container_width=True):
            _do_stop()

    if st.session_state.timer_running and not st.session_state.do_store_write:
        import time; time.sleep(1); st.rerun()

    st.divider()
    st.markdown('<div class="section-label">📊 Total Waktu Per Aktivitas</div>', unsafe_allow_html=True)
    live = get_live_ms()
    cols = st.columns(4)
    for i, a in enumerate(ACTIVITIES):
        total = st.session_state.totals.get(a["key"], 0)
        if st.session_state.act_key == a["key"]: total += live
        with cols[i % 4]:
            st.metric(f"{a['icon']} {a['label']}", fmt_ms(total))


def _do_stop():
    elapsed  = get_live_ms()
    store_id = st.session_state.store_id
    if elapsed <= 0 or not st.session_state.act_key:
        st.session_state.timer_elapsed_ms = 0; st.session_state.timer_running = False
        st.session_state.timer_started_at = None
        st.session_state.act_key = ""; st.session_state.act_label = ""; st.rerun(); return

    entry = {"activity_key": st.session_state.act_key, "activity_label": st.session_state.act_label,
             "store_id": store_id, "store_name": st.session_state.store_name or "—",
             "duration_ms": elapsed, "ended_at": datetime.now(timezone.utc).isoformat(),
             "latitude": None, "longitude": None, "location_accuracy_m": None}

    st.session_state.totals[st.session_state.act_key] = (
        st.session_state.totals.get(st.session_state.act_key, 0) + elapsed)
    st.session_state.timer_elapsed_ms = 0; st.session_state.timer_running = False
    st.session_state.timer_started_at = None
    st.session_state.act_key = ""; st.session_state.act_label = ""

    if store_id and store_id not in st.session_state.store_geo_done:
        st.session_state.pending_store_session = entry
        st.session_state.do_store_write        = True
    else:
        with st.spinner("Menyimpan…"):
            ok, msg = write_session(st.session_state.spv, st.session_state.region,
                                    st.session_state.distributor, entry)
        if ok:
            st.success(f"✅ **{entry['activity_label']}** — {fmt_ms(elapsed)} — {msg}")
        else:
            st.error(f"❌ {msg}")
        st.rerun()


if __name__ == "__main__":
    main()
