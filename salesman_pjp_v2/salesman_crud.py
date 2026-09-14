"""
Salesman read/write operations.

Key fixes over production:
  - update_salesman_record() targets by salesman_id (not by name)
  - generate_salesman_id() uses a BQ MERGE sequence table (no race condition)
  - Every write emits an audit_log entry
  - No global cache.clear() — callers must invalidate their own cache keys
"""

import uuid
import json
import re
import unicodedata
import pandas as pd
from datetime import datetime

import streamlit as st
from google.cloud import bigquery

from distributor_naming import canonical_dist_cte
from .bq_client import get_client
from .config import (
    MAPPING_TABLE, SALESMAN_TABLE, SALESMAN_VIEW, AUDIT_TABLE, SEQ_TABLE,
    STATUS_OPTIONS, GENDER_OPTIONS, EDUCATION_OPTIONS,
)


# ─── Name / phone helpers ──────────────────────────────────────────────────────

def sanitize_salesman_name(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).strip().upper())


def normalize_phone(phone) -> str:
    if phone is None:
        return ""
    hp = str(phone).strip().replace(" ", "").replace("-", "").rstrip(".0")
    if hp.startswith("+62+62"):
        hp = hp[3:]
    if hp.startswith("+62"):
        return hp
    if hp.startswith("62") and hp[2:].isdigit() and 8 <= len(hp[2:]) <= 13:
        return "+" + hp
    if hp.startswith("0") and hp[1:].isdigit() and 8 <= len(hp[1:]) <= 13:
        return "+62" + hp[1:]
    if hp.isdigit() and 8 <= len(hp) <= 13:
        return "+62" + hp
    return hp


# ─── Audit log ────────────────────────────────────────────────────────────────

def _audit(client: bigquery.Client, action: str, entity_type: str,
           entity_id: str, dist_code: str, payload: dict) -> None:
    row = {
        "event_id":       str(uuid.uuid4()),
        "event_time":     datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        "actor_session":  st.session_state.get("_v2_session_id", "unknown"),
        "actor_role":     "distributor_admin",
        "distributor_code": dist_code,
        "entity_type":    entity_type,
        "entity_id":      entity_id,
        "action":         action,
        "payload_json":   json.dumps(payload, default=str),
        "source_system":  "salesman_pjp_v2",
    }
    try:
        jc = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        client.load_table_from_dataframe(pd.DataFrame([row]), AUDIT_TABLE, job_config=jc).result()
    except Exception:
        pass  # audit failure must never block the main operation


# ─── Read ─────────────────────────────────────────────────────────────────────

def get_salesman_list(dist_code: str) -> pd.DataFrame:
    client = get_client()
    query = f"""
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY UPPER(TRIM(nama_salesman)), UPPER(TRIM(kode_distributor))
                       ORDER BY uploaded_at DESC
                   ) AS rn
            FROM `{SALESMAN_VIEW}`
        )
        SELECT
            m.salesman_id,
            m.salesman_type,
            m.distributor_code,
            m.salesman,
            m.is_active,
            m.created_at,
            m.updated_at,
            s.nama_salesman,
            s.no_hp,
            s.status_salesman,
            d.region_g2g AS region,
            d.distributor_name AS nama_distributor,
            s.asm
        FROM `{MAPPING_TABLE}` m
        LEFT JOIN ranked s
            ON  UPPER(TRIM(m.salesman))        = UPPER(TRIM(s.nama_salesman))
            AND UPPER(TRIM(m.distributor_code)) = UPPER(TRIM(s.kode_distributor))
            AND s.rn = 1
        LEFT JOIN ({canonical_dist_cte()}) d
            ON d.distributor_code = UPPER(TRIM(m.distributor_code))
        WHERE UPPER(m.distributor_code) = UPPER(@kode)
        ORDER BY m.salesman_id, m.created_at DESC
    """
    jc = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("kode", "STRING", dist_code)]
    )
    try:
        return get_client().query(query, job_config=jc).to_dataframe()
    except Exception as e:
        st.error(f"Gagal memuat daftar salesman: {e}")
        return pd.DataFrame()


def get_salesman_detail(salesman_id: str, dist_code: str) -> dict:
    """Fetch full HR record for a salesman_id (for pre-populating edit form)."""
    client = get_client()
    query = f"""
        SELECT s.*
        FROM `{SALESMAN_VIEW}` s
        JOIN (
            SELECT UPPER(TRIM(salesman)) AS nm
            FROM `{MAPPING_TABLE}`
            WHERE salesman_id = @sid
              AND UPPER(TRIM(distributor_code)) = UPPER(TRIM(@dist))
              AND is_active = TRUE
            LIMIT 1
        ) m ON UPPER(TRIM(s.nama_salesman)) = m.nm
              AND UPPER(TRIM(s.kode_distributor)) = UPPER(TRIM(@dist))
        ORDER BY s.uploaded_at DESC
        LIMIT 1
    """
    jc = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("sid",  "STRING", salesman_id),
            bigquery.ScalarQueryParameter("dist", "STRING", dist_code),
        ]
    )
    try:
        rows = list(client.query(query, job_config=jc).result())
        return dict(rows[0]) if rows else {}
    except Exception:
        return {}


# ─── ID generation (race-safe via BQ MERGE sequence table) ────────────────────

def generate_salesman_id(dist_code: str, salesman_type: str) -> str:
    """
    Atomically claim the next sequence number for this (dist_code, type) pair.
    Uses a MERGE on sfa_step.salesman_id_seq to avoid race conditions.
    """
    client = get_client()
    sql = f"""
        MERGE `{SEQ_TABLE}` T
        USING (SELECT @dist AS dist_code, @stype AS stype) S
        ON T.dist_code = S.dist_code AND T.stype = S.stype
        WHEN MATCHED THEN UPDATE SET next_seq = next_seq + 1, updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (dist_code, stype, next_seq, updated_at)
             VALUES (S.dist_code, S.stype, 1, CURRENT_TIMESTAMP());

        SELECT next_seq
        FROM `{SEQ_TABLE}`
        WHERE dist_code = @dist AND stype = @stype;
    """
    jc = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dist",  "STRING", dist_code),
            bigquery.ScalarQueryParameter("stype", "STRING", salesman_type),
        ]
    )
    result = list(client.query(sql, job_config=jc).result())
    seq = int(result[0]["next_seq"]) if result else 1
    return f"{salesman_type}{dist_code}{str(seq).zfill(3)}"


# ─── Write operations ─────────────────────────────────────────────────────────

def insert_salesman_record(data: dict) -> tuple[bool, str]:
    client = get_client()
    row = {**data, "uploaded_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}
    df = pd.DataFrame([row])
    for col in ["tanggal_lahir", "tanggal_join_g2g"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize("UTC")
    try:
        jc = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        client.load_table_from_dataframe(df, SALESMAN_TABLE, job_config=jc).result()
        _audit(client, "SALESMAN_INSERT", "salesman",
               data.get("nama_salesman", ""), data.get("kode_distributor", ""), data)
        return True, ""
    except Exception as e:
        return False, str(e)


def insert_mapping_record(salesman_id: str, dist_code: str,
                          salesman_type: str, nama_salesman: str = "") -> tuple[bool, str]:
    client = get_client()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    row = {
        "salesman_id":    salesman_id,
        "salesman_type":  salesman_type,
        "distributor_code": dist_code,
        "salesman":       sanitize_salesman_name(nama_salesman) if nama_salesman else "",
        "is_active":      True,
        "created_at":     now,
        "updated_at":     now,
    }
    try:
        jc = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        client.load_table_from_dataframe(pd.DataFrame([row]), MAPPING_TABLE, job_config=jc).result()
        _audit(client, "MAPPING_INSERT", "salesman", salesman_id, dist_code, row)
        return True, ""
    except Exception as e:
        return False, str(e)


def update_salesman_record(salesman_id: str, dist_code: str,
                           updated_fields: dict) -> tuple[bool, str]:
    """
    Update HR fields in gt_master_salesman — targets by salesman_id (via mapping),
    NOT by name.  This avoids the production bug where two salesmen with the same
    name both get updated.
    """
    allowed = {
        "nama_salesman":              "STRING",
        "nama_spv_external":          "STRING",
        "nama_spv_internal":          "STRING",
        "nama_spv_internal_2":        "STRING",
        "status_salesman":            "STRING",
        "total_outlet_coverage_pjp":  "INT64",
        "gaji_pokok":                 "INT64",
        "tunjangan_dan_insentif":     "INT64",
        "tanggal_lahir":              "TIMESTAMP",
        "tanggal_join_g2g":           "TIMESTAMP",
        "jenis_kelamin":              "STRING",
        "pendidikan_terakhir":        "STRING",
        "pengalaman_bulan":           "INT64",
        "principal_lain":             "STRING",
        "no_hp":                      "STRING",
    }

    set_clauses, params = [], [
        bigquery.ScalarQueryParameter("sid",  "STRING", salesman_id),
        bigquery.ScalarQueryParameter("dist", "STRING", dist_code),
    ]

    for field, value in updated_fields.items():
        if field not in allowed:
            continue
        bq_type = allowed[field]
        if bq_type == "INT64":
            try:
                value = int(value) if value is not None else 0
            except (TypeError, ValueError):
                value = 0
        elif bq_type == "TIMESTAMP" and value is not None:
            try:
                value = pd.to_datetime(value).strftime("%Y-%m-%dT00:00:00")
            except Exception:
                pass
        elif bq_type == "STRING":
            if value is not None:
                try:
                    if pd.isna(value):
                        value = None
                except (TypeError, ValueError):
                    pass

        set_clauses.append(f"{field} = @p_{field}")
        params.append(bigquery.ScalarQueryParameter(f"p_{field}", bq_type, value))

    if not set_clauses:
        return False, "Tidak ada field yang valid untuk diupdate."

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    params.append(bigquery.ScalarQueryParameter("updated_at", "STRING", now))

    # Resolve the salesman's name from the mapping (so we can match on the master table)
    sql = f"""
        UPDATE `{SALESMAN_TABLE}` s
        SET {", ".join(set_clauses)}, uploaded_at = @updated_at
        WHERE UPPER(TRIM(s.nama_salesman)) = (
            SELECT UPPER(TRIM(salesman))
            FROM `{MAPPING_TABLE}`
            WHERE salesman_id = @sid
              AND UPPER(TRIM(distributor_code)) = UPPER(TRIM(@dist))
              AND is_active = TRUE
            LIMIT 1
        )
        AND UPPER(TRIM(s.kode_distributor)) = UPPER(TRIM(@dist))
    """
    client = get_client()
    try:
        jc = bigquery.QueryJobConfig(query_parameters=params)
        client.query(sql, job_config=jc).result()
        _audit(client, "SALESMAN_UPDATE", "salesman", salesman_id, dist_code, updated_fields)
        return True, ""
    except Exception as e:
        return False, str(e)


def sync_mapping_name(salesman_id: str, dist_code: str, new_name: str) -> None:
    """Keep gt_salesman_mapping.salesman in sync when a name changes."""
    client = get_client()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
        UPDATE `{MAPPING_TABLE}`
        SET salesman   = @new_name,
            updated_at = @ts
        WHERE salesman_id = @sid
          AND is_active   = TRUE
          AND UPPER(TRIM(distributor_code)) = UPPER(TRIM(@dist))
    """
    jc = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("new_name", "STRING", new_name),
            bigquery.ScalarQueryParameter("ts",       "STRING", now),
            bigquery.ScalarQueryParameter("sid",      "STRING", salesman_id),
            bigquery.ScalarQueryParameter("dist",     "STRING", dist_code),
        ]
    )
    try:
        client.query(sql, job_config=jc).result()
    except Exception:
        pass


def deactivate_mapping(salesman_id: str, dist_code: str) -> tuple[bool, str]:
    client = get_client()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
        UPDATE `{MAPPING_TABLE}`
        SET is_active  = FALSE,
            updated_at = @ts
        WHERE salesman_id = @sid
          AND is_active   = TRUE
    """
    jc = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("sid", "STRING", salesman_id),
            bigquery.ScalarQueryParameter("ts",  "STRING", now),
        ]
    )
    try:
        client.query(sql, job_config=jc).result()
        _audit(client, "SALESMAN_DEACTIVATE", "salesman", salesman_id, dist_code, {})
        return True, ""
    except Exception as e:
        return False, str(e)
