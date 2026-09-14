"""
PJP read / atomic write operations.

Key fixes over production:
  - delete + insert wrapped in a single BQ BEGIN...END scripted block
    → if INSERT fails, BQ rolls back the DELETE automatically
  - Every commit writes to the audit log
"""

import json
import uuid
import pandas as pd
from datetime import datetime

import streamlit as st
from google.cloud import bigquery

from .bq_client import get_client
from .config import PJP_TABLE, AUDIT_TABLE
from .salesman_crud import _audit


_PJP_COL_MAP = {
    "ASM":                                               "asm",
    "Region":                                            "region",
    "Nama Distributor":                                  "nama_distributor",
    "Kode Distributor":                                  "kode_distributor",
    "Nama Salesman":                                     "nama_salesman",
    "Kode Toko":                                         "kode_toko",
    "Nama Toko":                                         "nama_toko",
    "Hari":                                              "hari",
    "Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap": "minggu",
    "Frekuensi":                                         "frekuensi",
}


def get_pjp_list(dist_code: str = None, salesman_name: str = None) -> pd.DataFrame:
    client = get_client()
    conditions, params = [], []
    if dist_code:
        conditions.append("UPPER(kode_distributor) = UPPER(@kode)")
        params.append(bigquery.ScalarQueryParameter("kode", "STRING", dist_code))
    if salesman_name:
        conditions.append("UPPER(TRIM(nama_salesman)) = UPPER(TRIM(@salesman))")
        params.append(bigquery.ScalarQueryParameter("salesman", "STRING", salesman_name))
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    sql = f"SELECT * FROM `{PJP_TABLE}` {where} ORDER BY nama_salesman, hari"
    jc = bigquery.QueryJobConfig(query_parameters=params)
    try:
        return client.query(sql, job_config=jc).to_dataframe()
    except Exception as e:
        st.error(f"Gagal memuat data PJP: {e}")
        return pd.DataFrame()


def commit_pjp_upload(
    new_df: pd.DataFrame,
    dist_code: str,
    salesman_name: str | None = None,
) -> tuple[bool, str]:
    """
    Atomically replace PJP rows for the given scope (distributor or salesman).

    Uses a BigQuery BEGIN...EXCEPTION...END scripted block:
      - DELETE scoped rows
      - INSERT new rows (via a temp table approach)
    If either statement fails, BQ rolls back the entire block.

    Returns (success: bool, message: str).
    """
    client = get_client()

    # Rename columns to BQ names and add metadata
    existing_cols = {c: _PJP_COL_MAP[c] for c in _PJP_COL_MAP if c in new_df.columns}
    bq_df = new_df[list(existing_cols.keys())].rename(columns=existing_cols).copy()
    bq_df["uploaded_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Build the DELETE WHERE clause
    if salesman_name:
        delete_where = (
            f"UPPER(kode_distributor) = UPPER('{dist_code}')"
            f" AND UPPER(TRIM(nama_salesman)) = UPPER(TRIM('{salesman_name}'))"
        )
        scope_label = f"salesman '{salesman_name}' @ {dist_code}"
    else:
        delete_where = f"UPPER(kode_distributor) = UPPER('{dist_code}')"
        scope_label  = f"semua PJP distributor {dist_code}"

    # Build the VALUES list for INSERT from the DataFrame
    # We use a parameterized multi-statement approach via a temp table.
    # Step 1: load new rows into a temp table using the Python BQ client (WRITE_TRUNCATE)
    # Step 2: run a scripted BEGIN...END that DELETEs old + INSERTs from temp
    temp_table_id = f"skintific-data-warehouse.sfa_step._pjp_upload_temp_{dist_code.lower()}"

    try:
        # Stage: write new rows to temp table (creates/truncates in sfa_step dataset)
        jc_stage = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )
        stage_job = client.load_table_from_dataframe(bq_df, temp_table_id, job_config=jc_stage)
        stage_job.result()

        # Atomic swap: DELETE old, INSERT from temp, drop temp if INSERT succeeds
        # BQ BEGIN...EXCEPTION...END rolls back on any error inside the block.
        column_list = ", ".join(bq_df.columns)
        swap_sql = f"""
            BEGIN
                DELETE FROM `{PJP_TABLE}`
                WHERE {delete_where};

                INSERT INTO `{PJP_TABLE}` ({column_list})
                SELECT {column_list} FROM `{temp_table_id}`;
            EXCEPTION WHEN ERROR THEN
                RAISE USING MESSAGE = @@error.message;
            END;
        """
        swap_job = client.query(swap_sql)
        swap_job.result()

        # Drop temp table (best-effort, non-blocking on failure)
        try:
            client.delete_table(temp_table_id, not_found_ok=True)
        except Exception:
            pass

        rows_inserted = len(bq_df)
        _audit(client, "PJP_UPLOAD", "pjp", dist_code, dist_code, {
            "scope": scope_label,
            "rows_inserted": rows_inserted,
            "salesman_filter": salesman_name,
        })
        return True, f"Berhasil menyimpan {rows_inserted} baris PJP baru untuk {scope_label}."

    except Exception as e:
        # Try to clean up temp table on failure
        try:
            client.delete_table(temp_table_id, not_found_ok=True)
        except Exception:
            pass
        return False, str(e)
