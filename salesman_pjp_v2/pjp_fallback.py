"""
Database Basis Fallback Layer — Effective PJP View.

Business rule:
    effective_salesman = COALESCE(PJP assignment, Database Basis)

The basis has THREE brand-specific fallback columns per store:
    AJ = salesman_skt  →  Skintific brand salesman
    AK = salesman_g2g  →  Glad2Glow (G2G) brand salesman
    AL = salesman_tph  →  Timephoria brand salesman

The basis has NO hari / frekuensi / minggu columns.
Schedule fields (hari, frekuensi, minggu) come from the PJP only.
Basis-sourced rows show NULL for schedule — the store has a default salesman
but the visit schedule has not been defined in the PJP yet.

Effective PJP structure per store:
    - If ANY PJP rows exist for the store  →  show those rows, source='PJP'
    - If NO PJP rows for the store         →  show one row per non-null basis
                                              salesman, source='Basis'
"""

import pandas as pd
from .bq_client import get_client
from .config import PJP_TABLE, BASIS_TABLE, BASIS_COL_SKT, BASIS_COL_G2G, BASIS_COL_TPH
from google.cloud import bigquery


_EFFECTIVE_PJP_SQL = """
WITH pjp AS (
    SELECT
        UPPER(TRIM(kode_toko))       AS kode_toko,
        UPPER(TRIM(nama_salesman))   AS nama_salesman,
        hari,
        minggu,
        frekuensi,
        kode_distributor
    FROM `{pjp_table}`
    WHERE UPPER(TRIM(kode_distributor)) = UPPER(TRIM(@dist_code))
),

basis AS (
    SELECT
        UPPER(TRIM(cust_id))                AS kode_toko,
        UPPER(TRIM(store_name))             AS store_name,
        NULLIF(UPPER(TRIM({col_skt})), '')  AS salesman_skt,
        NULLIF(UPPER(TRIM({col_g2g})), '')  AS salesman_g2g,
        NULLIF(UPPER(TRIM({col_tph})), '')  AS salesman_tph
    FROM `{basis_table}`
    WHERE UPPER(TRIM(distributor_g2g)) = UPPER(TRIM(@dist_name))
),

-- Stores that already appear in PJP for this distributor
pjp_covered AS (
    SELECT DISTINCT kode_toko FROM pjp
),

-- Basis rows for stores NOT in PJP — unpivot to one row per brand salesman
basis_fallback AS (
    SELECT
        b.kode_toko,
        b.store_name,
        brand.brand_label                            AS brand,
        brand.salesman_name,
        CAST(NULL AS STRING)                         AS hari,
        CAST(NULL AS STRING)                         AS minggu,
        CAST(NULL AS STRING)                         AS frekuensi,
        'Basis'                                      AS assignment_source
    FROM basis b
    LEFT JOIN pjp_covered pc ON b.kode_toko = pc.kode_toko
    CROSS JOIN UNNEST([
        STRUCT('SKT' AS brand_label, b.salesman_skt AS salesman_name),
        STRUCT('G2G' AS brand_label, b.salesman_g2g AS salesman_name),
        STRUCT('TPH' AS brand_label, b.salesman_tph AS salesman_name)
    ]) AS brand
    WHERE pc.kode_toko IS NULL          -- store not in PJP
      AND brand.salesman_name IS NOT NULL
),

-- PJP rows for covered stores
pjp_rows AS (
    SELECT
        p.kode_toko,
        b.store_name,
        CAST(NULL AS STRING)             AS brand,
        p.nama_salesman,
        p.hari,
        p.minggu,
        p.frekuensi,
        'PJP'                            AS assignment_source
    FROM pjp p
    LEFT JOIN basis b ON p.kode_toko = b.kode_toko
)

SELECT * FROM pjp_rows
UNION ALL
SELECT * FROM basis_fallback
ORDER BY kode_toko, assignment_source DESC, brand, nama_salesman
"""


def get_effective_pjp(dist_code: str, dist_name: str) -> pd.DataFrame:
    """
    Return the effective PJP for a distributor:
    - PJP-covered stores: actual assignments (hari/minggu/frekuensi populated)
    - Uncovered stores: basis fallback rows, one per brand salesman (schedule = NULL)

    Columns returned:
        kode_toko, store_name, brand, nama_salesman,
        hari, minggu, frekuensi, assignment_source
    """
    sql = _EFFECTIVE_PJP_SQL.format(
        pjp_table=PJP_TABLE,
        basis_table=BASIS_TABLE,
        col_skt=BASIS_COL_SKT,
        col_g2g=BASIS_COL_G2G,
        col_tph=BASIS_COL_TPH,
    )
    client = get_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dist_code", "STRING", dist_code),
            bigquery.ScalarQueryParameter("dist_name",  "STRING", dist_name),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()
    return df.reset_index(drop=True)


def get_coverage_summary(effective_df: pd.DataFrame) -> dict:
    """
    Compute coverage statistics from the effective PJP DataFrame.
    Returns a dict with keys:
        total_stores, pjp_stores, basis_stores, basis_pct,
        skt_missing, g2g_missing, tph_missing
    """
    all_stores    = effective_df["kode_toko"].nunique()
    pjp_stores    = effective_df.loc[effective_df["assignment_source"] == "PJP",  "kode_toko"].nunique()
    basis_stores  = effective_df.loc[effective_df["assignment_source"] == "Basis", "kode_toko"].nunique()
    basis_pct     = round(basis_stores / all_stores * 100, 1) if all_stores else 0

    basis_rows = effective_df[effective_df["assignment_source"] == "Basis"]
    skt_missing = basis_rows.loc[basis_rows["brand"] == "SKT", "kode_toko"].nunique()
    g2g_missing = basis_rows.loc[basis_rows["brand"] == "G2G", "kode_toko"].nunique()
    tph_missing = basis_rows.loc[basis_rows["brand"] == "TPH", "kode_toko"].nunique()

    return {
        "total_stores": all_stores,
        "pjp_stores":   pjp_stores,
        "basis_stores": basis_stores,
        "basis_pct":    basis_pct,
        "skt_missing":  skt_missing,
        "g2g_missing":  g2g_missing,
        "tph_missing":  tph_missing,
    }
