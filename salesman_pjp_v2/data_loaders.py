"""
Cached BigQuery data loaders.
Basis loader fetches the three brand-salesman fallback columns (SKT / G2G / TPH).

Distributor identity (name + current region) comes from the ONE shared
definition in distributor_naming.canonical_dist_cte(), the same one
salesman_pjp.py and the gt_master_salesman_v view use, so the three cannot
drift apart. See that module for why identity is the code and not the name.
"""

import streamlit as st
import pandas as pd
from distributor_naming import canonical_dist_cte
from .bq_client import get_client
from .config import DISTRIBUTOR_TABLE, BASIS_TABLE, BASIS_COL_SKT, BASIS_COL_G2G, BASIS_COL_TPH


@st.cache_data(show_spinner="Memuat data distributor...", ttl=1800)
def load_distributor_data() -> pd.DataFrame:
    # region_g2g is the CURRENT org mapping. master_distributor also carries a
    # plain `region` column holding the OLD one (still "Southern Sumatera 2"
    # for DST351/DST352, which moved to Southern Sumatera 1 in September
    # 2026) — never read that column.
    client = get_client()
    df = client.query("""
        SELECT
            distributor_name,
            region_g2g   AS region,
            distributor_code,
            asm_g2g      AS asm
        FROM ({cte})
        WHERE distributor_code IN (
            SELECT UPPER(TRIM(distributor_code)) FROM `{table}`
            WHERE status = 'Active'
        )
    """.format(cte=canonical_dist_cte(DISTRIBUTOR_TABLE),
               table=DISTRIBUTOR_TABLE)).to_dataframe()
    df["distributor_code"] = df["distributor_code"].astype(str).str.strip()
    return df.drop_duplicates(subset=["distributor_code"]).reset_index(drop=True)


@st.cache_data(show_spinner="Memuat data toko...", ttl=1800)
def load_store_data() -> pd.DataFrame:
    """
    Loads store master joined to distributor, for the Excel Lookup sheet.
    Returns store_code, store_name, distributor_name, store_label.
    """
    # Joined BY CODE (dst_id_g2g), not by name. The old name-based join broke
    # the moment the store table and the master disagreed on spelling — the
    # same failure mode that split DST351 into two distributors — and it
    # silently produced a NULL match rather than an error.
    client = get_client()
    df = client.query("""
        WITH dist AS ({cte})
        SELECT
            UPPER(b.cust_id)    AS store_code,
            UPPER(b.store_name) AS store_name,
            COALESCE(d.distributor_name, UPPER(TRIM(b.distributor_g2g)))
                                AS distributor_name
        FROM `{basis_table}` b
        LEFT JOIN dist d
          ON d.distributor_code = UPPER(TRIM(b.dst_id_g2g))
    """.format(cte=canonical_dist_cte(DISTRIBUTOR_TABLE),
               basis_table=BASIS_TABLE)).to_dataframe()
    df = df.dropna(subset=["store_code", "store_name", "distributor_name"])
    df["store_code"]       = df["store_code"].astype(str).str.strip()
    df["store_name"]       = df["store_name"].astype(str).str.strip()
    df["distributor_name"] = df["distributor_name"].astype(str).str.strip()
    df["store_label"]      = df["store_code"] + " - " + df["store_name"]
    return df.drop_duplicates(subset=["store_code"]).reset_index(drop=True)


@st.cache_data(show_spinner="Memuat data basis toko...", ttl=3600)
def load_basis_pjp(dist_name: str) -> pd.DataFrame:
    """
    Load fallback salesman assignments per store from master_store_database_basis.
    Columns AJ/AK/AL → salesman_skt / salesman_g2g / salesman_tph.

    Args:
        dist_name: UPPER-cased distributor name (as stored in distributor_g2g column).

    Returns DataFrame with columns:
        store_code, store_name, salesman_skt, salesman_g2g, salesman_tph
    """
    client = get_client()
    query = """
        SELECT
            UPPER(TRIM(cust_id))                    AS store_code,
            UPPER(TRIM(store_name))                 AS store_name,
            UPPER(TRIM({col_skt}))                  AS salesman_skt,
            UPPER(TRIM({col_g2g}))                  AS salesman_g2g,
            UPPER(TRIM({col_tph}))                  AS salesman_tph
        FROM `{table}`
        WHERE UPPER(TRIM(distributor_g2g)) = UPPER(TRIM(@dist_name))
        ORDER BY store_code
    """.format(
        col_skt=BASIS_COL_SKT,
        col_g2g=BASIS_COL_G2G,
        col_tph=BASIS_COL_TPH,
        table=BASIS_TABLE,
    )
    from google.cloud import bigquery as bq
    job_config = bq.QueryJobConfig(
        query_parameters=[bq.ScalarQueryParameter("dist_name", "STRING", dist_name)]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    for col in ["salesman_skt", "salesman_g2g", "salesman_tph"]:
        df[col] = df[col].where(df[col] != "", other=None)
    return df.reset_index(drop=True)


def build_lookup_tables(dist_df: pd.DataFrame):
    distributor_map = dict(zip(dist_df["distributor_code"], dist_df["distributor_name"]))
    asm_options     = sorted(dist_df["asm"].dropna().unique().tolist())
    region_options  = sorted(dist_df["region"].dropna().unique().tolist())
    return distributor_map, asm_options, region_options
