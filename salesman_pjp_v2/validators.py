"""
Upload validation for PJP template files.
Direct port of production logic with enhanced error categories and duplicate detection.
"""

import pandas as pd
from .config import PJP_REQUIRED, DAY_OPTIONS, WEEK_OPTIONS, FREQUENCY_OPTIONS


def _is_empty(val) -> bool:
    return pd.isna(val) or str(val).strip() == ""


def _get_unique_distributors(df: pd.DataFrame, col="Kode Distributor") -> list:
    if col not in df.columns:
        return []
    return (
        df[col].dropna().astype(str).str.strip()
        .replace("", pd.NA).dropna().unique().tolist()
    )


def read_template_sheet(uploaded_file, distributor_map: dict,
                        store_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Parse the PJP Template sheet from an uploaded Excel file.
    Header is always at row 3 (0-indexed: header_row=2).
    """
    df = pd.read_excel(uploaded_file, sheet_name="PJP Template", header=2)
    df = df.dropna(how="all")

    if "Hari" in df.columns:
        df["Hari"] = df["Hari"].astype(str).str.strip().str.title()
    if "Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap" in df.columns:
        df["Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap"] = (
            df["Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap"]
            .astype(str).str.strip().str.title()
        )
    if "Frekuensi" in df.columns:
        df["Frekuensi"] = df["Frekuensi"].astype(str).str.strip().str.upper()

    # Resolve Kode Distributor from Nama Distributor
    name_to_code = {v: k for k, v in distributor_map.items()}
    if "Nama Distributor" in df.columns:
        df["Kode Distributor"] = df["Nama Distributor"].apply(
            lambda x: name_to_code.get(str(x).strip(), "") if pd.notna(x) else ""
        )

    # Backfill Nama Toko from store_df
    if store_df is not None and "Kode Toko" in df.columns:
        code_to_name = dict(zip(store_df["store_code"], store_df["store_name"]))
        df["Nama Toko"] = df["Kode Toko"].apply(
            lambda x: code_to_name.get(str(x).strip(), "") if pd.notna(x) else ""
        )

    return df.reset_index(drop=True)


def validate_pjp_df(
    df: pd.DataFrame,
    distributor_map: dict,
    store_df: pd.DataFrame | None = None,
) -> tuple[list, list]:
    """
    Validate a PJP DataFrame.

    Returns:
        (errors, warnings)
        errors   — must be resolved before upload
        warnings — informational, upload can proceed
    """
    errors, warnings = [], []

    # Layer 1: column presence
    missing_cols = [c for c in PJP_REQUIRED if c not in df.columns]
    if missing_cols:
        errors.append(f"Kolom wajib tidak ditemukan: {', '.join(missing_cols)}")
        return errors, warnings

    # Layer 2: row completeness
    for i, row in df.iterrows():
        excel_n = i + 4  # header is row 3, data starts row 4
        values = {c: row.get(c, "") for c in PJP_REQUIRED}
        non_empty = [c for c, v in values.items() if not _is_empty(v)]
        empty     = [c for c, v in values.items() if _is_empty(v)]
        if non_empty and empty:
            errors.append(f"Baris {excel_n}: kolom wajib belum terisi — {', '.join(empty)}")

    # Layer 3: single distributor per file
    unique_dist = _get_unique_distributors(df)
    if len(unique_dist) > 1:
        errors.append(
            f"File hanya boleh berisi 1 kode distributor. "
            f"Ditemukan {len(unique_dist)}: {', '.join(unique_dist)}"
        )
        return errors, warnings

    valid_store_codes = set()
    if store_df is not None and not store_df.empty:
        valid_store_codes = set(store_df["store_code"].dropna().tolist())

    for i, row in df.iterrows():
        excel_n = i + 4

        # Layer 4: referential integrity
        kode = str(row.get("Kode Distributor", "")).strip()
        if kode and kode not in distributor_map:
            errors.append(f"Baris {excel_n}: Kode Distributor '{kode}' tidak valid")

        kode_toko = str(row.get("Kode Toko", "")).strip()
        if kode_toko and valid_store_codes and kode_toko not in valid_store_codes:
            warnings.append(
                f"Baris {excel_n}: Kode Toko '{kode_toko}' tidak ditemukan di master store"
            )

        # Layer 5: enum validation
        for col, opts in [
            ("Hari",                                               DAY_OPTIONS),
            ("Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap",  WEEK_OPTIONS),
            ("Frekuensi",                                          FREQUENCY_OPTIONS),
        ]:
            val = row.get(col, "")
            if pd.notna(val) and str(val).strip() and val not in opts:
                errors.append(
                    f"Baris {excel_n}: '{col}' nilai tidak valid — '{val}'"
                )

    # Layer 6: duplicate store-salesman-day within the file
    if all(c in df.columns for c in ["Kode Toko", "Nama Salesman", "Hari"]):
        dup_mask = df.duplicated(subset=["Kode Toko", "Nama Salesman", "Hari"], keep=False)
        dup_rows = df.index[dup_mask].tolist()
        if dup_rows:
            row_nums = ", ".join(str(r + 4) for r in dup_rows[:10])
            warnings.append(
                f"Baris {row_nums}: kombinasi Kode Toko + Nama Salesman + Hari duplikat "
                f"({len(dup_rows)} baris total)"
            )

    return errors, warnings
