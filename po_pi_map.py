"""One-to-many PI mapping for the RSA PI Validation tab.

Main table keeps one primary reference_cust_order_no.
All confirmed PI numbers live in gt_schema.po_supply_control_g2g_pi_map.
"""

from __future__ import annotations

from dataclasses import dataclass, field

SUPPLY_TABLE = "skintific-data-warehouse.gt_schema.po_supply_control_g2g"
PI_MAP_TABLE = "skintific-data-warehouse.gt_schema.po_supply_control_g2g_pi_map"

PRIMARY_BADGE_STYLE = (
    "display:inline-block;margin:0.15rem 0.25rem 0.15rem 0;"
    "padding:0.2rem 0.55rem;border-radius:999px;font-size:0.75rem;"
    "font-weight:700;background:#0B6E4F;color:#FFFFFF;"
)
SECONDARY_BADGE_STYLE = (
    "display:inline-block;margin:0.15rem 0.25rem 0.15rem 0;"
    "padding:0.2rem 0.55rem;border-radius:999px;font-size:0.75rem;"
    "font-weight:600;background:#E8EEF5;color:#1F2937;border:1px solid #C9D4E3;"
)


@dataclass(frozen=True)
class PiMapSelection:
    distributor_name: str
    file_name: str
    selected_refs: tuple[str, ...]
    primary_ref: str
    validation_by_rsa: str
    candidate_refs: tuple[str, ...] = ()


@dataclass
class PiMapPlan:
    selection: PiMapSelection | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.selection is not None and not self.errors


def clean_ref(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text


def plan_pi_mapping(
    distributor_name: str,
    file_name: str,
    candidate_refs: list[str],
    selected_refs: list[str],
    primary_ref: str | None,
    validation_by_rsa: str,
) -> PiMapPlan:
    plan = PiMapPlan()
    label = f"{distributor_name} / {file_name}"
    candidates = tuple(dict.fromkeys(clean_ref(r) for r in candidate_refs if clean_ref(r)))
    selected = tuple(dict.fromkeys(clean_ref(r) for r in selected_refs if clean_ref(r)))
    primary = clean_ref(primary_ref)
    note = clean_ref(validation_by_rsa)

    if not selected:
        plan.errors.append(f"{label}: centang minimal satu nomor PI.")
        return plan
    unknown = [ref for ref in selected if ref not in candidates]
    if unknown:
        plan.errors.append(f"{label}: nomor tidak ada di kandidat DMS: {', '.join(unknown)}")
        return plan
    if not primary:
        plan.errors.append(f"{label}: pilih Primary PI.")
        return plan
    if primary not in selected:
        plan.errors.append(f"{label}: Primary PI harus termasuk nomor yang dicentang.")
        return plan
    if not note:
        plan.errors.append(f"{label}: isi Validation by RSA.")
        return plan

    plan.selection = PiMapSelection(
        distributor_name=distributor_name.strip().upper(),
        file_name=file_name,
        selected_refs=selected,
        primary_ref=primary,
        validation_by_rsa=note,
        candidate_refs=candidates,
    )
    return plan


def render_ref_badges_html(selected_refs: list[str], primary_ref: str | None) -> str:
    """Primary badge is green; other mapped refs stay muted."""
    primary = clean_ref(primary_ref)
    parts = []
    for ref in selected_refs:
        value = clean_ref(ref)
        if not value:
            continue
        if value == primary:
            parts.append(
                f'<span style="{PRIMARY_BADGE_STYLE}" title="Primary PI">PRIMARY · {value}</span>'
            )
        else:
            parts.append(f'<span style="{SECONDARY_BADGE_STYLE}">{value}</span>')
    if not parts:
        return "<span style='color:#6B7280;font-size:0.75rem;'>Belum ada PI yang dicentang.</span>"
    return "".join(parts)


def ensure_pi_map_table(client) -> None:
    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{PI_MAP_TABLE}` (
          distributor_name STRING,
          file_name STRING,
          reference_cust_order_no STRING,
          is_primary BOOL,
          status STRING,
          validation_by_rsa STRING,
          mapped_at TIMESTAMP,
          mapped_by STRING
        )
        CLUSTER BY distributor_name, file_name
        OPTIONS (
          description = 'One-to-many PI map for po_supply_control_g2g. Primary also written to reference_cust_order_no.'
        )
        """
    ).result()


def fetch_existing_maps(client, distributors: list[str], file_names: list[str]) -> dict[tuple[str, str], dict]:
    """Return {(distributor, file): {selected: [...], primary: str, note: str}}."""
    from google.cloud import bigquery

    if not distributors or not file_names:
        return {}
    ensure_pi_map_table(client)
    rows = client.query(
        f"""
        SELECT
          UPPER(TRIM(distributor_name)) AS distributor_name,
          file_name,
          reference_cust_order_no,
          is_primary,
          validation_by_rsa
        FROM `{PI_MAP_TABLE}`
        WHERE status = 'CONFIRMED'
          AND UPPER(TRIM(distributor_name)) IN UNNEST(@distributors)
          AND file_name IN UNNEST(@file_names)
        ORDER BY distributor_name, file_name, is_primary DESC, reference_cust_order_no
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "distributors", "STRING", [d.strip().upper() for d in distributors]
                ),
                bigquery.ArrayQueryParameter("file_names", "STRING", list(file_names)),
            ]
        ),
    ).result()

    out: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (row.distributor_name, row.file_name)
        bucket = out.setdefault(key, {"selected": [], "primary": "", "note": ""})
        bucket["selected"].append(row.reference_cust_order_no)
        if row.is_primary:
            bucket["primary"] = row.reference_cust_order_no
        if row.validation_by_rsa:
            bucket["note"] = row.validation_by_rsa
    return out


def apply_pi_mapping(client, selection: PiMapSelection, mapped_by: str = "rsa") -> None:
    """Write link rows and set primary on the supply-control table."""
    from google.cloud import bigquery

    ensure_pi_map_table(client)
    structs = [
        bigquery.StructQueryParameter(
            None,
            bigquery.ScalarQueryParameter("reference_cust_order_no", "STRING", ref),
            bigquery.ScalarQueryParameter("is_primary", "BOOL", ref == selection.primary_ref),
        )
        for ref in selection.selected_refs
    ]
    sql = f"""
    BEGIN TRANSACTION;

    DELETE FROM `{PI_MAP_TABLE}`
    WHERE UPPER(TRIM(distributor_name)) = @distributor_name
      AND file_name = @file_name;

    INSERT INTO `{PI_MAP_TABLE}` (
      distributor_name,
      file_name,
      reference_cust_order_no,
      is_primary,
      status,
      validation_by_rsa,
      mapped_at,
      mapped_by
    )
    SELECT
      @distributor_name,
      @file_name,
      item.reference_cust_order_no,
      item.is_primary,
      'CONFIRMED',
      @validation_by_rsa,
      CURRENT_TIMESTAMP(),
      @mapped_by
    FROM UNNEST(@items) AS item;

    UPDATE `{SUPPLY_TABLE}`
    SET
      reference_cust_order_no = @primary_ref,
      validation_by_rsa = @validation_by_rsa
    WHERE UPPER(TRIM(distributor_name)) = @distributor_name
      AND file_name = @file_name
      AND reference_cust_order_no IS NOT NULL;

    COMMIT TRANSACTION;
    """
    client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "distributor_name", "STRING", selection.distributor_name
                ),
                bigquery.ScalarQueryParameter("file_name", "STRING", selection.file_name),
                bigquery.ScalarQueryParameter("primary_ref", "STRING", selection.primary_ref),
                bigquery.ScalarQueryParameter(
                    "validation_by_rsa", "STRING", selection.validation_by_rsa
                ),
                bigquery.ScalarQueryParameter("mapped_by", "STRING", mapped_by),
                bigquery.ArrayQueryParameter("items", "RECORD", structs),
            ]
        ),
    ).result()
