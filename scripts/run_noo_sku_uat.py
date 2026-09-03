#!/usr/bin/env python3
"""Standalone UAT / controlled-production runner for NOO & SKU Mapping.

Runs the complete upload flow from the command line, using the SAME modules the
Streamlit app uses (`noo_sku.*`) — validation, enrichment, duplicate detection,
mapping and the guarded Sheets write are not reimplemented here. This script is
a driver, so UAT behaviour cannot diverge from production behaviour.

    dry-run     read, validate, enrich, classify, preview. NO WRITE. (default)
    pilot       write at most PILOT_MAX_ROWS rows, then verify them.
    production  controlled write; requires --confirm-production.

Examples
--------
    python scripts/run_noo_sku_uat.py --mode dry-run --file test_noo.xlsx \\
        --distributor DST082

    python scripts/run_noo_sku_uat.py --mode pilot --file test_noo.xlsx \\
        --distributor DST082

    python scripts/run_noo_sku_uat.py --mode production --file noo.xlsx \\
        --distributor DST082 --confirm-production

Exit codes
----------
    0  success
    1  validation / duplicate check failed — nothing written
    2  configuration, credential or spreadsheet-access problem
    3  aborted by the operator
    4  write failed
    5  written but post-write verification FAILED — inspect immediately
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from noo_sku import (auth, config, enrichment, parsers, pipeline,  # noqa: E402
                     sources, validators, writer)
from noo_sku.customer_code import CustomerCodeResolver  # noqa: E402
from noo_sku.normalize import norm_key, now_business  # noqa: E402

EXIT_OK = 0
EXIT_VALIDATION = 1
EXIT_CONFIG = 2
EXIT_ABORTED = 3
EXIT_WRITE_FAILED = 4
EXIT_VERIFY_FAILED = 5

RULE = "=" * 60


def line(label, value=""):
    print(f"{label}\n{value}\n" if value != "" else f"{label}\n")


def header(title):
    print(f"\n{RULE}\n{title}\n{RULE}\n")


# ─── Argument parsing ─────────────────────────────────────────────────────────
def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        prog="run_noo_sku_uat.py",
        description="Controlled UAT runner for the NOO & SKU Mapping pools.")
    ap.add_argument("--mode", choices=list(config.MODES),
                    default=config.MODE_DRY_RUN,
                    help="write mode (default: dry-run, never writes)")
    ap.add_argument("--file", required=True, help="filled .xlsx upload template")
    ap.add_argument("--distributor", help="authenticated Distributor Code, e.g. DST082")
    ap.add_argument("--type", choices=["noo", "sku", "auto"], default="auto",
                    help="upload type; 'auto' detects it from the template header")
    ap.add_argument("--confirm-production", action="store_true",
                    help="required for --mode production")
    ap.add_argument("--yes", action="store_true",
                    help="skip the interactive prompt (for scripted runs)")
    ap.add_argument("--pilot-max-rows", type=int, default=None,
                    help=f"pilot ceiling (default {config.DEFAULT_PILOT_MAX_ROWS})")
    ap.add_argument("--report-dir", default=str(REPO_ROOT / "reports"),
                    help="where to write the run report")
    ap.add_argument("--no-report", action="store_true")
    return ap.parse_args(argv)


# ─── Setup ────────────────────────────────────────────────────────────────────
def build_settings(args):
    settings = config.Settings(mode=args.mode,
                               pilot_max_rows=args.pilot_max_rows)
    if args.mode != config.MODE_DRY_RUN and settings.dry_run:
        print("WARNING: mode is "
              f"{args.mode!r} but WRITE_ENABLED is not true — no write will "
              "occur. Set WRITE_ENABLED=true to write.")
    return settings


def connect(settings):
    """Credentials and Sheets client. Never reads a key from source."""
    creds, project = sources.load_credentials()
    client = sources.SheetsClient(creds, settings.tracker_spreadsheet_id)
    client.tab_names()          # fail fast if the spreadsheet is unreachable
    return creds, project, client


def resolve_distributor(client, code):
    distributors = sources.load_distributors(client)
    record = distributors.get(norm_key(code))
    if record is None:
        raise SystemExit(f"Distributor {code!r} tidak ditemukan di "
                         f"{config.TAB_DIST_DATABASE}.")
    if not record["active"]:
        raise SystemExit(f"Distributor {code!r} berstatus "
                         f"{record['status']!r} — hanya Active yang diizinkan.")
    return record, distributors


# ─── Flow ─────────────────────────────────────────────────────────────────────
def run_pipeline(args, settings, creds, project, client, distributor,
                 distributors, parsed):
    resolver = CustomerCodeResolver(
        dist_database=sources.suffixes_from_dist_database(distributors),
        sku_history=sources.suffixes_from_sku_history(client),
        po_history=_safe(sources.suffixes_from_po_history, creds, project))
    dist_enricher = enrichment.DistributorEnricher(
        master_distributor=_safe(sources.load_master_distributor, creds, project),
        dist_database=distributors)
    code = distributor["distributor_code"]

    if parsed.kind == parsers.UPLOAD_NOO:
        # Scope to every branch the admin is authorised for, not just their
        # own login code -- a single-code scope was the exact 2026-09-03 bug:
        # a multi-branch upload naming a sibling branch could never find that
        # branch's stores in master_store_database_basis, so the NOO Detector
        # correctly reported "not found" for data it was never given.
        allowed = auth.authorized_branches(distributors, code)
        company_name = distributor.get("company", "")
        by_cust, by_ref = _safe(
            sources.load_store_basis, creds, project, tuple(sorted(allowed)),
            default=({}, {"skt": {}, "tph": {}, "fcr": {}}))
        return pipeline.run_noo(
            parsed, distributor=distributor, resolver=resolver,
            dist_enricher=dist_enricher,
            store_enricher=enrichment.StoreEnricher(by_cust, by_ref),
            ledger=sources.load_noo_ledger(client, set(allowed)),
            known_cities=_safe(sources.load_city_reference, creds, default=set()),
            when=now_business(), allowed_branches=allowed,
            company_name=company_name)

    if not resolver.resolve(code).resolved:
        raise SystemExit(
            f"Singkatan distributor untuk {code} belum terdaftar; Customer Code "
            "tidak bisa dibuat otomatis. Lengkapi kolom 'Customer Branch Code' "
            f"pada sheet {config.TAB_DIST_DATABASE}.")
    products = sources.load_products(creds, project)
    return pipeline.run_sku(
        parsed, distributor=distributor, resolver=resolver,
        dist_enricher=dist_enricher,
        product_enricher=enrichment.ProductEnricher(products),
        ledger=sources.load_sku_ledger(client, code),
        product_lookup=products, when=now_business())


def _safe(fn, *a, default=None, **kw):
    """Optional data source: degrade rather than abort the whole run."""
    try:
        return fn(*a, **kw)
    except Exception as exc:
        print(f"  ! sumber opsional gagal dimuat ({fn.__name__}): "
              f"{type(exc).__name__}")
        return default if default is not None else {}


def print_summary(args, settings, distributor, parsed, result, tab):
    header("NOO/SKU MAPPING UAT")
    s = result.summary
    line("Mode:", settings.mode.upper()
         + ("  (DRY RUN — tidak menulis)" if settings.dry_run else ""))
    line("Distributor:", distributor["distributor_code"])
    line("Distributor Name:", distributor["distributor_name"])
    line("Upload Type:", result.kind)
    line("Input File:", args.file)
    line("Rows in File:", str(len(parsed.rows)))
    line("Valid Rows:", str(len(result.pool_rows)))
    line("Invalid Rows:", str(s["error"]))
    line("Duplicates:", str(s["exact_duplicate"] + s["duplicate_in_file"]))
    line("New Rows:", str(s["new"]))
    line("Correction Rows:", str(s["correction"]))
    line("Rows To Write:", str(len(result.eligible_rows)))
    line("Fallback Mappings:", str(result.fallback_count))
    line("Ambiguous Mappings:", str(result.ambiguous_count))
    line("Target Spreadsheet:", settings.tracker_spreadsheet_id)
    line("Target Tab:", tab)

    if result.errors:
        header("VALIDATION ERRORS")
        for issue in result.errors[:40]:
            print(f"  Baris {issue.row} — {issue.column}: {issue.problem} "
                  f"{issue.suggestion}")
        if len(result.errors) > 40:
            print(f"  ... dan {len(result.errors) - 40} error lainnya")

    dup = pipeline.duplicate_details(result)
    if dup:
        header("DUPLICATES (tidak akan ditulis)")
        for d in dup[:20]:
            print(f"  Baris {d['Baris']}: {d['Status']} — {d['Keterangan']}")

    mapping = pipeline.mapping_sources(result)
    if mapping:
        header("MAPPING SOURCE PER ROW")
        for m in mapping[:20]:
            extra = (f"store={m.get('Mapping Source (Store)')} "
                     f"on={m.get('Matched On')} SE={m.get('SE')!r}"
                     if result.kind == "NOO" else
                     f"product={m.get('Mapping Source (Product)')} "
                     f"cust_code={m.get('Customer Code')}")
            print(f"  Baris {m['Baris']} [{m['Brand']}] "
                  f"dist={m['Mapping Source (Distributor)']} "
                  f"fallback={m['Fallback']} {extra}")


def confirm(prompt, auto_yes):
    if auto_yes:
        print(f"{prompt} YES (--yes)")
        return True
    try:
        return input(f"{prompt} ").strip().upper() in ("YES", "Y")
    except EOFError:
        return False


def write_report(args, settings, distributor, parsed, result, tab, write_result,
                 verification, status):
    if args.no_report:
        return None
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = report_dir / f"noo_sku_uat_{stamp}.json"
    s = result.summary
    payload = {
        "timestamp": now_business().isoformat(),
        "mode": settings.mode,
        "write_enabled": settings.write_enabled,
        "distributor_code": distributor["distributor_code"],
        "distributor_name": distributor["distributor_name"],
        "upload_type": result.kind,
        "input_file": os.path.basename(args.file),
        "rows_in_file": len(parsed.rows),
        "valid_rows": len(result.pool_rows),
        "invalid_rows": s["error"],
        "duplicate_rows": s["exact_duplicate"] + s["duplicate_in_file"],
        "new_rows": s["new"],
        "correction_rows": s["correction"],
        "rows_to_write": len(result.eligible_rows),
        "rows_written": getattr(write_result, "rows_written", 0),
        "target_spreadsheet": settings.tracker_spreadsheet_id,
        "target_tab": tab,
        "batch_reference": result.upload_id,
        "input_time": result.pool_rows[0]["input_time"] if result.pool_rows else None,
        "fallback_count": result.fallback_count,
        "ambiguous_count": result.ambiguous_count,
        "mapping_sources": pipeline.mapping_sources(result),
        "errors": [{"row": i.row, "column": i.column, "problem": i.problem}
                   for i in result.errors],
        "verification": verification,
        "status": status,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False),
                    encoding="utf-8")
    print(f"\nReport: {path}")
    return path


# ─── Main ─────────────────────────────────────────────────────────────────────
def main(argv=None):
    args = parse_args(argv)

    if args.mode == config.MODE_PRODUCTION and not args.confirm_production:
        print("REFUSED: --mode production requires --confirm-production.")
        return EXIT_CONFIG
    if not args.distributor:
        print("REFUSED: --distributor wajib diisi (identitas terautentikasi).")
        return EXIT_CONFIG
    upload_path = Path(args.file)
    if not upload_path.is_file():
        print(f"REFUSED: file tidak ditemukan: {upload_path}")
        return EXIT_CONFIG

    try:
        settings = build_settings(args)
    except ValueError as exc:
        print(f"REFUSED: {exc}")
        return EXIT_CONFIG

    try:
        creds, project, client = connect(settings)
        distributor, distributors = resolve_distributor(client, args.distributor)
    except SystemExit as exc:
        print(f"REFUSED: {exc}")
        return EXIT_CONFIG
    except Exception as exc:
        print(f"REFUSED: tidak bisa mengakses spreadsheet/kredensial: "
              f"{type(exc).__name__}: {exc}")
        return EXIT_CONFIG

    # ── parse + template checks ──
    try:
        with upload_path.open("rb") as fh:
            parsed = parsers.parse_upload(fh)
    except parsers.ParseError as exc:
        print(f"REFUSED: {exc}")
        return EXIT_VALIDATION

    if args.type != "auto":
        expected = (parsers.UPLOAD_NOO if args.type == "noo"
                    else parsers.UPLOAD_SKU)
        wrong = parsers.check_template_kind(parsed, expected)
        if wrong:
            print(f"REFUSED: {wrong}")
            return EXIT_VALIDATION
    missing = parsers.missing_columns(parsed, parsed.kind)
    if missing:
        print("REFUSED: kolom wajib tidak ditemukan: " + ", ".join(missing))
        return EXIT_VALIDATION
    if not parsed.rows:
        print("REFUSED: file tidak berisi data.")
        return EXIT_VALIDATION

    tab = writer.pool_tab_for(parsed.kind)
    headers = writer.pool_headers_for(parsed.kind)

    # ── layout assert BEFORE anything else touches the pool ──
    try:
        writer.assert_layout(client, tab, headers)
    except writer.LayoutMismatch as exc:
        print(f"REFUSED: {exc}")
        return EXIT_CONFIG

    try:
        result = run_pipeline(args, settings, creds, project, client,
                              distributor, distributors, parsed)
    except SystemExit as exc:
        print(f"REFUSED: {exc}")
        return EXIT_VALIDATION

    print_summary(args, settings, distributor, parsed, result, tab)

    if result.errors:
        header("RESULT")
        print("STATUS: VALIDATION FAILED — tidak ada data yang ditulis.")
        write_report(args, settings, distributor, parsed, result, tab, None,
                     None, "VALIDATION_FAILED")
        return EXIT_VALIDATION

    if not result.eligible_rows:
        header("RESULT")
        print(f"STATUS: NOTHING TO WRITE — {result.message}")
        write_report(args, settings, distributor, parsed, result, tab, None,
                     None, "NOTHING_TO_WRITE")
        return EXIT_VALIDATION

    if settings.is_pilot and len(result.eligible_rows) > settings.pilot_max_rows:
        header("RESULT")
        print(f"REFUSED: mode PILOT hanya mengizinkan "
              f"{settings.pilot_max_rows} baris, tetapi "
              f"{len(result.eligible_rows)} baris memenuhi syarat.")
        write_report(args, settings, distributor, parsed, result, tab, None,
                     None, "PILOT_LIMIT_EXCEEDED")
        return EXIT_VALIDATION

    if settings.dry_run:
        header("RESULT")
        print(f"STATUS: DRY RUN OK — {len(result.eligible_rows)} baris siap "
              "ditulis, tidak ada data yang dikirim ke spreadsheet.")
        write_report(args, settings, distributor, parsed, result, tab, None,
                     None, "DRY_RUN_OK")
        return EXIT_OK

    if not confirm("Continue with write? [YES/NO]:", args.yes):
        header("RESULT")
        print("STATUS: ABORTED — tidak ada data yang ditulis.")
        write_report(args, settings, distributor, parsed, result, tab, None,
                     None, "ABORTED")
        return EXIT_ABORTED

    # ── write ──
    try:
        write_result = writer.append_rows(
            client, tab, result.eligible_rows, headers=headers,
            settings=settings, upload_id=result.upload_id)
    except (writer.LayoutMismatch, writer.PilotLimitExceeded) as exc:
        print(f"WRITE REFUSED: {exc}")
        return EXIT_WRITE_FAILED
    except Exception as exc:
        print(f"WRITE FAILED: {type(exc).__name__}: {exc}")
        write_report(args, settings, distributor, parsed, result, tab, None,
                     None, "WRITE_FAILED")
        return EXIT_WRITE_FAILED

    # ── verify ──
    header("WRITE VERIFICATION")
    verification = writer.verify_written(
        client, tab, headers, result.eligible_rows,
        input_time=result.pool_rows[0]["input_time"],
        distributor_code=distributor["distributor_code"])
    line("Rows Written:", str(write_result.rows_written))
    line("Rows Verified:", str(verification["verified"]))
    line("Verification:", "PASS" if verification["passed"] else "FAILED")
    if not verification["passed"]:
        print(f"Reason: hanya {verification['verified']} dari "
              f"{verification['expected']} baris yang ditemukan kembali di "
              f"'{tab}' dengan input_time="
              f"{result.pool_rows[0]['input_time']!r}.")
        print("Segera periksa spreadsheet dan lihat prosedur rollback di "
              "docs/streamlit_noo_sku_mapping_uat.md.")

    status = "SUCCESS" if verification["passed"] else "VERIFICATION_FAILED"
    header("BATCH REFERENCE (untuk rollback)")
    line("input_time:", result.pool_rows[0]["input_time"])
    line("customer_branch_code:", distributor["distributor_code"])
    line("Target Tab:", tab)

    write_report(args, settings, distributor, parsed, result, tab, write_result,
                 verification, status)
    return EXIT_OK if verification["passed"] else EXIT_VERIFY_FAILED


if __name__ == "__main__":
    sys.exit(main())
