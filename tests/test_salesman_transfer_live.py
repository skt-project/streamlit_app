"""Proof that the transfer SQL really is atomic -- against real BigQuery.

The hermetic suite (test_salesman_transfer.py) pins the SHAPE of the script. A
fake client cannot prove that BigQuery rolls a half-done transaction back, so
this module runs the real `build_transfer_script()` output on TEMP tables inside
one script job.

Safe by construction:
  * OPT-IN. Skipped unless SALESMAN_TRANSFER_LIVE_KEY points at a service-account
    JSON file. The path is read from the environment only; nothing is stored here.
  * Needs only job-creation + read rights: a read-only account is enough.
  * Writes NO production table. The mapping and HR tables are TEMP tables that
    exist only for the duration of each script; the HR one is created with the
    production table's schema and zero rows (`WHERE FALSE`), so the column list
    the script depends on is the real one.
  * Uses synthetic ids/names/amounts only. Reads no production data.

Run: SALESMAN_TRANSFER_LIVE_KEY=<path to key json> pytest tests/test_salesman_transfer_live.py -q
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import salesman_transfer as T  # noqa: E402

KEY = os.environ.get("SALESMAN_TRANSFER_LIVE_KEY", "")
pytestmark = pytest.mark.skipif(
    not (KEY and Path(KEY).is_file()),
    reason="live BigQuery proof is opt-in: set SALESMAN_TRANSFER_LIVE_KEY")

HR_SCHEMA_SOURCE = "skintific-data-warehouse.gt_schema.gt_master_salesman"
NOW = "2026-11-09 10:00:00"


@pytest.fixture(scope="module")
def client():
    from google.cloud import bigquery
    from google.oauth2 import service_account
    creds = service_account.Credentials.from_service_account_file(KEY)
    return bigquery.Client(credentials=creds, project="skintific-data-warehouse")


def _plan(**kw):
    base = dict(salesman_id="TSTDST901001", salesman_type="MIX", salesman_name="TEST SALESMAN",
                from_distributor="DST901", to_distributor="DST902",
                to_distributor_name="TEST DIST TWO", to_region="TEST REGION", to_asm="TEST ASM",
                spv={"nama_spv_internal": "NEW SPV", "nama_spv_external": None,
                     "nama_spv_internal_2": None})
    base.update(kw)
    return T.TransferPlan(**base)


SETUP = f"""
DECLARE err STRING DEFAULT '';
CREATE TEMP TABLE m (salesman_id STRING, salesman_type STRING, distributor_code STRING,
                     salesman STRING, is_active BOOL, created_at STRING, updated_at STRING);
CREATE TEMP TABLE h AS SELECT * FROM `{HR_SCHEMA_SOURCE}` WHERE FALSE;
INSERT INTO m VALUES
  ('TSTDST901001', 'MIX', 'DST901', 'TEST SALESMAN',  TRUE,  '2026-11-01 00:00:00', '2026-11-01 00:00:00'),
  ('TSTDST901002', 'MIX', 'DST901', 'OTHER SALESMAN', TRUE,  '2026-11-01 00:00:00', '2026-11-01 00:00:00'),
  ('GTIDST902001', 'GTI', 'DST902', 'TEST SALESMAN',  FALSE, '2026-10-01 00:00:00', '2026-10-02 00:00:00');
INSERT INTO h (nama_salesman, nama_spv_internal, kode_distributor, nama_distributor, region, asm,
               status_salesman, gaji_pokok, no_hp, uploaded_at)
VALUES ('TEST SALESMAN', 'OLD SPV', 'DST901', 'TEST DIST ONE', 'OLD REGION', 'OLD ASM',
        'Eksklusif', 1234567, '+6281000000000', '2026-10-09 08:21:33');
"""

STATE = """
SELECT 'map' AS t, salesman_id AS k, distributor_code AS d, CAST(is_active AS STRING) AS a,
       updated_at AS x, '' AS y FROM m
UNION ALL
SELECT 'hr', nama_salesman, kode_distributor, nama_spv_internal, uploaded_at,
       CONCAT(CAST(gaji_pokok AS STRING), '|', no_hp, '|', region, '|', asm, '|', nama_distributor) FROM h
UNION ALL SELECT 'err', err, '', '', '', ''
"""


def _run(client, *plans, setup=SETUP):
    """Run each transfer in turn inside ONE script, then read back the state."""
    from google.cloud import bigquery
    blocks, params = [], []
    for i, plan in enumerate(plans):
        script = T.build_transfer_script("m", "h")
        blocks.append(f"BEGIN\n{script}\nEXCEPTION WHEN ERROR THEN "
                      f"SET err = CONCAT(err, '|', @@error.message);\nEND;\n")
        if i == 0:
            params = T.build_transfer_params(plan, NOW)
    rows = list(client.query(
        setup + "".join(blocks) + STATE,
        job_config=bigquery.QueryJobConfig(query_parameters=params)).result())
    out = {"map": [], "hr": [], "err": ""}
    for r in rows:
        if r["t"] == "err":
            out["err"] = r["k"]
        else:
            out[r["t"]].append(dict(r.items()))
    return out


def _active(state, sid="TSTDST901001"):
    return sorted(r["d"] for r in state["map"] if r["k"] == sid and r["a"] == "true")


def _untouched(state):
    """The initial data: owner active at DST901, nothing at DST902, one HR row."""
    assert _active(state) == ["DST901"]
    assert not [r for r in state["map"] if r["d"] == "DST902" and r["k"] == "TSTDST901001"]
    assert [r["d"] for r in state["hr"]] == ["DST901"]
    old = next(r for r in state["map"] if r["k"] == "TSTDST901001")
    assert old["x"] == "2026-11-01 00:00:00"  # the UPDATE was rolled back too


def test_happy_path_keeps_the_id_and_moves_ownership(client):
    s = _run(client, _plan())
    assert s["err"] == ""
    assert _active(s) == ["DST902"]                                      # one owner, at the destination
    ids = [r for r in s["map"] if r["k"] == "TSTDST901001"]
    old = next(r for r in ids if r["d"] == "DST901")
    assert old["a"] == "false"                                            # retired, not deleted
    new = next(r for r in ids if r["d"] == "DST902")
    assert new["a"] == "true" and new["x"] == NOW                         # same ID, new owner
    decoy = next(r for r in s["map"] if r["k"] == "TSTDST901002")
    assert decoy["a"] == "true" and decoy["d"] == "DST901"                # neighbours untouched
    stale = next(r for r in s["map"] if r["k"] == "GTIDST902001")
    assert stale["a"] == "false"                                          # abandoned attempt untouched
    assert not [r for r in s["map"] if r["k"].startswith("TSTDST902")]    # no ID generated
    hr_old = next(r for r in s["hr"] if r["d"] == "DST901")
    hr_new = next(r for r in s["hr"] if r["d"] == "DST902")
    assert hr_old["a"] == "OLD SPV" and hr_old["x"] == "2026-10-09 08:21:33"   # history untouched
    assert hr_new["a"] == "NEW SPV"                                       # SPV override applied
    assert hr_new["y"] == "1234567|+6281000000000|TEST REGION|TEST ASM|TEST DIST TWO"  # copied + re-homed


def test_wrong_source_distributor_changes_nothing(client):
    s = _run(client, _plan(from_distributor="DST999"))
    assert "SOURCE_CHANGED" in s["err"]
    _untouched(s)


def test_missing_hr_row_rolls_the_mapping_back(client):
    """The partial-write case: the mapping UPDATE + INSERT have already run when
    the HR step fails. BigQuery must undo both."""
    s = _run(client, _plan(salesman_name="NO SUCH HR ROW"))
    assert "HR_MISSING" in s["err"]
    _untouched(s)
    assert not [r for r in s["map"] if r["d"] == "DST902" and r["a"] == "true"]


def test_double_submit_cannot_create_a_second_owner(client):
    s = _run(client, _plan(), _plan())
    assert "SOURCE_CHANGED" in s["err"]
    assert _active(s) == ["DST902"]


def test_an_id_already_active_elsewhere_is_never_made_active_twice(client):
    """Corrupt starting state: the ID is active at DST901 AND DST903. The move would
    leave two active owners, so the post-condition must abort and roll back."""
    corrupt = SETUP + ("INSERT INTO m VALUES ('TSTDST901001', 'MIX', 'DST903', 'TEST SALESMAN', "
                       "TRUE, '2026-11-02 00:00:00', '2026-11-02 00:00:00');\n")
    s = _run(client, _plan(), setup=corrupt)
    assert "POSTCONDITION" in s["err"]
    assert _active(s) == ["DST901", "DST903"]      # exactly as it was; nothing half-applied
    assert not [r for r in s["map"] if r["k"] == "TSTDST901001" and r["d"] == "DST902"]
