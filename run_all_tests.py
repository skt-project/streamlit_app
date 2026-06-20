"""Single entry point for the assessment app's test suite.

Usage:
    python run_all_tests.py            # full suite
    python run_all_tests.py --sanity   # fast subset only (pytest -m sanity)

No BigQuery credentials or Streamlit secrets required — everything here runs
against pure functions in assessment_logic.py plus static syntax checks on the
two app files. See tests/test_assessment_logic.py for what's covered, and the
plan/changelog for what's explicitly NOT covered (integration/system tests
against live BigQuery — out of scope for this pass, needs real read-only
credentials and a designated test distributor).
"""
import sys
import subprocess


def main():
    args = ["-v", "--tb=short"]
    if "--sanity" in sys.argv:
        args += ["-m", "sanity"]

    try:
        import pytest_cov  # noqa: F401
        args += ["--cov=assessment_logic", "--cov-report=term-missing"]
    except ImportError:
        pass

    result = subprocess.run([sys.executable, "-m", "pytest", "tests/", *args])

    print("\n" + "=" * 60)
    if result.returncode == 0:
        print("ALL TESTS PASSED")
    else:
        print(f"TESTS FAILED (pytest exit code {result.returncode})")
    print("=" * 60)

    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
