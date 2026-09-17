"""Generate the `[admin]` secrets block for the PJP admin login.

Admin credentials are never stored in Python source and never committed. This
script turns a password you type into a bcrypt hash and prints the TOML block
to paste into the deployment's secrets:

* locally     -> .streamlit/secrets.toml   (already in .gitignore)
* Streamlit Cloud -> App settings > Secrets
* Cloud Run   -> the mounted secrets.toml / Secret Manager entry

The password is read with getpass, so it is never echoed, never stored in shell
history, and never written to disk. Only the hash is printed.

Usage:
    python scripts/set_pjp_admin_password.py
    python scripts/set_pjp_admin_password.py --username ops.admin
"""
from __future__ import annotations

import argparse
import sys
from getpass import getpass
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import pjp_auth  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--username", default="admin",
                        help="Admin username (default: admin).")
    args = parser.parse_args()

    password = getpass(f"Password baru untuk admin '{args.username}': ")
    confirm = getpass("Ulangi password: ")

    check = pjp_auth.validate_new_password(password, confirm)
    if not check.ok:
        print(f"GAGAL: {check.message}")
        return 1

    hashed = pjp_auth.hash_password(password)
    del password, confirm

    print()
    print("Tambahkan blok berikut ke secrets deployment "
          "(.streamlit/secrets.toml lokal sudah di-gitignore):")
    print()
    print("[admin]")
    print(f'username = "{args.username}"')
    print(f'password_hash = "{hashed}"')
    print()
    print("Untuk beberapa admin, gunakan:")
    print()
    print("[admin]")
    print("users = [")
    print(f'  {{ username = "{args.username}", password_hash = "{hashed}" }},')
    print("]")
    print()
    print("JANGAN commit file secrets.toml.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
