"""
Authentication layer: per-distributor password gate + monthly deadline lock.

Passwords come exclusively from st.secrets["distributor_passwords"], e.g.:
  [distributor_passwords]
  DST171 = "my_password"
  DST157 = "another_password"

The monthly deadline comes from st.secrets["pjp_input_deadline"] (ISO date string).
"""

import streamlit as st
from .config import get_input_deadline
from datetime import datetime


# ─── Password helpers ──────────────────────────────────────────────────────────

def _get_password(dist_code: str) -> str | None:
    try:
        passwords = st.secrets.get("distributor_passwords", {})
        return passwords.get(str(dist_code).strip().upper())
    except Exception:
        return None


def is_authenticated(dist_code: str) -> bool:
    return st.session_state.get(f"v2_auth_{dist_code}", False)


def render_password_gate(dist_code: str, dist_name: str) -> bool:
    """
    Show a password prompt if not yet authenticated for this distributor.
    Returns True if the user is authenticated (either already or just now).
    """
    if is_authenticated(dist_code):
        return True

    expected = _get_password(dist_code)

    st.markdown(
        f"""
        <div style='text-align:center; padding:2rem 0 1rem 0;'>
            <span style='font-size:2.5rem'>🔒</span>
            <h3 style='margin:0.5rem 0 0.25rem 0;'>Akses Terkunci</h3>
            <p style='color:#888; margin:0;'>Masukkan password untuk distributor
            <b>{dist_name}</b> ({dist_code})</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        entered = st.text_input(
            "Password Distributor",
            type="password",
            key=f"v2_pw_input_{dist_code}",
            placeholder="••••••••",
        )
        if st.button("🔓 Masuk", key=f"v2_pw_btn_{dist_code}", type="primary", use_container_width=True):
            if expected is None:
                st.error("Password untuk distributor ini belum dikonfigurasi. Hubungi administrator.")
            elif entered == expected:
                st.session_state[f"v2_auth_{dist_code}"] = True
                st.rerun()
            else:
                st.error("Password salah. Silakan coba lagi.")

    return False


# ─── Period deadline lock ──────────────────────────────────────────────────────

def render_deadline_gate() -> bool:
    """
    Show a locked-period screen if today is past the monthly submission deadline.
    Returns True if the period is still open.
    Deadline is read from st.secrets["pjp_input_deadline"] — changeable without deploy.
    """
    deadline = get_input_deadline()
    today = datetime.now().date()

    if today <= deadline:
        return True

    st.markdown(
        f"""
        <div style='display:flex; flex-direction:column; align-items:center;
                    justify-content:center; padding:5rem 2rem; text-align:center;'>
            <div style='font-size:4rem; line-height:1; margin-bottom:1.5rem;'>🔒</div>
            <h2 style='margin:0 0 0.75rem 0; font-size:1.75rem; font-weight:600;'>
                Periode Input Sudah Ditutup
            </h2>
            <p style='color:#888; margin:0 0 0.5rem 0; font-size:1rem; max-width:420px;'>
                Batas akhir pengisian adalah
                <b>{deadline.strftime("%d %B %Y")}</b>.
            </p>
            <p style='color:#aaa; margin:0; font-size:0.9rem;'>
                Hubungi tim G2G jika ada pertanyaan.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    return False
