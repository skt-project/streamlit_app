"""Demo visual for the updated PI Validation one-to-many flow.

Run:
    streamlit run demo_pi_one_to_many.py
"""
from __future__ import annotations

from po_pi_map import plan_pi_mapping, render_ref_badges_html
import streamlit as st

st.set_page_config(page_title="Demo PI Validation one-to-many", layout="wide")

CASES = [
    {
        "distributor": "AGUNG TERNATE",
        "file_name": "1ST PO Glad2glow Agung Ternate.xlsx",
        "refs": [
            "G2G_AGUNG_TERNATE_26",
            "G2G_AGUNG_TERNATE_26-1",
            "G2G_AGUNG_TERNATE_26-2",
        ],
    },
    {
        "distributor": "CV FNB PRATAMA",
        "file_name": "FNB G2G PO.xlsx",
        "refs": ["G2G_FNB_PRATAMA_11", "G2G_FNB_PRATAMA_11-1"],
    },
]

st.title("Demo PI Validation one-to-many")
st.caption(
    "Primary hijau = kolom utama. Nomor lain yang dicentang masuk tabel link. "
    "Demo lokal, tidak menulis BigQuery."
)

pending = []
for i, case in enumerate(CASES):
    with st.container(border=True):
        st.markdown(f"**{case['distributor']}** · `{case['file_name']}`")
        st.write(f"Jumlah Ref: **{len(case['refs'])}**")
        selected = st.multiselect(
            "PI yang di-map",
            options=case["refs"],
            default=case["refs"],
            key=f"demo_refs_{i}",
        )
        primary = st.selectbox(
            "Primary PI",
            options=selected or ["(centang dulu)"],
            disabled=not selected,
            key=f"demo_primary_{i}",
        )
        st.markdown(
            render_ref_badges_html(selected, primary if selected else None),
            unsafe_allow_html=True,
        )
        note = st.selectbox(
            "Validation by RSA",
            options=["Mapping PI by RSA", "Others"],
            key=f"demo_note_{i}",
        )
        pending.append((case, selected, primary if selected else "", note))

if st.button("Save PI Validation", type="primary"):
    rows = []
    links = []
    for case, selected, primary, note in pending:
        plan = plan_pi_mapping(
            case["distributor"],
            case["file_name"],
            case["refs"],
            selected,
            primary,
            note,
        )
        if not plan.ok:
            for message in plan.errors:
                st.error(message)
            continue
        sel = plan.selection
        rows.append(
            {
                "distributor": sel.distributor_name,
                "file_name": sel.file_name,
                "reference_cust_order_no": sel.primary_ref,
                "validation_by_rsa": sel.validation_by_rsa,
            }
        )
        for ref in sel.selected_refs:
            links.append(
                {
                    "distributor": sel.distributor_name,
                    "file_name": sel.file_name,
                    "reference_cust_order_no": ref,
                    "is_primary": ref == sel.primary_ref,
                    "status": "CONFIRMED",
                }
            )
    if rows:
        st.subheader("Tabel utama (primary saja)")
        st.dataframe(rows, use_container_width=True, hide_index=True)
        st.subheader("Tabel link po_supply_control_g2g_pi_map")
        st.dataframe(links, use_container_width=True, hide_index=True)
