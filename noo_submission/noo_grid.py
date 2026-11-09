import numpy as np
import pandas as pd
import streamlit as st

_HTML = '<div class="root"></div>'

_CSS = """
:host { display:block; font-family: "Source Sans", "Source Sans Pro", ui-sans-serif, system-ui, sans-serif; }
* { box-sizing: border-box; }
.wrap { overflow:auto; max-height:640px; border:1px solid #e3e9f4; border-radius:8px; background:#fff; }
table { border-collapse:separate; border-spacing:0; table-layout:fixed; font-size:13px; color:#1f2937; }
th, td { height:38px; padding:0; border-bottom:1px solid #eef2f8; border-right:1px solid #f1f4f9;
         background:#fff; white-space:nowrap; overflow:hidden; }
th { position:sticky; top:0; z-index:2; background:#f7f9fd; font-size:12px; font-weight:600; color:#4b5a78;
     text-align:left; padding:0 10px; height:36px; }
td.num, td.ro { padding:0 10px; }
td.num { text-align:right; color:#6b7280; }
td.ro  { color:#6b7280; }
td.chk { text-align:center; }
input[type=checkbox] { width:16px; height:16px; cursor:pointer; vertical-align:middle; }
input.cell, select.cell { width:100%; height:100%; border:0; outline:0; background:transparent; font:inherit;
                          color:inherit; padding:0 10px; }
input.cell:focus, select.cell:focus { box-shadow: inset 0 0 0 2px #3b82f6; background:#fff; }
/* Validation: dropdown berwarna + panah supaya jelas bisa diklik */
select.val { appearance:none; -webkit-appearance:none; font-weight:600; cursor:pointer; padding-right:26px;
  background-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'><path d='M2 4l4 4 4-4' fill='none' stroke='%23555' stroke-width='1.8' stroke-linecap='round'/></svg>");
  background-repeat:no-repeat; background-position:right 8px center; }
select.val:focus { background-color:inherit; }
select.cell:not(.val) { cursor:pointer; }
/* Store ID */
td.sid-on  { background:#fff; box-shadow: inset 0 0 0 2px #93c5fd; }
td.sid-on input::placeholder { color:#3b82f6; font-style:italic; }
td.sid-off { background:#f3f4f6; }
td.sid-off input { color:#9ca3af; cursor:not-allowed; }
tr:hover td:not(.sid-off):not(.sid-on) { background:#fafcff; }
tr.picked td:not(.sid-off):not(.sid-on) { background:#eef4ff; }
"""

_JS = """
export default function (component) {
  const { data, parentElement, setStateValue } = component;
  const root = parentElement.querySelector('.root');
  const S = (parentElement.__noo = parentElement.__noo || { edits: {}, tok: null });

  // Select All dari luar berubah -> buang centang manual
  if (S.tok !== data.sel_token) {
    for (const k in S.edits) { if (S.edits[k].Pilih !== undefined) delete S.edits[k].Pilih; }
    S.tok = data.sel_token;
  }

  const cols = data.cols, rows = data.rows, warna = data.warna || {};
  const esc = (s) => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  const val = (i, c) => (S.edits[i] && S.edits[i][c] !== undefined) ? S.edits[i][c] : rows[i][c];

  function commit(i, c, v) {
    (S.edits[i] = S.edits[i] || {})[c] = v;
    render();
    setStateValue('edits', JSON.parse(JSON.stringify(S.edits)));
  }

  function render() {
    // Lebar kolom otomatis mengikuti isi terpanjang (header / nilai), min = c.width*0.6, maks 380px
    const CH = 7.4;
    const W = cols.map(c => {
      if (c.kind === 'index' || c.kind === 'check') return c.width;
      let m = String(c.label).length + 2;
      rows.forEach((r, i) => { m = Math.max(m, String(val(i, c.name) ?? '').length); });
      if (c.kind === 'status') m = Math.max(m, ...c.options.map(o => o.length));
      if (c.kind === 'select') m = Math.max(m, ...rows.map((r, i) => String(val(i, c.name) ?? '').length));
      const extra = (c.kind === 'status' || c.kind === 'select') ? 48 : 28;
      return Math.round(Math.min(380, Math.max(c.width * 0.6, m * CH + extra)));
    });
    const total = W.reduce((a, b) => a + b, 0);
    let h = `<div class="wrap"><table style="width:${total}px"><colgroup>` + W.map(w => `<col style="width:${w}px">`).join('') + '</colgroup><thead><tr>';
    h += cols.map(c => `<th title="${esc(c.help || '')}">${esc(c.label)}</th>`).join('') + '</tr></thead><tbody>';
    rows.forEach((r, i) => {
      const status = val(i, 'Validasi');
      const picked = !!val(i, 'Pilih');
      h += `<tr class="${picked ? 'picked' : ''}">`;
      cols.forEach(c => {
        const v = val(i, c.name);
        if (c.kind === 'index') h += `<td class="num">${esc(v)}</td>`;
        else if (c.kind === 'check') h += `<td class="chk"><input type="checkbox" data-i="${i}" data-c="${c.name}" ${v ? 'checked' : ''}></td>`;
        else if (c.kind === 'status') {
          h += `<td><select class="cell val" data-i="${i}" data-c="${c.name}" style="${esc(warna[v] || '')}">` +
               c.options.map(o => `<option ${o === v ? 'selected' : ''}>${esc(o)}</option>`).join('') + '</select></td>';
        } else if (c.kind === 'select') {
          const opts = c.options.includes(v) || v == null ? c.options : [v, ...c.options];
          h += `<td><select class="cell" data-i="${i}" data-c="${c.name}">` +
               opts.map(o => `<option ${o === v ? 'selected' : ''}>${esc(o)}</option>`).join('') + '</select></td>';
        } else if (c.kind === 'storeid') {
          const on = status === 'Existing';
          const shown = on && v === '-' ? '' : v;
          h += `<td class="${on ? 'sid-on' : 'sid-off'}"><input class="cell" data-i="${i}" data-c="${c.name}" value="${esc(shown)}" ` +
               (on ? 'placeholder="Type Store ID"' : 'disabled title="Editable only when Validation = Existing"') + '></td>';
        } else if (c.kind === 'number') {
          h += `<td><input class="cell" type="number" step="0.0001" data-i="${i}" data-c="${c.name}" value="${esc(v)}" style="text-align:right"></td>`;
        } else if (c.kind === 'text') {
          h += `<td><input class="cell" data-i="${i}" data-c="${c.name}" value="${esc(v)}"></td>`;
        } else {
          h += `<td class="ro">${esc(v)}</td>`;
        }
      });
      h += '</tr>';
    });
    root.innerHTML = h + '</tbody></table></div>';

    root.querySelectorAll('input[type=checkbox]').forEach(el => el.onchange = () => commit(+el.dataset.i, el.dataset.c, el.checked));
    root.querySelectorAll('select').forEach(el => el.onchange = () => commit(+el.dataset.i, el.dataset.c, el.value));
    root.querySelectorAll('input.cell').forEach(el => el.onchange = () => {
      const c = cols.find(x => x.name === el.dataset.c);
      commit(+el.dataset.i, el.dataset.c, c.kind === 'number' ? (el.value === '' ? null : parseFloat(el.value)) : el.value);
    });
  }
  render();
}
"""

_komponen = st.components.v2.component("noo_grid", html=_HTML, css=_CSS, js=_JS)


def _js(v):
    """Nilai pandas -> nilai JSON-aman."""
    if v is None or (not isinstance(v, (list, dict)) and pd.isna(v)):
        return None
    if isinstance(v, (pd.Timestamp,)):
        return v.strftime("%Y-%m-%d %H:%M")
    if isinstance(v, (np.bool_, bool)):
        return bool(v)
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v)
    return str(v) if not isinstance(v, (int, float, str)) else v


def noo_grid(df: pd.DataFrame, cols: list, *, key: str, select_all=False, warna=None) -> pd.DataFrame:
    """Render tabel; kembalikan df + edit user (indeks sama dengan df).

    cols: list dict {name, label, kind, width, options?, help?};
          kind = index | check | status | select | storeid | number | text | ro
    """
    nama = [c["name"] for c in cols]
    rows = [{n: _js(v) for n, v in zip(nama, tup)}
            for tup in df[nama].itertuples(index=False, name=None)]
    for r in rows:                                    # NaN teks -> "" supaya input tidak tampil 'None'
        for c in cols:
            if r[c["name"]] is None and c["kind"] in ("text", "storeid"):
                r[c["name"]] = ""
    hasil = _komponen(
        key=key,
        data={"cols": cols, "rows": rows, "warna": warna or {}, "sel_token": bool(select_all)},
        default={"edits": {}},
        on_edits_change=lambda: None,
    )
    edits = (hasil.edits if hasattr(hasil, "edits") else None) or {}

    out = df.copy()
    n = len(out)
    for pos, chg in edits.items():
        try:
            p = int(pos)
        except (TypeError, ValueError):
            continue
        if not 0 <= p < n:
            continue
        idx = out.index[p]
        for c, v in chg.items():
            if c not in out.columns:
                continue
            if c in ("Longitude", "Latitude"):
                v = np.nan if v in (None, "") else float(v)
            elif c == "Pilih":
                v = bool(v)
            else:
                v = "" if v is None else str(v)
            out.at[idx, c] = v
    return out
