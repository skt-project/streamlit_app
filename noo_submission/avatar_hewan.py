"""Avatar hewan kawaii (SVG orisinal). Satu hewan per user, dipilih dari hash nama."""
# Avatar hewan bergaya kawaii (SVG orisinal, digambar sendiri). Satu hewan per user, dipilih dari hash nama.
_WAJAH = ('<circle cx="24" cy="38" r="2.6" fill="#2b2233"/><circle cx="40" cy="38" r="2.6" fill="#2b2233"/>'
          '<circle cx="25" cy="37" r="0.9" fill="#fff"/><circle cx="41" cy="37" r="0.9" fill="#fff"/>'
          '<ellipse cx="19" cy="44" rx="3.6" ry="2.2" fill="#ff9fb8" opacity=".8"/>'
          '<ellipse cx="45" cy="44" rx="3.6" ry="2.2" fill="#ff9fb8" opacity=".8"/>'
          '<path d="M29 44q3 3 6 0" stroke="#2b2233" stroke-width="1.6" fill="none" stroke-linecap="round"/>')
_HEWAN_SVG = {
    "cat": '<path d="M13 28L15 8L28 19Z" fill="#ffc98a"/><path d="M51 28L49 8L36 19Z" fill="#ffc98a"/>'
           '<path d="M16 24L17 14L24 20Z" fill="#ffadc2"/><path d="M48 24L47 14L40 20Z" fill="#ffadc2"/>'
           '<circle cx="32" cy="38" r="21" fill="#ffd9a8"/>' + _WAJAH +
           '<path d="M6 40h9M7 46l8-2M58 40h-9M57 46l-8-2" stroke="#b08a6a" stroke-width="1.2" stroke-linecap="round"/>',
    "bear": '<circle cx="14" cy="21" r="8" fill="#b9825a"/><circle cx="50" cy="21" r="8" fill="#b9825a"/>'
            '<circle cx="14" cy="21" r="4" fill="#e8b48f"/><circle cx="50" cy="21" r="4" fill="#e8b48f"/>'
            '<circle cx="32" cy="38" r="21" fill="#c99468"/><ellipse cx="32" cy="45" rx="9" ry="7" fill="#f1d3b3"/>'
            + _WAJAH.replace('<path d="M29 44q3 3 6 0"', '<ellipse cx="32" cy="42" rx="2.4" ry="1.8" fill="#3b2a2a"/><path d="M29 46q3 3 6 0"'),
    "rabbit": '<ellipse cx="22" cy="14" rx="6" ry="14" fill="#fff"/><ellipse cx="42" cy="14" rx="6" ry="14" fill="#fff"/>'
              '<ellipse cx="22" cy="15" rx="3" ry="9" fill="#ffc2d4"/><ellipse cx="42" cy="15" rx="3" ry="9" fill="#ffc2d4"/>'
              '<circle cx="32" cy="40" r="20" fill="#fff" stroke="#f1e6ee" stroke-width="1"/>' + _WAJAH,
    "panda": '<circle cx="14" cy="21" r="8" fill="#2b2233"/><circle cx="50" cy="21" r="8" fill="#2b2233"/>'
             '<circle cx="32" cy="38" r="21" fill="#fff" stroke="#eee" stroke-width="1"/>'
             '<ellipse cx="23" cy="37" rx="6" ry="7" fill="#2b2233" transform="rotate(-20 23 37)"/>'
             '<ellipse cx="41" cy="37" rx="6" ry="7" fill="#2b2233" transform="rotate(20 41 37)"/>'
             '<circle cx="24" cy="37" r="2.4" fill="#fff"/><circle cx="40" cy="37" r="2.4" fill="#fff"/>'
             '<circle cx="24.4" cy="37.2" r="1.2" fill="#2b2233"/><circle cx="39.6" cy="37.2" r="1.2" fill="#2b2233"/>'
             '<ellipse cx="32" cy="44" rx="2.4" ry="1.8" fill="#2b2233"/>'
             '<path d="M29 47q3 3 6 0" stroke="#2b2233" stroke-width="1.6" fill="none" stroke-linecap="round"/>'
             '<ellipse cx="18" cy="47" rx="3.4" ry="2" fill="#ff9fb8" opacity=".7"/><ellipse cx="46" cy="47" rx="3.4" ry="2" fill="#ff9fb8" opacity=".7"/>',
    "dog": '<ellipse cx="11" cy="32" rx="7" ry="13" fill="#9a6b45" transform="rotate(15 11 32)"/>'
           '<ellipse cx="53" cy="32" rx="7" ry="13" fill="#9a6b45" transform="rotate(-15 53 32)"/>'
           '<circle cx="32" cy="38" r="20" fill="#f3d3a8"/><ellipse cx="32" cy="46" rx="9" ry="7" fill="#fff3e0"/>'
           + _WAJAH.replace('<path d="M29 44q3 3 6 0"', '<ellipse cx="32" cy="42" rx="3" ry="2.2" fill="#2b2233"/><path d="M29 47q3 3 6 0"'),
    "frog": '<circle cx="19" cy="20" r="9" fill="#7ed37e"/><circle cx="45" cy="20" r="9" fill="#7ed37e"/>'
            '<circle cx="19" cy="20" r="5" fill="#fff"/><circle cx="45" cy="20" r="5" fill="#fff"/>'
            '<circle cx="19" cy="20.5" r="2.6" fill="#2b2233"/><circle cx="45" cy="20.5" r="2.6" fill="#2b2233"/>'
            '<ellipse cx="32" cy="42" rx="22" ry="17" fill="#8fdc8f"/>'
            '<ellipse cx="17" cy="45" rx="3.6" ry="2.2" fill="#ff9fb8" opacity=".8"/><ellipse cx="47" cy="45" rx="3.6" ry="2.2" fill="#ff9fb8" opacity=".8"/>'
            '<path d="M24 46q8 7 16 0" stroke="#2b2233" stroke-width="1.8" fill="none" stroke-linecap="round"/>'
            '<circle cx="29" cy="38" r="1" fill="#4a9a4a"/><circle cx="35" cy="38" r="1" fill="#4a9a4a"/>',
    "penguin": '<circle cx="32" cy="36" r="23" fill="#3a3550"/><ellipse cx="32" cy="42" rx="16" ry="15" fill="#fff"/>'
               '<circle cx="24" cy="36" r="2.6" fill="#2b2233"/><circle cx="40" cy="36" r="2.6" fill="#2b2233"/>'
               '<circle cx="25" cy="35" r="0.9" fill="#fff"/><circle cx="41" cy="35" r="0.9" fill="#fff"/>'
               '<path d="M27 41h10l-5 6Z" fill="#ffb347"/>'
               '<ellipse cx="19" cy="42" rx="3.4" ry="2" fill="#ff9fb8" opacity=".8"/><ellipse cx="45" cy="42" rx="3.4" ry="2" fill="#ff9fb8" opacity=".8"/>',
    "chick": '<path d="M32 10q-5 -3 -6 2q4 0 6 4q2 -4 6 -4q-1 -5 -6 -2Z" fill="#ffd23f"/>'
             '<circle cx="32" cy="38" r="22" fill="#ffe066"/>' + _WAJAH.replace('<path d="M29 44q3 3 6 0" stroke="#2b2233" stroke-width="1.6" fill="none" stroke-linecap="round"/>', '<path d="M28 42h8l-4 5Z" fill="#ff9f43"/>') +
             '<path d="M9 36q-4 8 3 11M55 36q4 8 -3 11" stroke="#f5c518" stroke-width="3" fill="none" stroke-linecap="round"/>',
}
_HEWAN_URUT = list(_HEWAN_SVG)


def _hewan_avatar(nama):
    # hewan kawaii acak tapi KONSISTEN per user (dipilih dari hash nama), jadi user yang sama selalu dapat hewan yang sama
    import zlib
    h = _HEWAN_URUT[zlib.crc32(str(nama).encode("utf-8")) % len(_HEWAN_URUT)]
    return f'<svg width="34" height="34" viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg">{_HEWAN_SVG[h]}</svg>'
