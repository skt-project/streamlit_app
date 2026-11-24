# Isi tutorial (bahasa Indonesia) + tampilan mini UI aplikasi (HTML/CSS beranimasi). Ubah teks / urutan langkah di sini.
# Format langkah: (nama tampilan, judul, isi markdown). Nama tampilan = kunci di dict ILUSTRASI (bagian bawah file).

TUTORIAL = {
    "login": ("Memulai", [
        ("login", "Masuk ke akun",
         """1. Isi **Username** dan **Password**.
2. Centang **Remember me** (opsional) supaya tetap masuk selama 1 hari di browser ini.
3. Klik **Login**.

Catatan: akun distributor memakai **kode distributor** sebagai username."""),
        ("daftar", "Belum punya akun?",
         """Klik **Sign Up**, lalu:

1. Isi **Username**: 3-20 karakter (huruf, angka, `_` atau `.`) *WAJIB DIISI KODE DISTRI MASING-MASING
2. Isi **Email**.
3. Isi **Password**: minimal 8 karakter, ketik 2 kali.
4. Klik **Create Account**, lalu login dengan akun baru."""),
        ("login_bawah", "Butuh bantuan?",
         """1. **Forgot password?** Hubungi admin untuk reset password.
2. **Help & Support** untuk mengirim tiket ke tim, bahkan sebelum login.
3. **Tutorial** untuk membuka panduan ini lagi."""),
    ]),
    "intro": ("Selamat datang", [
        ("topbar_tutorial", "Selamat datang!",
         """1. Tur singkat ini menjelaskan cara memakai **NOO & SKU Submission**.
2. Kapan saja Anda bisa membukanya lagi lewat tombol **Tutorial** di kanan atas."""),
        ("menu", "Gunakan menu di sebelah kiri",
         """1. **NOO & SKU Submission**: kirim outlet baru (NOO) dan SKU Mapping.
2. **SPV Confirmation**: konfirmasi tipe outlet dan validasi data.
3. **History**: lihat data yang sudah dikirim, per bulan."""),
        ("topbar_help", "Ada kendala?",
         """1. Klik **Help & Support** di kanan atas.
2. Ceritakan masalahnya, lalu kirim tiketnya.
3. Tim kami akan menindaklanjuti."""),
        ("bintang", "Beri penilaian",
         """1. Setelah submit, form singkat **Feedback** mungkin muncul.
2. Beri bintang dan isi masukan Anda.
3. Penilaian Anda membantu kami membuat aplikasi ini lebih mudah dipakai."""),
    ]),
    "pengajuan": ("NOO & SKU Submission", [
        ("upload", "1. Upload Failure List",
         """1. Upload Failure List (**.xlsx / .xls**, maks 10 MB per file). Boleh beberapa file sekaligus.
2. Tabel NOO dan SKU Mapping otomatis dibuat dari isi file.
3. Hasilnya muncul di bagian **Upload Option**."""),
        ("sku_manual", "2. SKU Mapping: Manual",
         """1. Pilih **Distributor** dan **Product**.
2. Isi **Customer Product Code** (wajib).
3. Klik **Add Row**.
4. Setelah semua baris siap, klik **Save All to Supabase**.

Banyak baris? Pakai **Paste All SKU**."""),
        ("sku_same", "Fitur Same as Product > Jika Sama Product ID nya",
         """**Customer Product Code** = kode produk menurut distributor.

1. Kalau kodenya **sama persis** dengan Product, centang **Same as Product**: kolom Customer Product Code terisi otomatis dari Product dan dikunci.
2. Kalau kodenya **berbeda**, hilangkan centang lalu ketik kodenya sendiri."""),
        ("sku_paste", "Paste All SKU (banyak baris sekaligus)",
         """Buka panel **Paste All SKU** kalau barisnya banyak.

**Step 1**
1. Pilih **Distributor**.
2. Paste **Product** (kiri) dan **Customer Product Code** (kanan), satu per baris. Jumlah barisnya harus sama, atau centang **Same as Product**.
3. Klik **Apply Paste**.

**Step 2**
1. Cek kolom **Match**.
2. Perbaiki Product yang salah lewat dropdown *Product (fix here)*.
3. Klik **Add to Staged**, lalu simpan dengan **Save All to Supabase**."""),
        ("sku_upload", "3. SKU Mapping: Upload Option",
         """Baris hasil ekstraksi file muncul di tabel. Yang perlu Anda isi atau cek:

1. **Select**: centang baris yang mau dikirim (**All** untuk semua).
2. **Product**: otomatis terisi kalau cocok. Kalau kosong, pilih dari **Product Description** atau **Product**.
3. Kalau muncul peringatan prefix **Customer Code** (G2G- harus 12, lainnya 17), klik **Fix Customer Code**.
4. Klik **Submit SKU Mapping**. Tombol terkunci sampai semua baris terpilih punya Product."""),
        ("noo", "4. NOO: Manual",
         """1. Paste **Customer Store Code** dan **Store Name** (satu per baris).
2. Klik **Apply Paste**.
3. Pilih distributor dan lengkapi detail tiap outlet.
4. Klik **Submit Manual NOO**."""),
        ("noo_upload", "5. NOO: Upload Option",
         """Outlet dari file muncul di tabel. Kolom bertanda bintang (*) wajib diisi:

1. **Customer Store Name**: otomatis dari Store Name, boleh diedit.
2. **City**, **Store Address**, **SPV**, **Sales Name**, **Store Type**, dan **Channel**.
3. Klik **Submit NOO**. Tombol aktif setelah semua kolom wajib terisi."""),
        ("apply_all", "Fitur Apply to all",
         """Di atas tabel NOO ada baris **helper (Apply to all..)**.

1. Pilih satu nilai (misalnya **City**, **SPV**, **Sales Name**, atau **Store Type**).
2. Nilai itu langsung diisikan ke **semua baris** yang punya pilihan tersebut. Hemat waktu kalau banyak outlet dengan isian sama.
3. Setelah itu tiap baris tetap bisa diubah satu per satu."""),
        ("kirim", "6. Submit",
         """1. Tombol submit aktif kalau data sudah lengkap.
2. Setelah submit, hasilnya bisa dipantau di **History**."""),
    ]),
    "spv": ("SPV Confirmation", [
        ("spv", "Untuk apa halaman ini?",
         """1. Mengonfirmasi **tipe outlet** setiap outlet NOO.
2. Memvalidasi datanya sebelum dikirim."""),
        ("distri", "1. Pilih distributor",
         """1. Pilih distributor di bagian atas.
2. Akun yang terhubung ke satu distributor sudah otomatis terkunci ke distributornya."""),
        ("tinjau", "2. Periksa tiap outlet",
         """1. Bandingkan data outlet dengan kolom **Similarity** (Name, Distributor, Code, Address, City, Region): ini outlet mirip yang sudah ada di master.
2. Pilih **NOO / Existing** untuk tiap outlet.
3. Klik **Refresh** untuk memuat data terbaru."""),
        ("spv_sim", "3. Existing dan rekomendasi Store ID",
         """1. Kalau outlet **Existing** dan kolom **Similarity (Code)** terisi, tombol **Auto-fill Store ID from Similarity** muncul di atas tabel.
2. Klik sekali untuk mengisi **Store ID** semua baris Existing dengan kode rekomendasinya.
3. Store ID tetap bisa diganti manual lewat dropdown *Store ID - Store Name*."""),
        ("konfirmasi", "4. Submit Confirmation",
         """1. Klik **Submit Confirmation**.
2. Outlet yang sudah dikonfirmasi hilang dari daftar."""),
    ]),
}


# ------------------------------------------------------------------ CSS tampilan mini UI
CSS_ILUSTRASI = """
@keyframes mk-pulse{0%,100%{box-shadow:0 0 0 2px rgba(255,79,139,.18)}50%{box-shadow:0 0 0 6px rgba(255,79,139,.30)}}
@keyframes mk-tap{0%,55%,100%{transform:translate(0,0)}75%{transform:translate(-3px,-3px) scale(.9)}}
@keyframes mk-cur{0%{transform:translate(26px,22px);opacity:0}35%{opacity:1}70%,100%{transform:translate(0,0);opacity:1}}
@keyframes mk-nav{0%,28%{background:#ffe3ed;color:#c2185b}34%,100%{background:#fff;color:#31333f}}
@keyframes mk-star{0%{transform:scale(0);opacity:0}60%{transform:scale(1.3);opacity:1}100%{transform:scale(1);opacity:1}}
@keyframes mk-pop{0%{transform:scale(.6);opacity:0}70%{transform:scale(1.08);opacity:1}100%{transform:scale(1);opacity:1}}
@keyframes mk-drop{0%{transform:translateY(-16px);opacity:0}50%,100%{transform:translateY(0);opacity:1}}
@keyframes mk-open{0%{transform:scaleY(0);opacity:0}40%,100%{transform:scaleY(1);opacity:1}}
.mk{border:1px solid #e6e8ee;border-radius:12px;background:#fff;padding:12px 14px;font-size:11.5px;color:#31333f;
 box-shadow:0 2px 10px rgba(0,0,0,.05);margin:2px 0 12px;position:relative;overflow:hidden;
 font-family:"Source Sans","Source Sans Pro",ui-sans-serif,system-ui,sans-serif}
.mk *{box-sizing:border-box;font-family:inherit}
.mk .l{font-size:10.5px;color:#555a66;margin:0 0 3px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mk .in{border:1px solid #d6d9e0;background:#f6f7fa;border-radius:7px;padding:6px 9px;color:#808495;font-size:11px;min-height:28px;
 display:flex;align-items:center;justify-content:space-between;gap:6px;position:relative}
.mk .in.w{background:#fff}
.mk .dim{opacity:.55}
.mk .bt{border:1px solid #d6d9e0;border-radius:7px;padding:6px 10px;text-align:center;font-weight:700;background:#fff;
 font-size:11px;min-height:28px;display:flex;align-items:center;justify-content:center;position:relative;white-space:nowrap}
.mk .bt.p{background:#ff4f7b;border-color:#ff4f7b;color:#fff}
.mk .r{display:flex;gap:8px;align-items:flex-end;margin-bottom:8px}
.mk .r>div{min-width:0}
.mk .hl{border-color:#ff4f8b !important;animation:mk-pulse 1.8s ease-in-out infinite}
.mk .cur{position:absolute;right:-2px;bottom:-12px;width:16px;height:16px;z-index:5;animation:mk-cur 2.4s ease-out infinite;pointer-events:none}
.mk .ck{display:flex;align-items:center;gap:5px;font-size:10.5px;margin-top:4px;color:#31333f}
.mk .ck i{width:11px;height:11px;border:1.5px solid #b3b7c2;border-radius:3px;display:inline-block;background:#fff}
.mk .ttl{font-weight:800;font-size:13px;color:#1c1633}
.mk .cap{font-size:10.5px;color:#7d7690;margin:2px 0 8px}
.mk .brand{font-size:19px;font-weight:800;color:#ff8fab;text-align:center;margin-bottom:6px;letter-spacing:-.5px}
.mk .exp{border:1px solid #e6e8ee;border-radius:8px;padding:8px 10px;background:#fff}
.mk .exp .h{font-weight:700;font-size:11px;margin-bottom:8px;color:#31333f}
.mk .tb{width:100%;border-collapse:collapse;font-size:10.5px}
.mk .tb th{background:#f3f4f8;text-align:left;padding:5px 7px;font-weight:700;color:#555a66;border-bottom:1px solid #e6e8ee}
.mk .tb td{padding:5px 7px;border-bottom:1px solid #f0f1f5}
.mk .pill{border-radius:14px;padding:4px 12px;font-weight:700;font-size:11px}
.mk .pill.a{background:#fff0f5;border:1px solid #ffd3e2;color:#d6336c}
.mk .pill.b{background:#e8f0fe;border:1px solid #c5d5f5;color:#1f5fd2}
.mk .nav{padding:6px 8px;border-radius:7px;font-weight:600;font-size:10.5px;margin-bottom:4px;background:#fff;animation:mk-nav 4.5s infinite}
.mk .side{width:150px;border:1px solid #e6e8ee;border-radius:9px;padding:8px;background:#fafbfd}
.mk .star{font-size:26px;color:#f5b400;display:inline-block;animation:mk-star .6s ease-out both}
.mk .ok{background:#e6f7ee;border:1px solid #b6e3c8;color:#1d7a47;border-radius:7px;padding:6px 10px;font-weight:600;font-size:11px;animation:mk-pop .6s ease-out 1.2s both}
.mk .uploader{border:1.5px dashed #c9cdd8;border-radius:9px;padding:12px;display:flex;align-items:center;justify-content:space-between;background:#f9fafc}
.mk .opts{border:1px solid #e6e8ee;border-radius:8px;background:#fff;box-shadow:0 6px 16px rgba(0,0,0,.10);padding:4px;margin-top:3px;
 transform-origin:top;animation:mk-open 2.4s ease-out infinite}
.mk .opts div{padding:5px 8px;border-radius:5px;font-size:10.5px}
.mk .opts div.s{background:#ffe3ed;font-weight:700}
.mk .row-in{animation:mk-drop 2.4s ease-out infinite}
.mk .chk-on{width:11px;height:11px;border-radius:3px;background:#ff4f7b;display:inline-block;color:#fff;font-size:8px;line-height:11px;text-align:center}
.mk .sel-sm{border:1px solid #d6d9e0;border-radius:6px;padding:3px 6px;font-size:10px;background:#fff;display:flex;justify-content:space-between;gap:4px;white-space:nowrap}
.mk .sim{background:#fff4f6;color:#a95e69}
@keyframes mk-fill{0%,40%{background:#fff;color:#808495}60%,100%{background:#ffe9f0;color:#c2185b}}
.mk .fillv{animation:mk-fill 3s ease-in-out infinite}
.mk .lg{display:flex;border:1px solid #f0e3e6;border-radius:14px;overflow:hidden;background:#fff}
.mk .lg .kiri{flex:0 0 30%;background:linear-gradient(160deg,#f9c3d6,#c2185b);position:relative;min-height:190px}
.mk .lg .tube{position:absolute;bottom:-6px;width:34%;border-radius:12px 12px 4px 4px;background:linear-gradient(180deg,#fbd0de,#b0235f)}
.mk .lg .kanan{flex:1;padding:14px 18px;min-width:0}
.mk .lg .logo{font-size:21px;font-weight:800;color:#ff8fab;text-align:center;letter-spacing:-.5px}
.mk .lg .wb{font-size:7.5px;letter-spacing:2px;font-weight:800;color:#a95e69;margin-top:6px}
.mk .lg .lt{font-size:14px;font-weight:800;color:#45343b;margin:1px 0 1px}
.mk .lg .sb{font-size:9px;color:#8c7a80;margin-bottom:8px}
.mk .lg .f{border:1px solid #e7d9dc;border-radius:7px;padding:7px 9px;font-size:10px;color:#8c7a80;margin-bottom:7px;background:#fff;
 display:flex;justify-content:space-between;align-items:center;position:relative}
.mk .lg .f.hl{border-color:#c98283 !important;animation:mk-pulse 1.8s ease-in-out infinite}
.mk .lg .rb{background:linear-gradient(135deg,#c98283,#d3a07f);color:#fff;border-radius:7px;padding:8px;text-align:center;font-weight:700;
 font-size:11px;position:relative;margin-top:2px}
.mk .lg .rb.hl{animation:mk-pulse 1.8s ease-in-out infinite;box-shadow:0 0 0 3px rgba(201,130,131,.35)}
.mk .lg .ob{border:1px solid #e7d9dc;border-radius:7px;padding:7px;text-align:center;font-size:10px;color:#a95e69;margin-top:7px}
.mk .lg .lk{display:flex;justify-content:space-between;align-items:center;font-size:9.5px;color:#a95e69;margin-top:8px;gap:6px}
.mk .lg .lk span{padding:3px 7px;border-radius:6px;border:1px solid transparent}
.mk .lg .lk span.hl{border-color:#c98283;animation:mk-pulse 1.8s ease-in-out infinite}
.mk .lg .fp{font-size:7.5px;color:#a89aa0;text-align:center;margin-top:9px}
.mk .lg .dim2{opacity:.35}
.mk .dlg{border:1px solid #ece0e3;border-radius:14px;padding:16px 20px;background:#fff;box-shadow:0 10px 30px rgba(0,0,0,.12);max-width:340px;margin:0 auto;position:relative}
.mk .dlg .dt{font-size:16px;font-weight:800;color:#45343b;display:flex;justify-content:space-between}
"""

# ------------------------------------------------------------------ komponen mini (mengikuti label asli di aplikasi)
_CUR = ('<svg class="cur" viewBox="0 0 16 16"><path d="M2 1l11 6-5 1.4L6.6 14z" fill="#31333f" stroke="#fff" stroke-width="1"/></svg>')


def _mk(isi):
    return f'<div class="mk">{isi}</div>'


def _l(t):
    return f'<div class="l">{t}</div>'


def _in(t="", hl=False, dim=False, w=False, caret=False, flex=1):
    kls = "in" + (" w" if w else "") + (" hl" if hl else "") + (" dim" if dim else "")
    return f'<div style="flex:{flex}" class="{kls}"><span>{t}</span>{"<span>&#9662;</span>" if caret else ""}</div>'


def _field(label, t="", flex=1, **kw):
    return f'<div style="flex:{flex}">{_l(label)}{_in(t, **kw).replace("flex:1", "flex:none")}</div>'


def _bt(t, p=False, hl=False, cur=False, flex=1):
    kls = "bt" + (" p" if p else "") + (" hl" if hl else "")
    return f'<div style="flex:{flex}" class="{kls}">{t}{_CUR if cur else ""}</div>'


def _chk(t, on=False):
    return f'<div class="ck"><i></i>{t}</div>'


def _exp(judul, isi):
    return f'<div class="exp"><div class="h">&#9662; {judul}</div>{isi}</div>'


# ------------------------------------------------------------------ tampilan per langkah
ILUSTRASI = {
    "login": _mk('<div class="lg"><div class="kiri"><div class="tube" style="left:8%;height:62%"></div>'
                 '<div class="tube" style="left:34%;height:78%"></div><div class="tube" style="left:60%;height:56%"></div></div>'
                 '<div class="kanan"><div class="logo">Glad2Glow</div><div class="wb">WELCOME BACK</div>'
                 '<div class="lt">Let&#39;s glow in.</div><div class="sb">Sign in to continue to your Glad2Glow workspace.</div>'
                 '<div class="f hl">Username</div><div class="f hl">Password<span>&#128065;</span></div>'
                 '<div class="ck" style="margin:0 0 7px"><i></i>Remember me</div>'
                 '<div class="rb hl">Login &rarr;' + _CUR + '</div></div></div>'),
    "daftar": _mk('<div class="dlg"><div class="dt"><span>Create your account</span><span style="font-weight:400">&times;</span></div>'
                  '<div class="sb" style="font-size:10px;color:#8c7a80;margin:2px 0 10px">Join the Glad2Glow community.</div>'
                  '<div class="lg" style="border:0;overflow:visible"><div class="kanan" style="padding:2px 0 0">'
                  + "".join(f'<div class="f hl" style="animation-delay:{k * .3}s">{t}</div>'
                            for k, t in enumerate(("Username", "Email", "Password (min. 8 characters)", "Confirm Password")))
                  + '<div class="rb hl">Create Account &rarr;' + _CUR + '</div></div></div></div>'),
    "login_bawah": _mk('<div class="lg"><div class="kiri" style="min-height:170px"><div class="tube" style="left:8%;height:62%"></div>'
                       '<div class="tube" style="left:34%;height:78%"></div><div class="tube" style="left:60%;height:56%"></div></div>'
                       '<div class="kanan"><div class="dim2"><div class="logo">Glad2Glow</div>'
                       '<div class="f">Username</div><div class="f">Password</div><div class="rb">Login &rarr;</div></div>'
                       '<div class="lk"><span class="hl">&#9432; Help &amp; Support</span><span>Tutorial</span>'
                       '<span class="hl">Forgot password?</span></div>'
                       '<div class="ob">Sign Up</div>'
                       '<div class="fp">By continuing, you agree to our Terms of Service and Privacy Policy.</div></div></div>'),
    "topbar_tutorial": _mk(
        '<div class="r" style="justify-content:space-between;align-items:center;margin:0">'
        '<div class="ttl">NOO &amp; SKU Submission</div>'
        '<div style="display:flex;gap:8px"><div class="pill a hl">Tutorial</div><div class="pill b">Help &amp; Support</div></div></div>'
        '<div class="cap" style="margin:8px 0 0">Upload Failure List file to process data.</div>'),
    "topbar_help": _mk(
        '<div class="r" style="justify-content:space-between;align-items:center;margin:0">'
        '<div class="ttl">NOO &amp; SKU Submission</div>'
        '<div style="display:flex;gap:8px"><div class="pill a">Tutorial</div><div class="pill b hl">Help &amp; Support</div></div></div>'
        '<div class="cap" style="margin:8px 0 0">Upload Failure List file to process data.</div>'),
    "menu": _mk(
        '<div style="display:flex;gap:10px"><div class="side">'
        '<div style="font-weight:800;font-size:11px;margin-bottom:8px">NOO &amp; SKU Submission</div>'
        '<div class="nav" style="animation-delay:0s">NOO &amp; SKU Submission</div>'
        '<div class="nav" style="animation-delay:1.5s">SPV Confirmation</div>'
        '<div class="nav" style="animation-delay:3s">History</div>'
        '<div style="margin-top:10px;font-size:10px;color:#7d7690">Log Out</div></div>'
        '<div style="flex:1;opacity:.5"><div class="ttl">Halaman</div>'
        '<div class="in" style="margin-top:8px;min-height:60px"></div></div></div>'),
    "bintang": _mk(
        '<div class="ttl">Feedback</div>'
        '<div style="font-weight:700;margin:8px 0 2px">Bagaimana pengalaman Anda menggunakan sistem ini?</div>'
        '<div class="cap">Berikan penilaian dari 1–5 bintang</div>'
        + "".join(f'<span class="star" style="animation-delay:{k * .25}s">&#9733;</span>' for k in range(5))),
    "upload": _mk(
        _l("Upload Failure List (.xlsx / .xls, max 10 MB per file)")
        + '<div class="uploader"><div><div style="font-weight:700">Drag and drop files here</div>'
        '<div class="cap" style="margin:2px 0 0">Limit 10MB per file &bull; XLSX, XLS</div></div>'
        + _bt("Browse files", hl=True, cur=True, flex=0).replace('style="flex:0"', 'style="flex:none"') + '</div>'),
    "sku_manual": _mk(_exp(
        "Manual Input, 1 by 1 row",
        '<div class="r">' + _field("Distributor *", "Pilih distributor", 3, caret=True, w=True)
        + _field("Customer Name", "", 2.2, dim=True) + _field("Customer Code", "", 1.2, dim=True) + '</div>'
        '<div class="r">' + _field("Product *", "Pilih product", 2, caret=True, w=True)
        + '<div style="flex:1.6">' + _l("Customer Product Code *")
        + _in("Product code (required)...", hl=True, w=True).replace('style="flex:1"', '') + _chk("Same as Product") + '</div>'
        + _field("Customer Product Name", "", 2, dim=True)
        + '<div style="flex:1.1">' + _bt("+ Add Row", hl=True, cur=True) + '</div></div>')),
    "sku_same": _mk(_exp(
        "Manual Input, 1 by 1 row",
        '<div class="r">' + _field("Product *", "G2G-123", 2, caret=True, w=True)
        + '<div style="flex:2">' + _l("Customer Product Code *")
        + '<div class="in dim fillv" style="min-height:28px"><span>G2G-123</span></div>'
        + '<div class="ck hl" style="padding:2px 4px;border-radius:5px;border:1px solid #ff4f8b;width:max-content"><span class="chk-on">&#10003;</span>Same as Product</div></div>'
        + '<div style="flex:1.1">' + _bt("+ Add Row") + '</div></div>')),
    "sku_paste": _mk(_exp(
        "Paste All SKU",
        '<div class="cap" style="margin-top:0">Step 1 — Select distributor, paste Product and Customer Product Code (one per line)</div>'
        + _field("Distributor", "Pilih distributor", 1, caret=True, w=True).replace('style="flex:1"', 'style="margin-bottom:6px"')
        + '<div class="r" style="align-items:stretch">'
        '<div style="flex:1">' + _l("Product") + '<div class="in w hl" style="min-height:46px;align-items:flex-start;flex-direction:column;justify-content:flex-start;gap:2px"><span>G2G-123</span><span>NXP-456</span></div></div>'
        '<div style="flex:1">' + _l("Customer Product Code") + '<div class="in w hl" style="min-height:46px;align-items:flex-start;flex-direction:column;justify-content:flex-start;gap:2px;animation-delay:.4s"><span>SKU001</span><span>SKU002</span></div>'
        + _chk("Same as Product") + '</div></div>'
        + '<div style="width:110px;margin:6px 0 8px">' + _bt("Apply Paste", hl=True, cur=True) + '</div>'
        '<div class="cap" style="margin:0 0 4px">Step 2 — Fix any wrong Product using the dropdown, then add to staged rows</div>'
        '<table class="tb"><tr><th>Pasted Product</th><th>Match</th><th>Product (fix here)</th><th>Customer Product Code</th></tr>'
        '<tr><td>G2G-123</td><td>Exact</td><td><div class="sel-sm"><span>G2G-123</span><span>&#9662;</span></div></td><td>SKU001</td></tr>'
        '<tr><td>NXP-45</td><td>Not found</td><td><div class="sel-sm hl"><span>Pilih</span><span>&#9662;</span></div></td><td>SKU002</td></tr></table>'
        '<div style="width:140px;margin-top:8px">' + _bt("+ Add to Staged", p=True) + '</div>')),
    "sku_upload": _mk(
        '<table class="tb"><tr><th>Select</th><th>Customer Name</th><th>Product Description</th><th>Product</th><th>Customer Product Code</th></tr>'
        + "".join(f'<tr class="row-in" style="animation-delay:{k * .4}s"><td><span class="chk-on">&#10003;</span></td><td>DISTRIBUTOR {k + 1}</td>'
                  f'<td><div class="sel-sm hl"><span>Pilih</span><span>&#9662;</span></div></td>'
                  f'<td><div class="sel-sm hl"><span>G2G-10{k}</span><span>&#9662;</span></div></td><td>CPC-0{k}</td></tr>'
                  for k in range(3))
        + '</table><div class="r" style="margin:10px 0 0;align-items:stretch">'
        + _bt("Fix Customer Code") + _bt("Submit SKU Mapping", p=True, hl=True, cur=True, flex=2) + '</div>'),
    "noo": _mk(_exp(
        "Manual Input and Paste Customer Store Code",
        '<div class="cap" style="margin-top:0">Step 1 — Paste Customer Store Code and Store Name (one per line)</div>'
        '<div class="r" style="align-items:stretch">'
        '<div style="flex:1">' + _l("Customer Store Code") + '<div class="in w" style="min-height:58px;align-items:flex-start;flex-direction:column;justify-content:flex-start;gap:2px"><span>DST001</span><span>DST002</span></div></div>'
        '<div style="flex:1">' + _l("Store Name") + '<div class="in w" style="min-height:58px;align-items:flex-start;flex-direction:column;justify-content:flex-start;gap:2px"><span>STORE A</span><span>STORE B</span></div></div></div>'
        + _bt("Apply Paste", hl=True, cur=True, flex=0).replace('style="flex:0"', 'style="width:110px"'))),
    "noo_upload": _mk(
        '<table class="tb"><tr><th>Store Code *</th><th>Customer Store Name *</th><th>City *</th><th>SPV *</th><th>Sales *</th><th>Type *</th></tr>'
        + "".join(f'<tr class="row-in" style="animation-delay:{k * .4}s"><td>DST00{k + 1}</td><td>STORE {chr(65 + k)}</td>'
                  + "".join(f'<td><div class="sel-sm hl" style="animation-delay:{m * .2}s"><span>Pilih</span><span>&#9662;</span></div></td>' for m in range(4))
                  + '</tr>' for k in range(2))
        + '</table><div style="height:10px"></div>' + _bt("Submit NOO", p=True, hl=True, cur=True)),
    "apply_all": _mk(
        '<div style="font-size:10px;font-weight:700;color:#ef4f58;margin-bottom:3px">*helper (Apply to all..)</div>'
        '<div class="r" style="align-items:stretch;margin-bottom:6px">'
        + _in("Apply to all...", w=True, caret=True, hl=True)
        + _in("Apply to all...", w=True, caret=True) + _in("Apply to all...", w=True, caret=True) + _in("Apply to all...", w=True, caret=True) + '</div>'
        '<table class="tb"><tr><th>Store Code</th><th>City</th><th>SPV</th><th>Sales</th><th>Type</th></tr>'
        + "".join(f'<tr><td>DST00{k + 1}</td><td class="fillv" style="animation-delay:{k * .3}s">Jakarta</td><td>-</td><td>-</td><td>-</td></tr>' for k in range(3))
        + '</table>'),
    "kirim": _mk(
        '<div class="r" style="align-items:stretch">'
        + _bt("Save All to Supabase", p=True, hl=True) + _bt("Submit SKU Mapping", p=True, hl=True).replace('class="bt p hl"', 'class="bt p hl" style="animation-delay:.5s;flex:1"')
        + _bt("Submit NOO", p=True, hl=True, cur=True).replace('class="bt p hl"', 'class="bt p hl" style="animation-delay:1s;flex:1"') + '</div>'
        '<div class="ok" style="margin-top:6px">Data berhasil dikirim</div>'),
    "spv": _mk(
        '<div class="ttl">SPV Confirmation</div>'
        '<div class="cap">Confirm outlet type and validate data before submission.</div>'
        '<div class="r">' + _field("Distributor *", "Pilih distributor", 3, caret=True, w=True, hl=True)
        + '<div style="flex:2"><div class="in dim">Region: —</div></div>' + '<div style="flex:1.2">' + _bt("Refresh") + '</div></div>'),
    "distri": _mk(
        _l("Distributor *") + _in("Pilih distributor", w=True, caret=True, hl=True).replace('style="flex:1"', '')
        + '<div class="opts"><div class="s">DISTRIBUTOR A</div><div>DISTRIBUTOR B</div><div>DISTRIBUTOR C</div></div>'),
    "tinjau": _mk(
        '<table class="tb"><tr><th>Store</th><th class="sim">Similarity (Name)</th><th class="sim">Similarity (Code)</th><th>NOO / Existing</th></tr>'
        + "".join(f'<tr><td>STORE {chr(65 + k)}</td><td class="sim">STORE {chr(65 + k)} BARU</td><td class="sim">{"ST-00" + str(k + 1)}</td>'
                  f'<td><div class="sel-sm hl" style="animation-delay:{k * .4}s"><span>{"New" if k % 2 == 0 else "Existing"}</span><span>&#9662;</span></div></td></tr>'
                  for k in range(3)) + '</table>'),
    "spv_sim": _mk(
        _bt("Auto-fill Store ID from Similarity (2)", hl=True, cur=True, flex=0).replace('style="flex:0"', 'style="width:230px;margin-bottom:8px"')
        + '<table class="tb"><tr><th>Store</th><th class="sim">Similarity (Code)</th><th>NOO / Existing</th><th>Store ID</th></tr>'
        + "".join(f'<tr><td>STORE {chr(65 + k)}</td><td class="sim">ST-00{k + 1}</td><td><div class="sel-sm"><span>Existing</span><span>&#9662;</span></div></td>'
                  f'<td><div class="sel-sm fillv" style="animation-delay:{k * .3}s"><span>ST-00{k + 1}</span><span>&#9662;</span></div></td></tr>'
                  for k in range(2)) + '</table>'),
    "konfirmasi": _mk(
        '<table class="tb"><tr><th>No</th><th>Store</th><th>Type</th></tr>'
        '<tr><td>1</td><td>STORE A</td><td>New</td></tr><tr><td>2</td><td>STORE B</td><td>Existing</td></tr></table>'
        '<div style="height:10px"></div>'
        + _bt(" Submit Confirmation", p=True, hl=True, cur=True)
        + '<div class="ok" style="margin-top:8px">2 outlet(s) confirmed</div>'),
}
