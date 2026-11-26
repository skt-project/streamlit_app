-- Jalankan sekali di Supabase (SQL Editor). Aman dijalankan ulang.
-- Satu tabel `account_requests` untuk dua jenis permintaan, dibedakan kolom `type`:
--   signup_approval   = akun ke-2 (SPV) untuk kode yang sudah terdaftar
--   password_approval = permintaan reset password (Forgot password)

-- 1) Kalau tabel lama password_reset_requests sudah ada, ganti namanya (data lama ikut).
do $$
begin
  if to_regclass('public.password_reset_requests') is not null
     and to_regclass('public.account_requests') is null then
    alter table public.password_reset_requests rename to account_requests;
  end if;
end $$;

-- 2) Buat tabel baru kalau belum ada.
create table if not exists public.account_requests (
  id            bigint generated always as identity primary key,
  type          text not null default 'password_approval',   -- signup_approval | password_approval
  username      text not null,
  email         text,
  status        text not null default 'pending',             -- pending | approved | rejected | done
  created_at    timestamptz not null default now(),
  decided_by    text,
  decided_at    timestamptz
);

-- 3) Kolom tambahan (aman kalau sudah ada). Dipakai untuk signup_approval:
--    password_hash + tagging disimpan di sini, baru dipindah ke tabel users saat admin Approve.
alter table public.account_requests add column if not exists type          text not null default 'password_approval';
alter table public.account_requests add column if not exists password_hash text;
alter table public.account_requests add column if not exists tagging       text;

-- 4) RLS + policy (aplikasi memakai API key yang sama dengan tabel lain; samakan dengan policy tabel users bila berbeda).
alter table public.account_requests enable row level security;
drop policy if exists "reset_select" on public.account_requests;
drop policy if exists "reset_insert" on public.account_requests;
drop policy if exists "reset_update" on public.account_requests;
drop policy if exists "account_requests_select" on public.account_requests;
drop policy if exists "account_requests_insert" on public.account_requests;
drop policy if exists "account_requests_update" on public.account_requests;
create policy "account_requests_select" on public.account_requests for select using (true);
create policy "account_requests_insert" on public.account_requests for insert with check (true);
create policy "account_requests_update" on public.account_requests for update using (true) with check (true);

-- 5) Satu kode (username) boleh punya 2 akun di tabel users, jadi TIDAK boleh ada UNIQUE pada username saja.
--    Cek constraint / index yang ada:
select conname, pg_get_constraintdef(oid) as definisi
from pg_constraint where conrelid = 'public.users'::regclass;
select indexname, indexdef from pg_indexes where schemaname = 'public' and tablename = 'users';
--    Kalau ada yang UNIQUE hanya pada kolom username, hapus (ganti NAMA dari hasil di atas):
-- alter table public.users drop constraint NAMA_CONSTRAINT;
-- drop index public.NAMA_INDEX;     -- jika berupa unique index

notify pgrst, 'reload schema';
