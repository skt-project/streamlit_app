-- Jalankan sekali di Supabase (SQL Editor): kolom waktu untuk dashboard SLA Performance.
-- Aplikasi mengisinya otomatis saat sebuah toko menjadi "DMS Done" / "DMS & Basis Done" (data lama tetap kosong).
alter table public."INPUT_NOO" add column if not exists dms_done_at   timestamptz;
alter table public."INPUT_NOO" add column if not exists basis_done_at timestamptz;

notify pgrst, 'reload schema';
