-- Jalankan sekali di Supabase (SQL Editor): kolom untuk bukti gambar (screenshot) di tiket Help & Support.
-- Gambar diperkecil oleh aplikasi lalu disimpan sebagai teks JSON berisi daftar data URL (maks. 3 gambar per tiket).
alter table public.help_support add column if not exists attachments text;

notify pgrst, 'reload schema';
