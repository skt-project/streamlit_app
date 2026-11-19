-- PO Suggestion Tool (po_buffer.py): download tracking table.
--
-- One row per STORE per download CLICK. A bulk ZIP of 5 stores writes 5 rows
-- that share one event_id; an individual PDF download writes 1 row.
--   per-store counter       = COUNT(*) grouped by store_code
--   individual / bulk split = download_type
--   stores inside one bulk  = rows sharing the same event_id
--
-- The app runs this statement itself the first time it needs the table
-- (CREATE TABLE IF NOT EXISTS), so running it by hand is only needed when the
-- app's service account has no bigquery.tables.create permission on gt_schema.
-- Keep it identical to po_buffer_tracking.create_table_ddl() (a test checks).

CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.gt_schema.po_buffer_download_log` (
  event_id STRING NOT NULL OPTIONS(description="One id per download action; shared by every store of one bulk ZIP"),
  store_code STRING NOT NULL OPTIONS(description="Store downloaded"),
  store_name STRING OPTIONS(description="Store name at download time"),
  region STRING OPTIONS(description="Region at download time"),
  distributor STRING OPTIONS(description="Distributor at download time"),
  download_type STRING NOT NULL OPTIONS(description="individual or bulk"),
  file_name STRING OPTIONS(description="PDF file name of this store in the download"),
  bulk_file_count INT64 OPTIONS(description="PDFs in the ZIP; NULL for individual downloads"),
  downloaded_at TIMESTAMP NOT NULL OPTIONS(description="UTC time of the click")
)
OPTIONS(description="PO Suggestion Tool: one row per store per download click (po_buffer.py)");
