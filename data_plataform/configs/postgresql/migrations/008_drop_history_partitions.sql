-- 008: one-time drop of the *_history catch-all partitions (the pre-first-month backfill).
-- All four are already landed to MinIO (ingestion watermark is well past their upper bounds),
-- so dropping them loses nothing durable. DROP is instant and returns space to the filesystem,
-- unlike DELETE (which would bloat and need VACUUM FULL). Frees ~90 GB.
-- Safe to re-run: IF EXISTS guards each drop.
DROP TABLE IF EXISTS sales.salesorder_history;
DROP TABLE IF EXISTS warehouse.transactionhistory_history;
DROP TABLE IF EXISTS purchasing.purchaseorderheader_history;
DROP TABLE IF EXISTS marketing.dailyperformance_history;
