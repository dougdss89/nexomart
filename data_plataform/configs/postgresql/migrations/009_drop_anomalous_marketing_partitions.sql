-- 009: one-off drop of the anomalous, already-landed marketing partitions.
-- dailyperformance_2026_08 = 276M rows / 228 GB and _2026_09 = 75M rows / 62 GB — ~800x the
-- normal monthly volume (Jul=334k, Oct=995), a runaway from the heavy-seeding sessions. Both are
-- fully in the MinIO landing (ingestion watermark is past 2026-10-01), so dropping loses nothing
-- durable. DROP is instant and returns ~290 GB to the filesystem. Safe to re-run (IF EXISTS).
DROP TABLE IF EXISTS marketing.dailyperformance_2026_08;
DROP TABLE IF EXISTS marketing.dailyperformance_2026_09;
