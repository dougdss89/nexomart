-- 007: stop pre-first-bound history from living in the *_other DEFAULT partitions, and pre-create the
-- monthly partitions through 2027-12 (nothing creates them automatically).
--  - purchaseorderheader, salesorder: zero-copy ATTACH of the old DEFAULT as `<table>_history`
--    (salesorder_other held 58M rows / 78 GB; copying it would not fit on the 131 GB free).
--  - transactionhistory, dailyperformance: small, so split by copy inside one transaction.
-- Safe to run only once per table (the DEFAULT is replaced by a new, empty one).
-- person.customer is LIST-partitioned by country and is not touched here.

-- sales.salesorder: DEFAULT only holds rows older than 2026-06-01 -> attach it as the history partition (zero-copy).
-- A validated CHECK lets ATTACH skip the full-table scan; the old DEFAULT is replaced by an empty one.
ALTER TABLE sales.salesorder DETACH PARTITION sales.salesorder_other;
ALTER TABLE sales.salesorder_other RENAME TO salesorder_history;
ALTER TABLE sales.salesorder_history ADD CONSTRAINT salesorder_history_chk
  CHECK (order_date IS NOT NULL AND order_date < '2026-06-01 00:00:00+00') NOT VALID;
ALTER TABLE sales.salesorder_history VALIDATE CONSTRAINT salesorder_history_chk;
ALTER TABLE sales.salesorder ATTACH PARTITION sales.salesorder_history FOR VALUES FROM (MINVALUE) TO ('2026-06-01 00:00:00+00');
ALTER TABLE sales.salesorder_history DROP CONSTRAINT salesorder_history_chk;
CREATE TABLE sales.salesorder_other PARTITION OF sales.salesorder DEFAULT;

DO $$
DECLARE d date; nm text;
BEGIN
  FOR d IN SELECT generate_series(date '2026-11-01', date '2027-12-01', interval '1 month')::date LOOP
    nm := 'salesorder_' || to_char(d, 'YYYY_MM');
    IF to_regclass('sales.' || nm) IS NULL THEN
      EXECUTE format('CREATE TABLE sales.%I PARTITION OF sales.salesorder FOR VALUES FROM (%L) TO (%L)',
                     nm, d::text || ' 00:00:00+00', (d + interval '1 month')::date::text || ' 00:00:00+00');
    END IF;
  END LOOP;
END $$;

ANALYZE sales.salesorder;
