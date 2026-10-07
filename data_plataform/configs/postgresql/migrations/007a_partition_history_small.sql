-- 007: stop pre-first-bound history from living in the *_other DEFAULT partitions, and pre-create the
-- monthly partitions through 2027-12 (nothing creates them automatically).
--  - purchaseorderheader, salesorder: zero-copy ATTACH of the old DEFAULT as `<table>_history`
--    (salesorder_other held 58M rows / 78 GB; copying it would not fit on the 131 GB free).
--  - transactionhistory, dailyperformance: small, so split by copy inside one transaction.
-- Safe to run only once per table (the DEFAULT is replaced by a new, empty one).
-- person.customer is LIST-partitioned by country and is not touched here.

-- purchasing.purchaseorderheader: DEFAULT only holds rows older than 2025-08-01 -> attach it as the history partition (zero-copy).
-- A validated CHECK lets ATTACH skip the full-table scan; the old DEFAULT is replaced by an empty one.
ALTER TABLE purchasing.purchaseorderheader DETACH PARTITION purchasing.purchaseorderheader_other;
ALTER TABLE purchasing.purchaseorderheader_other RENAME TO purchaseorderheader_history;
ALTER TABLE purchasing.purchaseorderheader_history ADD CONSTRAINT purchaseorderheader_history_chk
  CHECK (order_date IS NOT NULL AND order_date < '2025-08-01 00:00:00+00') NOT VALID;
ALTER TABLE purchasing.purchaseorderheader_history VALIDATE CONSTRAINT purchaseorderheader_history_chk;
ALTER TABLE purchasing.purchaseorderheader ATTACH PARTITION purchasing.purchaseorderheader_history FOR VALUES FROM (MINVALUE) TO ('2025-08-01 00:00:00+00');
ALTER TABLE purchasing.purchaseorderheader_history DROP CONSTRAINT purchaseorderheader_history_chk;
CREATE TABLE purchasing.purchaseorderheader_other PARTITION OF purchasing.purchaseorderheader DEFAULT;

DO $$
DECLARE d date; nm text;
BEGIN
  FOR d IN SELECT generate_series(date '2026-10-01', date '2027-12-01', interval '1 month')::date LOOP
    nm := 'purchaseorderheader_' || to_char(d, 'YYYY_MM');
    IF to_regclass('purchasing.' || nm) IS NULL THEN
      EXECUTE format('CREATE TABLE purchasing.%I PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM (%L) TO (%L)',
                     nm, d::text || ' 00:00:00+00', (d + interval '1 month')::date::text || ' 00:00:00+00');
    END IF;
  END LOOP;
END $$;

ANALYZE purchasing.purchaseorderheader;

-- warehouse.transactionhistory: DEFAULT mixes old history with rows past the last monthly bound -> split by copy (small table).
BEGIN;
ALTER TABLE warehouse.transactionhistory DETACH PARTITION warehouse.transactionhistory_other;
ALTER TABLE warehouse.transactionhistory_other RENAME TO transactionhistory_other_old;
CREATE TABLE warehouse.transactionhistory_history PARTITION OF warehouse.transactionhistory FOR VALUES FROM (MINVALUE) TO ('2026-05-01 00:00:00+00');

DO $$
DECLARE d date; nm text;
BEGIN
  FOR d IN SELECT generate_series(date '2026-10-01', date '2027-12-01', interval '1 month')::date LOOP
    nm := 'transactionhistory_' || to_char(d, 'YYYY_MM');
    IF to_regclass('warehouse.' || nm) IS NULL THEN
      EXECUTE format('CREATE TABLE warehouse.%I PARTITION OF warehouse.transactionhistory FOR VALUES FROM (%L) TO (%L)',
                     nm, d::text || ' 00:00:00+00', (d + interval '1 month')::date::text || ' 00:00:00+00');
    END IF;
  END LOOP;
END $$;

CREATE TABLE warehouse.transactionhistory_other PARTITION OF warehouse.transactionhistory DEFAULT;
DO $$
DECLARE n_old bigint; n_ins bigint;
BEGIN
  SELECT count(*) INTO n_old FROM warehouse.transactionhistory_other_old;
  INSERT INTO warehouse.transactionhistory SELECT * FROM warehouse.transactionhistory_other_old;
  GET DIAGNOSTICS n_ins = ROW_COUNT;
  IF n_ins <> n_old THEN RAISE EXCEPTION 'transactionhistory: copied % of % rows', n_ins, n_old; END IF;
END $$;
DROP TABLE warehouse.transactionhistory_other_old;
COMMIT;
ANALYZE warehouse.transactionhistory;

-- marketing.dailyperformance: DEFAULT mixes old history with rows past the last monthly bound -> split by copy (small table).
BEGIN;
ALTER TABLE marketing.dailyperformance DETACH PARTITION marketing.dailyperformance_other;
ALTER TABLE marketing.dailyperformance_other RENAME TO dailyperformance_other_old;
CREATE TABLE marketing.dailyperformance_history PARTITION OF marketing.dailyperformance FOR VALUES FROM (MINVALUE) TO ('2026-07-01 00:00:00+00');

DO $$
DECLARE d date; nm text;
BEGIN
  FOR d IN SELECT generate_series(date '2026-10-01', date '2027-12-01', interval '1 month')::date LOOP
    nm := 'dailyperformance_' || to_char(d, 'YYYY_MM');
    IF to_regclass('marketing.' || nm) IS NULL THEN
      EXECUTE format('CREATE TABLE marketing.%I PARTITION OF marketing.dailyperformance FOR VALUES FROM (%L) TO (%L)',
                     nm, d::text || ' 00:00:00+00', (d + interval '1 month')::date::text || ' 00:00:00+00');
    END IF;
  END LOOP;
END $$;

CREATE TABLE marketing.dailyperformance_other PARTITION OF marketing.dailyperformance DEFAULT;
DO $$
DECLARE n_old bigint; n_ins bigint;
BEGIN
  SELECT count(*) INTO n_old FROM marketing.dailyperformance_other_old;
  INSERT INTO marketing.dailyperformance SELECT * FROM marketing.dailyperformance_other_old;
  GET DIAGNOSTICS n_ins = ROW_COUNT;
  IF n_ins <> n_old THEN RAISE EXCEPTION 'dailyperformance: copied % of % rows', n_ins, n_old; END IF;
END $$;
DROP TABLE marketing.dailyperformance_other_old;
COMMIT;
ANALYZE marketing.dailyperformance;
