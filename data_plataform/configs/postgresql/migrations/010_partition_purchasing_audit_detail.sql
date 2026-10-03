-- 010: partition purchasing.purchaseorderaudit and purchasing.purchaseorderdetail by RANGE(created_at),
-- monthly, so both are covered by the 45-day retention routine (configs/postgresql/retention.sql).
-- Same shape as migrations 002-005/007: RENAME -> create partitioned (composite PK/UNIQUE on the
-- partition key) -> history + monthly partitions through 2027-12 + DEFAULT -> INSERT ... SELECT ->
-- verify row count -> DROP old. Neither table has inbound FKs, so the composite PK breaks nothing.
--
-- RUN WITH THE SEEDER PAUSED (the RENAME + large INSERT would otherwise race concurrent inserts):
--   docker exec airflow_scheduler airflow dags pause nexomart_seeder_dag
--   docker exec -i pgprojeto psql -U <user> -d nexomarket_db -v ON_ERROR_STOP=1 -f 010_partition_purchasing_audit_detail.sql
--   docker exec airflow_scheduler airflow dags unpause nexomart_seeder_dag
-- audit is ~68M rows / 18 GB and detail ~37M / 11 GB, so the INSERTs take several minutes each.

-- ============================ purchaseorderaudit ============================
BEGIN;

ALTER SEQUENCE purchasing.purchaseorderaudit_audit_id_seq OWNED BY NONE;
ALTER TABLE purchasing.purchaseorderaudit RENAME TO purchaseorderaudit_old;
-- RENAME keeps the old indexes' names; free the canonical names for the new table (the renamed
-- indexes vanish with purchaseorderaudit_old at the end). FK constraint names are per-table, so
-- they need no renaming.
ALTER INDEX purchasing.purchaseorderaudit_pkey RENAME TO purchaseorderaudit_old_pkey;
ALTER INDEX purchasing.purchaseorderaudit_row_guid_key RENAME TO purchaseorderaudit_old_row_guid_key;
ALTER INDEX purchasing.ix_purchasing_purchaseorderaudit_created_at RENAME TO poa_old_created_at_idx;
ALTER INDEX purchasing.ix_purchaseorderaudit_created_at RENAME TO poa_old_created_at2_idx;
ALTER INDEX purchasing.ix_purchaseorderaudit_header_id RENAME TO poa_old_header_idx;

CREATE TABLE purchasing.purchaseorderaudit (
    audit_id integer NOT NULL DEFAULT nextval('purchasing.purchaseorderaudit_audit_id_seq'::regclass),
    purchaseorderheader_id varchar NOT NULL,
    action varchar NOT NULL,
    employee_id integer NOT NULL,
    obs varchar(200),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz,
    is_active boolean NOT NULL,
    row_version integer NOT NULL,
    row_guid uuid NOT NULL,
    PRIMARY KEY (audit_id, created_at),
    UNIQUE (row_guid, created_at),
    FOREIGN KEY (employee_id) REFERENCES humanresources.employee(employee_id)
) PARTITION BY RANGE (created_at);

CREATE INDEX ix_purchasing_purchaseorderaudit_created_at ON purchasing.purchaseorderaudit (created_at);
CREATE INDEX ix_purchaseorderaudit_header_id ON purchasing.purchaseorderaudit (purchaseorderheader_id);

-- Pre-2026 history in one partition (retention prunes it; its data is already in the landing),
-- then one partition per month through 2027-12, then a DEFAULT catch-all.
CREATE TABLE purchasing.purchaseorderaudit_history PARTITION OF purchasing.purchaseorderaudit
    FOR VALUES FROM (MINVALUE) TO ('2026-01-01 00:00:00+00');
DO $$
DECLARE d date;
BEGIN
  FOR d IN SELECT generate_series(date '2026-01-01', date '2027-12-01', interval '1 month')::date LOOP
    EXECUTE format(
      'CREATE TABLE purchasing.%I PARTITION OF purchasing.purchaseorderaudit FOR VALUES FROM (%L) TO (%L)',
      'purchaseorderaudit_' || to_char(d, 'YYYY_MM'),
      d::text || ' 00:00:00+00',
      (d + interval '1 month')::date::text || ' 00:00:00+00');
  END LOOP;
END $$;
CREATE TABLE purchasing.purchaseorderaudit_other PARTITION OF purchasing.purchaseorderaudit DEFAULT;

DO $$
DECLARE n_old bigint; n_new bigint;
BEGIN
  SELECT count(*) INTO n_old FROM purchasing.purchaseorderaudit_old;
  INSERT INTO purchasing.purchaseorderaudit
    SELECT audit_id, purchaseorderheader_id, action, employee_id, obs, created_at,
           updated_at, is_active, row_version, row_guid
    FROM purchasing.purchaseorderaudit_old;
  GET DIAGNOSTICS n_new = ROW_COUNT;
  IF n_new <> n_old THEN RAISE EXCEPTION 'purchaseorderaudit: copied % of % rows', n_new, n_old; END IF;
END $$;

DROP TABLE purchasing.purchaseorderaudit_old;
ALTER SEQUENCE purchasing.purchaseorderaudit_audit_id_seq OWNED BY purchasing.purchaseorderaudit.audit_id;

COMMIT;
ANALYZE purchasing.purchaseorderaudit;

-- ============================ purchaseorderdetail ===========================
BEGIN;

ALTER TABLE purchasing.purchaseorderdetail RENAME TO purchaseorderdetail_old;
ALTER INDEX purchasing.purchaseorderdetail_pkey RENAME TO purchaseorderdetail_old_pkey;
ALTER INDEX purchasing.purchaseorderdetail_row_guid_key RENAME TO purchaseorderdetail_old_row_guid_key;
ALTER INDEX purchasing.ix_purchasing_purchaseorderdetail_created_at RENAME TO pod_old_created_at_idx;
ALTER INDEX purchasing.ix_purchaseorderdetail_created_at RENAME TO pod_old_created_at2_idx;
ALTER INDEX purchasing.ix_purchaseorderdetail_header_id RENAME TO pod_old_header_idx;

CREATE TABLE purchasing.purchaseorderdetail (
    purchaseorderdetail_id varchar(36) NOT NULL,
    purchaseorderheader_id varchar(36) NOT NULL,
    product_id integer NOT NULL,
    last_receipt_date timestamptz,
    is_fully_received boolean NOT NULL,
    due_date timestamptz NOT NULL,
    order_qty integer NOT NULL,
    unit_price numeric(19,4) NOT NULL,
    received_qty integer NOT NULL,
    rejected_qty integer NOT NULL,
    stocked_qty integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz,
    is_active boolean NOT NULL,
    row_version integer NOT NULL,
    row_guid uuid NOT NULL,
    PRIMARY KEY (purchaseorderdetail_id, created_at),
    UNIQUE (row_guid, created_at),
    FOREIGN KEY (product_id) REFERENCES product.product(product_id)
) PARTITION BY RANGE (created_at);

CREATE INDEX ix_purchasing_purchaseorderdetail_created_at ON purchasing.purchaseorderdetail (created_at);
CREATE INDEX ix_purchaseorderdetail_header_id ON purchasing.purchaseorderdetail (purchaseorderheader_id);

-- detail only has data from 2026-08 onward, so history is pre-2026-08.
CREATE TABLE purchasing.purchaseorderdetail_history PARTITION OF purchasing.purchaseorderdetail
    FOR VALUES FROM (MINVALUE) TO ('2026-08-01 00:00:00+00');
DO $$
DECLARE d date;
BEGIN
  FOR d IN SELECT generate_series(date '2026-08-01', date '2027-12-01', interval '1 month')::date LOOP
    EXECUTE format(
      'CREATE TABLE purchasing.%I PARTITION OF purchasing.purchaseorderdetail FOR VALUES FROM (%L) TO (%L)',
      'purchaseorderdetail_' || to_char(d, 'YYYY_MM'),
      d::text || ' 00:00:00+00',
      (d + interval '1 month')::date::text || ' 00:00:00+00');
  END LOOP;
END $$;
CREATE TABLE purchasing.purchaseorderdetail_other PARTITION OF purchasing.purchaseorderdetail DEFAULT;

DO $$
DECLARE n_old bigint; n_new bigint;
BEGIN
  SELECT count(*) INTO n_old FROM purchasing.purchaseorderdetail_old;
  INSERT INTO purchasing.purchaseorderdetail
    SELECT purchaseorderdetail_id, purchaseorderheader_id, product_id, last_receipt_date,
           is_fully_received, due_date, order_qty, unit_price, received_qty, rejected_qty,
           stocked_qty, created_at, updated_at, is_active, row_version, row_guid
    FROM purchasing.purchaseorderdetail_old;
  GET DIAGNOSTICS n_new = ROW_COUNT;
  IF n_new <> n_old THEN RAISE EXCEPTION 'purchaseorderdetail: copied % of % rows', n_new, n_old; END IF;
END $$;

DROP TABLE purchasing.purchaseorderdetail_old;

COMMIT;
ANALYZE purchasing.purchaseorderdetail;
