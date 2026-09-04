-- Repartitions purchasing.purchaseorderheader as RANGE PARTITION BY order_date.
-- Preserves existing data via RENAME + INSERT ... SELECT.
-- No FK constraints reference purchaseorderheader in this schema (purchaseorderdetail/
-- purchaseorderaudit enforce the relationship at the seeder level, not the DB) so no
-- constraint drops are needed before repartitioning.
-- Originally applied 2026-08-13; recovered and re-applied 2026-08-17 (see ../README.md).
BEGIN;

ALTER TABLE purchasing.purchaseorderheader RENAME TO purchaseorderheader_old;
-- Index names are schema-scoped, not table-scoped: rename the old ones out of the way so
-- the new table's CREATE INDEX (same names) doesn't collide mid-transaction.
ALTER INDEX purchasing.ix_audit_trail_action RENAME TO ix_audit_trail_action_old;
ALTER INDEX purchasing.ix_audit_trail_date RENAME TO ix_audit_trail_date_old;
ALTER INDEX purchasing.ix_audit_trail_user_name RENAME TO ix_audit_trail_user_name_old;

CREATE TABLE purchasing.purchaseorderheader (
    purchaseorderheader_id varchar(36) NOT NULL,
    vendor_id integer NOT NULL,
    employee_id integer NOT NULL,
    order_date timestamptz NOT NULL,
    ship_date timestamptz,
    orderfinished_date timestamptz,
    sub_total numeric(19,4) NOT NULL,
    tax_amt numeric(19,4) NOT NULL,
    freight numeric(19,4) NOT NULL,
    total_due numeric(19,4) NOT NULL,
    status varchar(20) NOT NULL,
    audit_trail jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz,
    is_active boolean NOT NULL,
    row_version integer NOT NULL,
    row_guid uuid NOT NULL,
    PRIMARY KEY (purchaseorderheader_id, order_date),
    UNIQUE (row_guid, order_date),
    FOREIGN KEY (vendor_id) REFERENCES purchasing.vendor(vendor_id),
    FOREIGN KEY (employee_id) REFERENCES humanresources.employee(employee_id)
) PARTITION BY RANGE (order_date);

-- order_date = now() - random(1..365 days) per the seeder, a full year of spread. Monthly
-- partitions covering that span plus a one-month buffer; DEFAULT catches anything outside.
CREATE TABLE purchasing.purchaseorderheader_2025_08 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE purchasing.purchaseorderheader_2025_09 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE purchasing.purchaseorderheader_2025_10 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE purchasing.purchaseorderheader_2025_11 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE purchasing.purchaseorderheader_2025_12 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');
CREATE TABLE purchasing.purchaseorderheader_2026_01 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE purchasing.purchaseorderheader_2026_02 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE purchasing.purchaseorderheader_2026_03 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE purchasing.purchaseorderheader_2026_04 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE purchasing.purchaseorderheader_2026_05 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE purchasing.purchaseorderheader_2026_06 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE purchasing.purchaseorderheader_2026_07 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE purchasing.purchaseorderheader_2026_08 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE purchasing.purchaseorderheader_2026_09 PARTITION OF purchasing.purchaseorderheader FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE purchasing.purchaseorderheader_other PARTITION OF purchasing.purchaseorderheader DEFAULT;

CREATE INDEX ix_audit_trail_action ON purchasing.purchaseorderheader ((((audit_trail[1]) ->> 'action'::text)::character varying));
CREATE INDEX ix_audit_trail_date ON purchasing.purchaseorderheader ((((audit_trail[0]) ->> 'date'::text)::character varying));
CREATE INDEX ix_audit_trail_user_name ON purchasing.purchaseorderheader ((((audit_trail[2]) ->> 'user'::text)::character varying));
-- AuditMixin is *supposed* to provide this on every table via a declared_attr Index, but
-- empirically doesn't (see docs/SEEDER_MICROBATCHING.md) — and even if it worked, it
-- wouldn't survive a RENAME-based repartition (stays attached to purchaseorderheader_old,
-- dropped with it). Recreated explicitly, and load-bearing: purchase_purchaseorderdetail.py
-- and purchase_purchaseorderaudit.py both use created_at as their fan-out watermark, since
-- purchaseorderheader_id is a random business ID, not a sequential one.
CREATE INDEX ix_purchaseorderheader_created_at ON purchasing.purchaseorderheader (created_at);

INSERT INTO purchasing.purchaseorderheader (
    purchaseorderheader_id, vendor_id, employee_id, order_date, ship_date, orderfinished_date,
    sub_total, tax_amt, freight, total_due, status, audit_trail, created_at, updated_at,
    is_active, row_version, row_guid
)
SELECT
    purchaseorderheader_id, vendor_id, employee_id, order_date, ship_date, orderfinished_date,
    sub_total, tax_amt, freight, total_due, status, audit_trail, created_at, updated_at,
    is_active, row_version, row_guid
FROM purchasing.purchaseorderheader_old;

DROP TABLE purchasing.purchaseorderheader_old;

COMMIT;
