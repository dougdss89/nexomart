-- Repartitions warehouse.transactionhistory as RANGE PARTITION BY transaction_date.
-- Preserves existing data via RENAME + INSERT ... SELECT.
-- Originally applied 2026-08-13; recovered and re-applied 2026-08-17 (see ../README.md).
BEGIN;

ALTER TABLE warehouse.transactionhistory RENAME TO transactionhistory_old;

-- Index name is schema-scoped, not table-scoped: free it up so the new table's
-- CREATE INDEX (same name) doesn't collide mid-transaction.
ALTER INDEX warehouse.ix_traceability_info_gin RENAME TO ix_traceability_info_gin_old;

CREATE TABLE warehouse.transactionhistory (
    transaction_id varchar(36) NOT NULL,
    product_id integer NOT NULL,
    reference_order_id integer NOT NULL,
    reference_order_line_id integer,
    transaction_date timestamptz DEFAULT now(),
    transactiontype_id integer NOT NULL,
    quantity integer NOT NULL,
    actual_cost numeric(19,4) NOT NULL,
    traceability_info jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz,
    is_active boolean NOT NULL,
    row_version integer NOT NULL,
    row_guid uuid NOT NULL,
    PRIMARY KEY (transaction_id, transaction_date),
    UNIQUE (row_guid, transaction_date),
    FOREIGN KEY (product_id) REFERENCES product.product(product_id),
    FOREIGN KEY (transactiontype_id) REFERENCES warehouse.transactiontype(transactiontype_id)
) PARTITION BY RANGE (transaction_date);

-- transaction_date = now() - random(0..90 days) per the seeder. Monthly partitions give
-- headroom on both sides; DEFAULT catches anything outside that (including the rare NULL,
-- since transaction_date has no NOT NULL constraint).
CREATE TABLE warehouse.transactionhistory_2026_05 PARTITION OF warehouse.transactionhistory FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE warehouse.transactionhistory_2026_06 PARTITION OF warehouse.transactionhistory FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE warehouse.transactionhistory_2026_07 PARTITION OF warehouse.transactionhistory FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE warehouse.transactionhistory_2026_08 PARTITION OF warehouse.transactionhistory FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE warehouse.transactionhistory_2026_09 PARTITION OF warehouse.transactionhistory FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE warehouse.transactionhistory_other PARTITION OF warehouse.transactionhistory DEFAULT;

CREATE INDEX ix_traceability_info_gin ON warehouse.transactionhistory USING gin (traceability_info);
-- AuditMixin is *supposed* to provide this on every table via a declared_attr Index, but
-- empirically doesn't (see docs/SEEDER_MICROBATCHING.md) — and even if it worked, it
-- wouldn't survive a RENAME-based repartition (stays attached to transactionhistory_old,
-- dropped with it). Recreated explicitly.
CREATE INDEX ix_transactionhistory_created_at ON warehouse.transactionhistory (created_at);

INSERT INTO warehouse.transactionhistory (
    transaction_id, product_id, reference_order_id, reference_order_line_id, transaction_date,
    transactiontype_id, quantity, actual_cost, traceability_info, created_at, updated_at,
    is_active, row_version, row_guid
)
SELECT
    transaction_id, product_id, reference_order_id, reference_order_line_id, transaction_date,
    transactiontype_id, quantity, actual_cost, traceability_info, created_at, updated_at,
    is_active, row_version, row_guid
FROM warehouse.transactionhistory_old;

DROP TABLE warehouse.transactionhistory_old;

COMMIT;
