-- Repartitions sales.salesorder as RANGE PARTITION BY order_date.
-- Preserves existing data via RENAME + INSERT ... SELECT.
-- No FK constraints reference salesorder in this schema (ordereventlog/returnorder/
-- transactionhistory enforce the relationship at the seeder level, not the DB) so no
-- constraint drops are needed before repartitioning.
-- Originally applied 2026-08-13; recovered and re-applied 2026-08-17 (see ../README.md).
BEGIN;

ALTER SEQUENCE sales.salesorder_salesorder_id_seq OWNED BY NONE;

ALTER TABLE sales.salesorder RENAME TO salesorder_old;

CREATE TABLE sales.salesorder (
    salesorder_id integer NOT NULL DEFAULT nextval('sales.salesorder_salesorder_id_seq'),
    salesordernumber varchar(20) NOT NULL,
    customer_id integer NOT NULL,
    saleschannel_id integer NOT NULL,
    specialoffer_id integer NOT NULL,
    store_id integer NOT NULL,
    order_date timestamptz NOT NULL,
    order_status varchar NOT NULL,
    total_amount numeric(19,4) NOT NULL,
    currency_code varchar(3) NOT NULL,
    order_items jsonb NOT NULL DEFAULT '{}'::jsonb,
    payment_info jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz,
    is_active boolean NOT NULL,
    row_version integer NOT NULL,
    row_guid uuid NOT NULL,
    PRIMARY KEY (salesorder_id, order_date),
    UNIQUE (salesordernumber, order_date),
    UNIQUE (row_guid, order_date),
    FOREIGN KEY (saleschannel_id) REFERENCES sales.saleschannel(saleschannel_id),
    FOREIGN KEY (specialoffer_id) REFERENCES sales.specialoffer(special_offer_id),
    FOREIGN KEY (store_id) REFERENCES person.seller(seller_id)
) PARTITION BY RANGE (order_date);

-- order_date = now() - random(1..30 days) per the seeder, so live rows never trail more
-- than ~30 days behind whenever a run happens. Monthly partitions give headroom on both
-- sides; DEFAULT catches anything outside that window instead of failing inserts.
CREATE TABLE sales.salesorder_2026_06 PARTITION OF sales.salesorder FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE sales.salesorder_2026_07 PARTITION OF sales.salesorder FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE sales.salesorder_2026_08 PARTITION OF sales.salesorder FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE sales.salesorder_2026_09 PARTITION OF sales.salesorder FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE sales.salesorder_2026_10 PARTITION OF sales.salesorder FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE sales.salesorder_other PARTITION OF sales.salesorder DEFAULT;

-- AuditMixin is *supposed* to provide this on every table via a declared_attr Index, but
-- empirically doesn't (see docs/SEEDER_MICROBATCHING.md) — and even if it worked, it
-- wouldn't survive a RENAME-based repartition (stays attached to salesorder_old, dropped
-- with it). Recreated explicitly.
CREATE INDEX ix_salesorder_created_at ON sales.salesorder (created_at);

INSERT INTO sales.salesorder (
    salesorder_id, salesordernumber, customer_id, saleschannel_id, specialoffer_id,
    store_id, order_date, order_status, total_amount, currency_code, order_items,
    payment_info, created_at, updated_at, is_active, row_version, row_guid
)
SELECT
    salesorder_id, salesordernumber, customer_id, saleschannel_id, specialoffer_id,
    store_id, order_date, order_status, total_amount, currency_code, order_items,
    payment_info, created_at, updated_at, is_active, row_version, row_guid
FROM sales.salesorder_old;

DROP TABLE sales.salesorder_old;

ALTER SEQUENCE sales.salesorder_salesorder_id_seq OWNED BY sales.salesorder.salesorder_id;

COMMIT;
