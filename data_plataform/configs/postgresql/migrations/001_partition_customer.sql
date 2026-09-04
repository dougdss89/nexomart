-- Repartitions person.customer as LIST PARTITION BY country_id.
-- Preserves existing data via RENAME + INSERT ... SELECT (safe to run whether the table
-- currently has 0 rows or millions — it's a straight copy either way).
-- Originally applied 2026-08-13; recovered and re-applied 2026-08-17 after a container
-- recreation wiped it (see ../README.md).
BEGIN;

ALTER SEQUENCE person.customer_customer_id_seq OWNED BY NONE;

ALTER TABLE person.customer RENAME TO customer_old;

-- Index names are schema-scoped, not table-scoped: free the old ones up so the new
-- table's CREATE INDEX (same names) doesn't collide mid-transaction.
ALTER INDEX person.ix_customer_demographics_gin RENAME TO ix_customer_demographics_gin_old;
ALTER INDEX person.ix_customer_dem_addr_city RENAME TO ix_customer_dem_addr_city_old;
ALTER INDEX person.ix_customer_dem_addr_country RENAME TO ix_customer_dem_addr_country_old;
ALTER INDEX person.ix_customer_dem_addr_state RENAME TO ix_customer_dem_addr_state_old;
ALTER INDEX person.ix_customer_dem_buying_contact_name RENAME TO ix_customer_dem_buying_contact_name_old;
ALTER INDEX person.ix_customer_dem_metrics_first_purchase RENAME TO ix_customer_dem_metrics_first_purchase_old;
ALTER INDEX person.ix_customer_dem_metrics_last_purchase RENAME TO ix_customer_dem_metrics_last_purchase_old;
ALTER INDEX person.ix_customer_dem_metrics_total_orders RENAME TO ix_customer_dem_metrics_total_orders_old;
ALTER INDEX person.ix_customer_dem_occupation RENAME TO ix_customer_dem_occupation_old;

CREATE TABLE person.customer (
    customer_id integer NOT NULL DEFAULT nextval('person.customer_customer_id_seq'),
    customer_name varchar(100) NOT NULL,
    last_name varchar(100) NOT NULL,
    social_number varchar(15) NOT NULL,
    birthdate timestamptz NOT NULL,
    customer_type varchar(15) NOT NULL,
    credit_limit numeric(19,4) NOT NULL,
    country_id integer NOT NULL,
    state_province_id integer NOT NULL,
    demographics jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz,
    is_active boolean NOT NULL,
    row_version integer NOT NULL,
    row_guid uuid NOT NULL,
    PRIMARY KEY (customer_id, country_id),
    UNIQUE (row_guid, country_id),
    FOREIGN KEY (country_id) REFERENCES person.countryregion(country_id),
    FOREIGN KEY (state_province_id) REFERENCES person.stateprovince(state_province_id)
) PARTITION BY LIST (country_id);

-- One partition per canonical country (country_id values confirmed against the live
-- person.countryregion seed order). A DEFAULT partition catches anything unexpected
-- instead of failing inserts.
CREATE TABLE person.customer_br PARTITION OF person.customer FOR VALUES IN (1);   -- BR
CREATE TABLE person.customer_ar PARTITION OF person.customer FOR VALUES IN (2);   -- AR
CREATE TABLE person.customer_co PARTITION OF person.customer FOR VALUES IN (3);   -- CO
CREATE TABLE person.customer_us PARTITION OF person.customer FOR VALUES IN (4);   -- US
CREATE TABLE person.customer_ca PARTITION OF person.customer FOR VALUES IN (5);   -- CA
CREATE TABLE person.customer_mx PARTITION OF person.customer FOR VALUES IN (6);   -- MX
CREATE TABLE person.customer_pt PARTITION OF person.customer FOR VALUES IN (7);   -- PT
CREATE TABLE person.customer_es PARTITION OF person.customer FOR VALUES IN (8);   -- ES
CREATE TABLE person.customer_fr PARTITION OF person.customer FOR VALUES IN (9);   -- FR
CREATE TABLE person.customer_de PARTITION OF person.customer FOR VALUES IN (10);  -- DE
CREATE TABLE person.customer_jp PARTITION OF person.customer FOR VALUES IN (11);  -- JP
CREATE TABLE person.customer_au PARTITION OF person.customer FOR VALUES IN (12);  -- AU
CREATE TABLE person.customer_other PARTITION OF person.customer DEFAULT;

-- Expression indexes on the parent propagate to every partition (including future ones
-- attached with the same structure).
CREATE INDEX ix_customer_demographics_gin ON person.customer USING gin (demographics);
CREATE INDEX ix_customer_dem_addr_city ON person.customer ((((demographics -> 'address') ->> 'city')::varchar));
CREATE INDEX ix_customer_dem_addr_country ON person.customer ((((demographics -> 'address') ->> 'country')::varchar));
CREATE INDEX ix_customer_dem_addr_state ON person.customer ((((demographics -> 'address') ->> 'state_name')::varchar));
CREATE INDEX ix_customer_dem_buying_contact_name ON person.customer (((((demographics -> 'social') -> 'buying_contact') ->> 'name')::varchar));
CREATE INDEX ix_customer_dem_metrics_first_purchase ON person.customer ((((demographics -> 'metrics') ->> 'date_of_first_purchase')::varchar));
CREATE INDEX ix_customer_dem_metrics_last_purchase ON person.customer ((((demographics -> 'metrics') ->> 'date_last_purchase')::varchar));
CREATE INDEX ix_customer_dem_metrics_total_orders ON person.customer ((((demographics -> 'metrics') ->> 'count_orders')::integer));
CREATE INDEX ix_customer_dem_occupation ON person.customer ((((demographics -> 'social') ->> 'occupation')::varchar));
-- AuditMixin is *supposed* to provide this on every table via a declared_attr Index, but
-- empirically doesn't (see docs/SEEDER_MICROBATCHING.md) — and even if it worked, it
-- wouldn't survive a RENAME-based repartition (stays attached to customer_old, dropped with
-- it). Recreated explicitly; general-purpose housekeeping here (person_customer_email.py's
-- fan-out uses customer_id as its watermark instead, since it's already a sequential PK).
CREATE INDEX ix_customer_created_at ON person.customer (created_at);

INSERT INTO person.customer (
    customer_id, customer_name, last_name, social_number, birthdate, customer_type,
    credit_limit, country_id, state_province_id, demographics, created_at, updated_at,
    is_active, row_version, row_guid
)
SELECT
    customer_id, customer_name, last_name, social_number, birthdate, customer_type,
    credit_limit, country_id, state_province_id, demographics, created_at, updated_at,
    is_active, row_version, row_guid
FROM person.customer_old;

DROP TABLE person.customer_old;

ALTER SEQUENCE person.customer_customer_id_seq OWNED BY person.customer.customer_id;

COMMIT;
