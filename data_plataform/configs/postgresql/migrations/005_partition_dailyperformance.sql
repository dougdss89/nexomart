-- Repartitions marketing.dailyperformance as RANGE PARTITION BY date_ref.
-- Preserves existing data via RENAME + INSERT ... SELECT.
-- Originally applied 2026-08-13; recovered and re-applied 2026-08-17 (see ../README.md).
-- (The original run also carried a `zero_damaged_pages` workaround for a one-off disk
-- corruption incident on 2026-08-13 — irrelevant on a freshly created table, omitted here.)
BEGIN;

ALTER SEQUENCE marketing.dailyperformance_performance_id_seq OWNED BY NONE;

ALTER TABLE marketing.dailyperformance RENAME TO dailyperformance_old;
-- Index names are schema-scoped, not table-scoped: rename the old ones out of the way so
-- the new table's CREATE INDEX (same names) doesn't collide mid-transaction.
ALTER INDEX marketing.ix_performance_metrics_likes RENAME TO ix_performance_metrics_likes_old;
ALTER INDEX marketing.ix_performance_metrics_quality_score RENAME TO ix_performance_metrics_quality_score_old;
ALTER INDEX marketing.ix_performance_metrics_shares RENAME TO ix_performance_metrics_shares_old;

CREATE TABLE marketing.dailyperformance (
    performance_id integer NOT NULL DEFAULT nextval('marketing.dailyperformance_performance_id_seq'),
    campaign_id integer NOT NULL,
    ad_group_id integer NOT NULL,
    ad_id integer NOT NULL,
    date_ref date NOT NULL,
    cost numeric(19,4) NOT NULL,
    impressions integer NOT NULL,
    clicks integer NOT NULL,
    conversions integer NOT NULL,
    conversion_value numeric(19,2) NOT NULL,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz,
    is_active boolean NOT NULL,
    row_version integer NOT NULL,
    row_guid uuid NOT NULL,
    PRIMARY KEY (performance_id, date_ref),
    UNIQUE (row_guid, date_ref),
    FOREIGN KEY (ad_group_id) REFERENCES marketing.marketingadgroup(ad_group_id),
    FOREIGN KEY (ad_id) REFERENCES marketing.advertise(ad_id),
    FOREIGN KEY (campaign_id) REFERENCES marketing.marketingcampaign(campaign_id)
) PARTITION BY RANGE (date_ref);

-- date_ref = trailing 7-30 days of history per ad, generated relative to "today" at seed
-- time. Monthly partitions with a buffer month on each side; DEFAULT catches the rest.
CREATE TABLE marketing.dailyperformance_2026_07 PARTITION OF marketing.dailyperformance FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE marketing.dailyperformance_2026_08 PARTITION OF marketing.dailyperformance FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE marketing.dailyperformance_2026_09 PARTITION OF marketing.dailyperformance FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE marketing.dailyperformance_other PARTITION OF marketing.dailyperformance DEFAULT;

CREATE INDEX ix_performance_metrics_likes ON marketing.dailyperformance (((metrics ->> 'likes'::text)::integer));
CREATE INDEX ix_performance_metrics_quality_score ON marketing.dailyperformance (((metrics ->> 'quality_score'::text)::integer));
CREATE INDEX ix_performance_metrics_shares ON marketing.dailyperformance (((metrics ->> 'shares'::text)::integer));
-- AuditMixin is *supposed* to provide this on every table via a declared_attr Index, but
-- empirically doesn't (see docs/SEEDER_MICROBATCHING.md) — and even if it worked, it
-- wouldn't survive a RENAME-based repartition (stays attached to dailyperformance_old,
-- dropped with it). Recreated explicitly.
CREATE INDEX ix_dailyperformance_created_at ON marketing.dailyperformance (created_at);

INSERT INTO marketing.dailyperformance (
    performance_id, campaign_id, ad_group_id, ad_id, date_ref, cost, impressions, clicks,
    conversions, conversion_value, metrics, created_at, updated_at, is_active, row_version, row_guid
)
SELECT
    performance_id, campaign_id, ad_group_id, ad_id, date_ref, cost, impressions, clicks,
    conversions, conversion_value, metrics, created_at, updated_at, is_active, row_version, row_guid
FROM marketing.dailyperformance_old;

DROP TABLE marketing.dailyperformance_old;

ALTER SEQUENCE marketing.dailyperformance_performance_id_seq OWNED BY marketing.dailyperformance.performance_id;

COMMIT;
