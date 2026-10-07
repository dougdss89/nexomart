-- 011: add the AuditMixin columns to product.productcategory, which predated AuditMixin and so
-- lacked created_at/updated_at/is_active/row_version/row_guid. CLAUDE.md requires every model to
-- inherit AuditMixin, and the dimension-history seeding (api/utils/cli_common.stamp_dimension_history)
-- sets created_at/updated_at on every dimension row -- without these columns the product_category
-- seeder's insert fails with "Unconsumed column names: created_at, updated_at". Small table
-- (~19 rows), no inbound FKs, so a plain ADD COLUMN is instant. The pre-existing created_date column
-- is left untouched (kept for backward compatibility; created_at is the AuditMixin/landing column).
--
-- RUN WITH THE SEEDER PAUSED:
--   docker exec airflow_scheduler airflow dags pause nexomart_seeder_dag
--   docker exec -i pgprojeto psql -U admin -d nexomarket_db -v ON_ERROR_STOP=1 -f 011_productcategory_auditmixin.sql
--   docker exec airflow_scheduler airflow dags unpause nexomart_seeder_dag

BEGIN;

ALTER TABLE product.productcategory
    ADD COLUMN IF NOT EXISTS created_at  timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS updated_at  timestamptz,
    ADD COLUMN IF NOT EXISTS is_active   boolean     NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS row_version integer     NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS row_guid    uuid        NOT NULL DEFAULT gen_random_uuid();

-- AuditMixin declares created_at as index=True and row_guid as unique=True.
CREATE INDEX IF NOT EXISTS ix_productcategory_created_at
    ON product.productcategory (created_at);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_productcategory_row_guid') THEN
        ALTER TABLE product.productcategory
            ADD CONSTRAINT uq_productcategory_row_guid UNIQUE (row_guid);
    END IF;
END $$;

COMMIT;
