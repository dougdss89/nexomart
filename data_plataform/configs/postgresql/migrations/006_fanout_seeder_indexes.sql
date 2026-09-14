-- Indexes backing the 2026-08-17 fan-out seeder fix (person_customer_email.py,
-- purchase_purchaseorderdetail.py, purchase_purchaseorderaudit.py — see
-- docs/SEEDER_MICROBATCHING.md). Declared on the ORM models too (so a fresh DB built via
-- dbapi/create_tables.py gets them automatically), but that only applies to a brand-new
-- table — an existing DB (or one rebuilt via the migrations/00{1..5} partitioning scripts)
-- needs these created explicitly. Safe to re-run (IF NOT EXISTS).

-- Used by person_customer_email.py to compute MAX(customer_id) quickly for its watermark.
CREATE INDEX IF NOT EXISTS ix_customer_email_customer_id ON person.customer_email (customer_id);

-- Used by purchase_purchaseorderdetail.py / purchase_purchaseorderaudit.py to compute
-- MAX(created_at) quickly for their watermark (the header_id indexes are general-purpose
-- lookup support, not load-bearing for the current fan-out query — created_at is).
CREATE INDEX IF NOT EXISTS ix_purchaseorderdetail_header_id ON purchasing.purchaseorderdetail (purchaseorderheader_id);
CREATE INDEX IF NOT EXISTS ix_purchaseorderaudit_header_id ON purchasing.purchaseorderaudit (purchaseorderheader_id);
CREATE INDEX IF NOT EXISTS ix_purchaseorderdetail_created_at ON purchasing.purchaseorderdetail (created_at);
CREATE INDEX IF NOT EXISTS ix_purchaseorderaudit_created_at ON purchasing.purchaseorderaudit (created_at);
