-- Retention: drop monthly RANGE partitions that are BOTH older than the retention window
-- AND fully landed (upper bound <= the table's ingestion watermark). DROP is instant and returns
-- space to the filesystem; a partition is only removed once its whole range is already in the
-- MinIO landing, so ingestion (which reads COALESCE(updated_at, created_at) > watermark and never
-- looks back) never re-reads it -> no duplication. Generic: auto-covers every RANGE-partitioned
-- table that has a row in ingestion_control.watermarks, so partitioning a new table and giving it
-- a watermark is all that's needed to enroll it. DEFAULT partitions and future/empty ones never
-- match. Run via: docker exec -i pgprojeto psql -U <user> -d nexomarket_db -v ON_ERROR_STOP=1 -f retention.sql
-- Retention window is 45 days (see cutoff below).
DO $$
DECLARE
  r record;
  cutoff timestamptz := now() - interval '45 days';
  wm timestamptz;
  upper_txt text;
  upper_ts timestamptz;
  freed text;
  dropped int := 0;
BEGIN
  FOR r IN
    SELECT np.nspname AS schema, p.relname AS parent, c.oid AS child_oid, c.relname AS child,
           pg_get_expr(c.relpartbound, c.oid) AS bound
    FROM pg_partitioned_table pt
    JOIN pg_class p ON p.oid = pt.partrelid
    JOIN pg_namespace np ON np.oid = p.relnamespace
    JOIN pg_inherits i ON i.inhparent = p.oid
    JOIN pg_class c ON c.oid = i.inhrelid
    WHERE pt.partstrat = 'r'            -- RANGE only (person.customer is LIST; untouched)
  LOOP
    IF r.bound = 'DEFAULT' THEN CONTINUE; END IF;
    upper_txt := substring(r.bound from 'TO \(''([^'']+)''');
    IF upper_txt IS NULL THEN CONTINUE; END IF;
    BEGIN
      upper_ts := upper_txt::timestamptz;
    EXCEPTION WHEN others THEN CONTINUE;
    END;

    SELECT last_watermark INTO wm
    FROM ingestion_control.watermarks
    WHERE schema_name = r.schema AND table_name = r.parent;
    IF wm IS NULL THEN CONTINUE; END IF;   -- never drop a table we have not landed yet

    IF upper_ts <= cutoff AND upper_ts <= wm THEN
      freed := pg_size_pretty(pg_total_relation_size(r.child_oid));
      RAISE NOTICE 'DROP %.% (upper=%, watermark=%, freed=%)', r.schema, r.child, upper_ts, wm, freed;
      EXECUTE format('DROP TABLE %I.%I', r.schema, r.child);
      dropped := dropped + 1;
    END IF;
  END LOOP;
  RAISE NOTICE 'retention done: % partition(s) dropped (cutoff=%)', dropped, cutoff;
END $$;
