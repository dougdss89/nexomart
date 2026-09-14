# nexomart_contract

Data contracts for the `nexomart` pipeline, one per stage of the flow:

```
api_contract/     ->  API payload contract (dbapi, FastAPI/Pydantic)
raw_contract/     ->  landing zone contract (MinIO, parquet, schema-on-read)
silver_contract/  ->  cleaned/conformed layer contract (Delta/Iceberg)
gold_contract/    ->  business layer contract (dimensional, consumed by BI/Trino)
```

Each stage has one subfolder per schema (`person/`, `sales/`, `product/`, `purchasing/`, etc. —
the same 7 domains from `CLAUDE.md`), and inside it one `.yml` per table.

## How to use

1. Copy the `_TEMPLATE.yml` for the stage you want to contract into `<stage>/<schema>/<table>.yml`.
2. Fill in the fields — the template has comments explaining what each block expects.
3. Use `person/customer.yml` in each of the 4 folders as a filled real example — it shows how
   the SAME data (`person.customer`) changes shape and guarantees at each stage:
   - **api**: the payload FastAPI validates via Pydantic before writing to Postgres.
   - **raw**: how that same data lands as parquet in MinIO (includes a real project gotcha: the
     `demographics` field arrives as a raw JSON STRING, not a struct — see the comment in the file).
   - **silver**: how we expect it to look after cleaning/typing (not yet implemented in
     `spark/jobs/nexomart/silver/person/` — the contract here documents the INTENT before the job
     exists, which is one of the advantages of writing the contract first).
   - **gold**: the final dimensional shape, consumed by Trino/BI.

## Why YAML and not just the database schema

The point of a data contract isn't to re-document the `CREATE TABLE` — it's to record the
things about the data that the physical schema alone doesn't say: who owns it, what's PII,
what quality rule must pass, how often it refreshes, what counts as a breaking change, and who
gets notified. The YAML stays versioned in git next to the code, reviewable in a PR like any
other schema change.
