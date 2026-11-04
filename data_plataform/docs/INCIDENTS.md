# Incident Log

Postmortems for bugs found in production-ish operation of the platform. Newest first.

---

## 2026-07-12 — `dim_customer` silently never written (silver layer)

**Symptom**: `spark.table("iceberglake.silver.dim_customer")` failed with `TABLE_OR_VIEW_NOT_FOUND` from a local notebook, even though the Airflow DAG `customer_dimension_orchestrator` showed every task — including `Write_customer_silver_layer` — as `success`.

**Impact**: Any silver-layer table could silently fail to write while Airflow reported the DAG as green. Confirmed this wasn't customer-specific: the same code pattern existed in all 21 dimension/fact scripts under `spark/jobs/silver_layer_advworks_scripts/`.

### Root causes (three, stacked)

1. **Spark cluster under-scaled.** `spark/docker-compose.yml` is sized for 3 `spark-worker` replicas (12 cores / 24GiB), but the stack had been brought up with the default `WORKERS=1` (`make start` / `make up` without `WORKERS=3`), leaving only 1 worker (4 cores / 8GiB). Jobs got stuck `WAITING` forever with `App ... requires more resource than any of Workers could have`.
   - *Fix*: `docker compose -f spark/docker-compose.yml up -d --scale spark-worker=3`. No code change — just an operational reminder (see README/Makefile).

2. **Hardcoded oversized executor memory.** `spark/jobs/landing_layer_scripts/advcustomer_extract_raw.py` (and its unused mirror in `spark/devapplication/`) had `.config("spark.executor.memory", "12g")` hardcoded in the `SparkSession.builder`, overriding `spark-defaults.conf` (8g). A 12GiB executor can never fit on an 8GiB worker, regardless of worker count — this is a structural deadlock, not a transient capacity issue.
   - *Fix*: removed the hardcoded `.config(...)` calls; the job now inherits `spark-defaults.conf` like its sibling bronze script already did.

3. **Silent write failure + swallowed exit code (the actual reason `dim_customer` never existed).** Reading `iceberglake.bronze.raw_dimcustomer` back for the surrogate-key step crashed the JVM with **SIGSEGV** inside Iceberg's Arrow-vectorized Parquet reader (`VectorizedDictionaryEncodedParquetValuesReader$BaseDictEncodedReader.nextBatch`), a known instability class in `iceberg-spark-runtime-3.5_2.13:1.7.1` when reading dictionary-encoded string columns (customer/address data — city, state, etc. — is exactly that kind of column; `dim_buyer` doesn't hit this because its schema doesn't have the same dictionary-heavy columns). The Spark job lost 4 executors in a row (`Command exited with code 134`) and the write genuinely failed.
   - But `writeCustomerDimension()`'s `except Py4JJavaError` / `except Exception` blocks just **logged and `return`ed a string** instead of raising or calling `exit(1)`. `main()` then logged `"dataframe has been writted successfully"` (false) and returned `"Success"`. The script's `if __name__ == "__main__": main()` **also ignored `main()`'s return value**, so the process exited 0 and Airflow marked the task green — a double layer of the same "swallowed failure" bug.
   - *Fixes*:
     - Set `read.parquet.vectorization.enabled=false` as a **table property** on `iceberglake.bronze.raw_dimcustomer` (baked into `raw_layer_extract_scripts/advcustomer_extract_raw.py`'s `.tableProperty(...)` chain so it survives the table's `createOrReplace()` on every bronze run — an `ALTER TABLE` alone would have been wiped out on the next run).
     - In all 21 `silver_layer_advworks_scripts/adv*.py` write functions: replaced `return "Py4JJavaError"` / `return "UnexpectedError"` with `print_exc(); exit(1)`.
     - In all 21 scripts' entrypoints: `main()` → `result = main(); exit(1) if result != "Success" else None` before the "completed successfully" log line.

### Verification

- Reproduced the SIGSEGV directly in the failing environment (`airflow_worker`, Spark 3.5.8 / JDK 17.0.19 / iceberg-spark-runtime 1.7.1) via a throwaway script; confirmed the table-property fix avoids it (read `raw_dimcustomer`, 18,508 rows, no crash).
- Re-ran `customer_dimension_orchestrator` end-to-end after all fixes: all 5 tasks `success`, and `iceberglake.silver.dim_customer` now has 18,508 real rows.

### Follow-up (not done yet)

Haven't re-run the other 20 dimension/fact DAGs to check whether any of them were *also* silently failing before this fix (unlikely to hit the exact same SIGSEGV, since that's tied to `raw_dimcustomer`'s specific column encoding, but the swallowed-exception bug applied to all of them equally, so any pre-existing failure there would have gone unnoticed too). Worth a sweep.
