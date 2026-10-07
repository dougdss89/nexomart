# MinIO

S3-compatible object storage for the platform. Backs the Landing, Bronze, Silver, and Gold
layers, plus Spark's event log for the History Server. Service definition:
`minio/docker-compose.yml`.

| | |
|---|---|
| Console | http://localhost:9001 |
| API | http://localhost:9000 |
| Credentials | `configs/secrets.env` (`MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`) |

## Storage

Data lives on a dedicated disk (`F:\WSLDisks\minio-data.vhdx`) mounted in WSL2 at `/mnt/wsl/minio-f` and bind-mounted into the container via `MINIO_DATA_DIR` in `configs/secrets.env`. See `scripts/windows/README.md` and README Known Issues #9.

## Buckets

Buckets are provisioned automatically by the `minio-init` service every time MinIO comes up
(`make up` or `make up-minio`). It is a one-shot container that runs `mc mb --ignore-existing`
for each bucket and exits, the same pattern as `airflow-init`. Idempotent, safe to run again
against buckets that already exist.

- **`spark-events`** is required and hardcoded in `minio-init` (not read from a variable): the
  Spark History Server reads `spark.eventLog.dir=s3a://spark-events/apps`
  (`spark/conf/spark-defaults.conf.template`) and fails on boot without it.
- **Everything else** comes from `MINIO_EXTRA_BUCKETS` in `configs/secrets.env`, a
  comma-separated list with no spaces:

  ```bash
  MINIO_EXTRA_BUCKETS=landing,bronze,silver,iceberg,deltadw
  ```

  To add a new bucket for a new pipeline or layer, append its name to this variable in both
  `secrets.env` and `secrets.env.example`. No code or compose file needs to change. The bucket
  is created on the next `make up` or `make up-minio`.

```bash
# Re-provision buckets without touching the rest of the stack
make up-minio
```

## Client configuration (`configs/api/minio.py`)

The shared `Minio` client used by the ingestion and seeder scripts is built with an explicit
`http_client`:

```python
_minio_http_client = urllib3.PoolManager(
    timeout=urllib3.Timeout(connect=10, read=60),
    retries=urllib3.Retry(total=0),
)
```

Without this, the SDK falls back to a `urllib3.PoolManager` with no read or connect timeout. A
stalled TCP connection during `put_object()` then blocks the calling thread forever with no
exception raised, which also means the caller's own retry-with-backoff logic
(`write_on_minio()` in the same file) never gets a chance to run, since it only fires on a
caught exception. This was found live on 2026-09-04, chasing an ingestion task stuck for over
40 minutes. Full incident: `docs/INGESTION_INCREMENTAL.md`.

`write_on_minio()` catches `S3Error` (a real error response from MinIO) as well as
`urllib3.exceptions.HTTPError` and `OSError`, the connection-level failures the timeout now
actually raises instead of hanging on.

## Known issue: container healthy but unreachable on `localhost:9000`/`:9001`

`docker ps` can show `minio-minio-1` as `Up (healthy)` while `curl localhost:9000` or `:9001`
times out or refuses. Check for a mundane host-level port collision first:

```bash
ss -tlnp | grep -E ':9000|:9001'
```

Something else on the host (a local dev server, another container, a Jupyter kernel) can already
own those ports, which silently blocks Docker's publish without raising an error at container
start. If nothing else owns the port, this can also be a genuine Docker port-publish failure
after a Docker Desktop/WSL2 network hiccup. Fix either way:

```bash
docker compose --env-file configs/secrets.env -f minio/docker-compose.yml up -d --force-recreate minio
```

## `quarantine` bucket

Rows that a seeder's Pydantic schema rejects, or that the table would refuse, never reach Postgres. They are written to the
`quarantine` bucket as Parquet instead (`dbapi/api/utils/quarantine.py`), so a generator or schema
bug does not use up database disk.

```
quarantine/nexomart/<schema>/<table>/<YYYY-MM-DD>/rejected-<timestamp>-<id>.parquet
```

| Column | Content |
|---|---|
| `payload` | the original row as JSON text, exactly as the seeder built it (outbox style, so it can be replayed) |
| `errors` | Pydantic's error list as JSON (field path, message, type) |
| `stage` | `pydantic` (the schema rejected it while it was built) or `physical` (the table would refuse it, caught after chaos and before the COPY) |
| `table_name` | `schema.table` |
| `rejected_at` | UTC timestamp |

How a seeder uses it: `with q.row(payload):` around the validation, and `q.finish()` after generating
the rows and before the insert (the physical check is added by the insert helpers). A rejected row is quarantined and the seeder continues with the valid
ones. If more than 1% of the rows are rejected the task fails, after saving the rejected rows, because
that rate points to a bug and not to a few stray rows. The limit can be changed with the
`QUARANTINE_MAX_REJECT_RATE` environment variable. If MinIO cannot be written, the task fails instead
of dropping the rows.

The physical check runs after chaos, right before the API to Postgres boundary. It drops rows that
would always fail at the database: NULL in a NOT NULL column (or a missing one with no default), text
longer than a `varchar(n)`, a number that overflows `numeric(p,s)`, an integer out of range. Without it,
one such row aborts the whole COPY. It is called inside `bulk_copy` and `insert_in_batches`
(`api/utils/cli_common.py`), and with `physical_gate(Model, rows)` in the seeders that use `insert()`
directly. Physical rejects count toward the same 1% limit as Pydantic rejects.

Rows created on purpose by chaos engineering are not quarantined: chaos runs after validation.

Fan-out seeders (`customer_email`, `purchaseorderdetail`, `purchaseorderaudit`) advance a watermark, so
a quarantined child row is not retried automatically. Replay it from the bucket after the fix.

Read a file:

```python
import pyarrow.parquet as pq
pq.read_table("rejected-....parquet").to_pandas()
```
