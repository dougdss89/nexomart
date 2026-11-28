"""Reads the landing manifest: which Parquet files the ingestion wrote since a given point in time.

`ingestion_control.landing_runs` (Postgres, written by dbapi/api/seeder/ingestions/ingestion_common.py
in the same transaction that advances the table's watermark) holds one row per ingestion run and
table: the files it wrote (relative to `<project>/<schema>/<table>/`) and the window `(from_wm,
to_wm]` of row stamps it covers. Consecutive runs chain: each run's `from_wm` is the previous
run's `to_wm`.

Why: listing the landing costs several disk seeks per object on MinIO's spinning disk (the
person.customer Bronze listing stage took ~90 s per task over 2,514 day folders, 2026-11-20). With
the manifest an incremental load knows exactly which files are new and never lists anything.
"""

import os
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from spark.src.common.logger import get_logger

# compact_landing.py never merges parts younger than its --grace-hours (default 48), so any file
# written after the Bronze table's last row is still there as long as that run is younger than the
# window. Keep this in sync with --grace-hours; the safety margin covers clock/run-length skew.
GRACE_HOURS = int(os.environ.get("LANDING_GRACE_HOURS", "48"))
SAFETY_HOURS = 6
_PG_ENV = ("PGUSER", "PGPWD", "POSTGREIP", "PGPORT", "PGDATABASENAME")

_logger = get_logger("landing_manifest")


def _pg_settings() -> dict[str, str] | None:
    """The Postgres connection settings, from the environment or else from the same API .env file
    dbapi's Settings reads (dbapi/api/.env, mounted at /dockervol/dbapi/api/.env). Parsed by hand:
    the Spark driver's Python has no pydantic-settings."""
    values = {name: os.environ.get(name, "") for name in _PG_ENV}
    if not all(values.values()):
        path = os.environ.get("API_ENV_FILE") or "/dockervol/dbapi/api/.env"
        try:
            with open(path, encoding="utf-8") as env_file:
                for line in env_file:
                    key, sep, value = line.strip().partition("=")
                    if sep and key in values and not values[key]:
                        values[key] = value.strip().strip("\"'")
        except OSError:
            return None
    return values if all(values.values()) else None


def _jdbc_options() -> dict[str, str] | None:
    pg = _pg_settings()
    if pg is None:
        return None
    return {
        "url": f"jdbc:postgresql://{pg['POSTGREIP']}:{pg['PGPORT']}/{pg['PGDATABASENAME']}",
        "user": pg["PGUSER"],
        "password": pg["PGPWD"],
        "driver": "org.postgresql.Driver",
    }


def unconsumed_paths(
    spark: SparkSession, schema: str, table: str, last_ns: int
) -> list[str] | None:
    """Files (relative to the table folder) of every ingestion run that started after the run
    containing `last_ns`, the newest row stamp (epoch nanoseconds) the target already holds.

    Returns None whenever the manifest can't prove it is complete, and the caller must read the
    whole landing instead:
      - the Postgres connection settings or the table are missing;
      - no run covers `last_ns` (data landed before manifests existed, or the run was pruned);
      - the chain of runs after it has a hole;
      - the first run to read is old enough that the compactor may have merged its parts.
    An empty list means the target is up to date."""
    options = _jdbc_options()
    if options is None:
        _logger.warning("PG settings not in the environment; manifest skipped")
        return None

    last_us = last_ns // 1000
    query = f"""
        SELECT run_ts,
               (extract(epoch FROM from_wm) * 1000000)::bigint AS from_us,
               (extract(epoch FROM to_wm) * 1000000)::bigint AS to_us,
               paths
        FROM ingestion_control.landing_runs
        WHERE schema_name = '{schema}' AND table_name = '{table}'
          AND (extract(epoch FROM to_wm) * 1000000)::bigint >= {last_us}
        ORDER BY from_wm
    """
    try:
        runs = spark.read.format("jdbc").options(**options).option("query", query).load().collect()
    except Exception as e:  # missing table, driver or connection: the glob read still works
        _logger.warning(
            f"manifest unavailable for {schema}.{table} ({type(e).__name__}); using glob"
        )
        return None

    if not runs:
        _logger.info(f"{schema}.{table}: no run covers the target's last stamp; using glob")
        return None
    anchor, newer = runs[0], runs[1:]
    if not anchor.from_us < last_us <= anchor.to_us:
        _logger.info(f"{schema}.{table}: last stamp falls in a manifest gap; using glob")
        return None
    previous_end = anchor.to_us
    for run in newer:
        if run.from_us != previous_end:
            _logger.info(f"{schema}.{table}: manifest chain broken at {run.run_ts}; using glob")
            return None
        previous_end = run.to_us
    if newer:
        age_hours = (datetime.now(UTC).timestamp() * 1e6 - newer[0].to_us) / 3.6e9
        if age_hours >= GRACE_HOURS - SAFETY_HOURS:
            _logger.info(
                f"{schema}.{table}: first unread run is {age_hours:.0f}h old, files may be "
                "compacted; using glob"
            )
            return None

    paths = [path for run in newer for path in run.paths]
    _logger.info(f"{schema}.{table}: {len(newer)} new runs, {len(paths):,} files (no listing)")
    return paths
