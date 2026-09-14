"""Lands every table of the `purchasing` schema into MinIO's nexomart landing zone.

Run manually:
    uv run python dbapi/api/seeder/ingestions/purchasing_ingestion.py [--chunksize N]

Triggered by Airflow right after the seeders (see the `ingestion` TaskGroup in
airflow/dags/nexomart/seeder/nexomart_seeder_dag.py). See ingestion_common.py for the shared logic.
"""

import argparse

from api.seeder.ingestions.ingestion_common import DEFAULT_CHUNKSIZE, ingest_schema

SCHEMA = "purchasing"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"Lands the '{SCHEMA}' schema into the nexomart landing zone")
    parser.add_argument(
        "--chunksize", type=int, default=DEFAULT_CHUNKSIZE,
        help=f"Rows per Parquet part file (default: {DEFAULT_CHUNKSIZE:,})",
    )
    args = parser.parse_args()
    ingest_schema(SCHEMA, chunksize=args.chunksize)
