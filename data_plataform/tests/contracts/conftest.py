import os
from pathlib import Path

# api.config builds the settings object and an async engine at import time. When there is a real
# api .env (the airflow-worker mounts dbapi/api/.env) the tests use it, so the live-database drift
# check reaches the real Postgres. Only when there is none (CI, a bare checkout) do dummy values
# stand in; those tests then skip because nothing is reachable.
_env_candidates = [
    Path(os.getenv("API_ENV_FILE", "/nonexistent")),
    Path("/workspace/api/.env"),
    Path(__file__).resolve().parents[2] / "dbapi" / "api" / ".env",
]
if not any(p.is_file() for p in _env_candidates):
    for key, value in {
        "PGUSER": "test",
        "PGPWD": "test",
        "POSTGREIP": "localhost",
        "PGPORT": "5432",
        "PGDATABASENAME": "test",
        "MINIO_ENDPOINT": "localhost:9000",
        "MINIO_ACCESS_KEY": "test",
        "MINIO_SECRET_KEY": "test",
    }.items():
        os.environ.setdefault(key, value)
