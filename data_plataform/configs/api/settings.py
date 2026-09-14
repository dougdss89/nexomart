import os
from pathlib import Path

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_api_env() -> str:
    """Locate the API .env file in Docker (/workspace/api/.env) or locally (dbapi/api/.env)."""
    if explicit := os.getenv("API_ENV_FILE"):
        return explicit
    candidates = [
        Path("/workspace/api/.env"),
        Path(__file__).resolve().parents[2] / "dbapi" / "api" / ".env",
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    return ""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_find_api_env(),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # PostgreSQL (asyncpg)
    PGUSER: str
    PGPWD: str
    POSTGREIP: str
    PGPORT: str
    PGDATABASENAME: str
    PGDRIVER: str | None = None
    PGJAR: str | None = None

    # MinIO
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_SECURE: bool = False

    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+asyncpg://{self.PGUSER}:{self.PGPWD}"
            f"@{self.POSTGREIP}:{self.PGPORT}/{self.PGDATABASENAME}"
        )


settings = Settings()
