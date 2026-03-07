from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import computed_field
from pathlib import Path

CURRENT_FILE_DIR = Path(__file__).resolve().parent

# Usando barras para compatibilidade de caminhos
API_ROOT_DIR = CURRENT_FILE_DIR / "data_plataform" / "dbapi"
ENV_FILE_PATH = API_ROOT_DIR / ".env"

class Settings(BaseSettings):
    
    model_config = SettingsConfigDict(
        env_file = str(ENV_FILE_PATH),
        env_file_encoding="utf-8",
        extra="ignore" # ignora variáveis extras no .env
    )

    # === Variáveis do PostgreSQL ===
    PGUSER: str
    PGPWD: str
    POSTGREIP: str
    PGPORT: str
    PGDATABASENAME: str 
    
    PGDRIVER: str | None = None
    PGJAR: str | None = None
    
    # === NOVAS Variáveis do MinIO ===
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_SECURE: bool = False # False significa que não vai exigir HTTPS (ideal para local/Docker)
    
    # montando a url do banco
    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.PGUSER}:{self.PGPWD}@{self.POSTGREIP}:{self.PGPORT}/{self.PGDATABASENAME}"
    
settings = Settings()