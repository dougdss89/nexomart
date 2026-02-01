from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import computed_field
from pathlib import Path

CURRENT_FILE_DIR = Path(__file__).resolve().parent

API_ROOT_DIR = CURRENT_FILE_DIR.data_plataform.dbapi
ENV_FILE_PATH = API_ROOT_DIR / ".env"

class Settings(BaseSettings):
    
    model_config = SettingsConfigDict(
        env_file = ".env",
        env_file_encoding="utf-8",
        extra="ignore" #ignora variáveis extras
        
    )

    #mapeando as variáveis exatas
    
    PGUSER:str
    PGPWD:str
    POSTGREIP:str
    PGPORT:str
    PGDATABASENASME:str
    
    PGDRIVER:str | None=None
    PGJAR:str | None=None
    
    #montando a url
    #evita ma formacao de string com python
    
    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.PGUSER}:{self.PGPWD}@{self.POSTGREIP}:{self.PGPORT}/{self.PGDATABASENAME}"
    
settings = Settings()