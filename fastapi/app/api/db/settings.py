import os
from urllib.parse import quote_plus

from pydantic import ValidationError, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict
from app.api.v1.core.config import settings

# Determina qual arquivo .env usar com base na variável de ambiente TESTING.
# Isso permite configurações separadas para desenvolvimento e teste.
# O Pytest (via conftest.py) é configurado para definir TESTING=true.
env_file = ".env.test" if os.getenv("TESTING") else ".env"


class Settings(BaseSettings):
    """
    Carrega e valida as variáveis de ambiente do arquivo .env apropriado.
    """

    model_config = SettingsConfigDict(
        # O caminho é relativo à raiz do projeto, de onde o pytest é executado.
        env_file=env_file,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    DB_USER: str = settings.DB_USER
    DB_PASSWORD: str = settings.DB_PASSWORD
    DB_HOST: str = settings.DB_HOST
    DB_PORT: int = settings.DB_PORT
    DB_NAME: str = settings.DB_NAME

    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        """
        Gera a string de conexão para o SQLAlchemy.
        A senha é tratada com 'quote_plus' para lidar com caracteres especiais
        de forma segura, prevenindo erros de autenticação.
        """
        # Trata a senha para que seja segura para uso em uma URL
        safe_password = quote_plus(self.DB_PASSWORD)

        return (
            f"postgresql+psycopg2://{self.DB_USER}:{safe_password}@"
            f"{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )


try:
    settings = Settings()

except ValidationError as e:
    print("ERRO DE VALIDAÇÃO: Não foi possível carregar .env.")
    print("Verifique se o arquivo '.env' está na raiz do projeto.")
    print(f"Detalhes do erro: {e}")
    raise
