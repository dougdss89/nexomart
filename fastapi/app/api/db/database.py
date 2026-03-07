from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from app.api.db.settings import settings


class DatabaseConnection:
    """
    Classe que gerencia a conexão com o banco de dados PostgreSQL
    usando SQLAlchemy,
    otimizada para trabalhar com um pooler de conexões
    externo como o PgBouncer.
    """

    _engine: Engine | None = None
    _session_factory: sessionmaker | None = None

    def __init__(self, db_url: str):
        """
        Inicializa a conexão com o banco de dados.
        :param db_url: A string de conexão completa.
        """
        try:
            self._engine = create_engine(
                db_url,
                echo=False,
            )
            self._session_factory = sessionmaker(
                autocommit=False, autoflush=False, bind=self._engine
            )
        except Exception as e:
            print(f'Erro inesperado ao criar o engine do SQLAlchemy: {e}')
            raise

    def get_session(self):
        if not self._session_factory:
            raise RuntimeError('A fábrica de sessões não foi inicializada.')
        return self._session_factory()

    def test_connection(self) -> bool:
        if not self._engine:
            print('Engine não inicializado. Não é possível testar a conexão.')
            return False
        try:
            with self._engine.connect() as connection:
                result = connection.execute(text('SELECT 1'))
                return result.scalar_one() == 1
        except OperationalError as e:
            print('Erro operacional ao conectar com o PgBouncer.')
            print('Verifique se as credenciais e o endereço estão corretos.')
            print(f'Detalhe do erro: {e}')
            return False
        except Exception as e:
            print(f'Um erro inesperado ocorreu ao testar a conexão: {e}')
            return False


try:
    db_connection = DatabaseConnection(db_url=settings.DATABASE_URL)
except Exception:
    print('ERRO FATAL ao inicializar a conexão em database.py. Encerrando.')
    raise
