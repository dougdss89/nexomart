from database import db_connection
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


def main():
    """
    Função principal para demonstrar o uso da conexão com o banco de dados.
    """
    print('\n--- INICIANDO APLICAÇÃO (SQLAlchemy) ---')

    print('Testando conexão com o banco de dados...')
    if not db_connection.test_connection():
        print('Conexão falhou. Encerrando aplicação.')
        return
    print('✅ Conexão bem-sucedida.')

    print('\n--- EXECUTANDO UMA CONSULTA ---')

    try:
        with db_connection.get_session() as session:
            query = text('SELECT version();')
            postgres_version = session.execute(query).scalar_one()
            print(f'🔍 Versão do servidor PostgreSQL: {postgres_version}')

    except SQLAlchemyError as e:
        print(f'Ocorreu um erro de SQLAlchemy durante a consulta: {e}')

    except Exception as e:
        print(f' Um erro inesperado ocorreu: {e}')


if __name__ == '__main__':
    main()
