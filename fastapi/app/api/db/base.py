from sqlalchemy.orm import DeclarativeBase


# Esta classe será o ponto central para todos os modelos do seu banco de dados.
# Todos os seus modelos de tabela (Shop, Product, etc.) herdarão dela.
class Base(DeclarativeBase):
    pass
