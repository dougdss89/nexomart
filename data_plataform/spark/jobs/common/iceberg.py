from pyspark.sql import DataFrame


def write_iceberg(df: DataFrame, table: str, mode: str = "append") -> None:
    """Escreve um DataFrame em tabela Iceberg v3 no catálogo nexo.

    Args:
        df: DataFrame a ser escrito.
        table: Nome qualificado no formato 'nexo.<schema>.<table>'.
        mode: 'append' (padrão) ou 'overwrite'.
    """
    writer = df.writeTo(table)
    if mode == "overwrite":
        writer.createOrReplace()
    else:
        writer.append()
