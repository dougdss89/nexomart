from pyspark.sql import DataFrame


def write_iceberg(df: DataFrame, table: str, mode: str = "append") -> None:
    """Writes a DataFrame to an Iceberg v3 table in the nexo catalog.

    Args:
        df: DataFrame to be written.
        table: Qualified name in the format 'nexo.<schema>.<table>'.
        mode: 'append' (default) or 'overwrite'.
    """
    writer = df.writeTo(table)
    if mode == "overwrite":
        writer.createOrReplace()
    else:
        writer.append()
