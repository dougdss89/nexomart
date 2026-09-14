from pyspark.sql import DataFrame

# NOTE: nexo stopped being an Iceberg catalog — Spark now writes it as Delta via the
# native unitycatalog-spark connector (UCSingleCatalog). See spark/jobs/common/delta.py
# for the Delta write helper and spark/conf/spark-defaults.conf.template for the catalog config.


def write_iceberg(df: DataFrame, table: str, mode: str = "append") -> None:
    """Writes a DataFrame to an Iceberg v3 table in the iceberglake catalog.

    Args:
        df: DataFrame to be written.
        table: Qualified name in the format 'iceberglake.<schema>.<table>'.
        mode: 'append' (default) or 'overwrite'.
    """
    writer = df.writeTo(table)
    if mode == "overwrite":
        writer.createOrReplace()
    else:
        writer.append()
