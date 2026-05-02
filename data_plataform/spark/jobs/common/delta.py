"""
Delta Lake helpers — write patterns for spark_catalog (default) and Unity Catalog OSS.

Usage examples
--------------
# Write to default catalog (Delta, no governance):
write_delta(df, "s3a://deltadw/bronze/sales_orders")

# Register a managed table under Unity Catalog OSS:
register_unity_table(spark, "unity.bronze.sales_orders", "s3a://deltadw/bronze/sales_orders")
"""

from pyspark.sql import DataFrame, SparkSession


DELTA_WRITE_OPTIONS = {
    "mergeSchema": "true",
    "dataChange": "true",
}


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",
    partition_by: list[str] | None = None,
) -> None:
    writer = df.write.format("delta").mode(mode).options(**DELTA_WRITE_OPTIONS)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def write_delta_table(
    df: DataFrame,
    table: str,
    mode: str = "append",
    partition_by: list[str] | None = None,
) -> None:
    """Write to a named Delta table (spark_catalog or unity catalog)."""
    writer = df.write.format("delta").mode(mode).options(**DELTA_WRITE_OPTIONS)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table)


def register_unity_table(spark: SparkSession, table: str, path: str) -> None:
    """
    Register an existing Delta path as a managed table in Unity Catalog OSS.
    Idempotent: skips if the table already exists.

    table: fully-qualified name  →  unity.<schema>.<table>
    path:  s3a://deltadw/<layer>/<table>
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table}
        USING DELTA
        LOCATION '{path}'
    """)
