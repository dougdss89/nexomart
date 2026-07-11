"""
Iceberg helpers — write patterns for iceberglake (HiveCatalog) and nexo (Unity Catalog REST).

Usage examples
--------------
# Write to iceberglake bronze:
write_iceberg(spark, df, "iceberglake.bronze.raw_dimbuyer")

# Write to iceberglake silver:
write_iceberg(spark, df, "iceberglake.silver.dim_buyer")
"""
from pyspark.sql import DataFrame, SparkSession


def write_iceberg(
    spark: SparkSession,
    df: DataFrame,
    full_table: str,
    format_version: str = "2",
) -> None:
    namespace = ".".join(full_table.split(".")[:2])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    (
        df.writeTo(full_table)
        .tableProperty("format-version", format_version)
        .tableProperty("write.format.default", "parquet")
        .createOrReplace()
    )
