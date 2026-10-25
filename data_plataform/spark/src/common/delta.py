"""
Iceberg helpers — write patterns for iceberglake (HiveCatalog).

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


def write_delta(
    spark: SparkSession,
    df: DataFrame,
    full_table: str,
    mode: str = "overwrite",
) -> None:
    """Writes a DataFrame to a Delta table under nexo or nexomart.

    Usage examples
    --------------
    write_delta(spark, df, "nexo.gold.gold_dim_buyer")
    write_delta(spark, df, "nexomart.gold.fact_salesorder")

    Neither project has a Spark catalog of its own — DeltaCatalog only works registered as
    "spark_catalog" (see spark/conf/spark-defaults.conf.template for why). Both write instead
    to a dedicated spark_catalog.<project>_<layer> database, read by Trino via the matching
    delta_lake catalog (trino/catalog/nexo.properties / nexomart.properties).

    Args:
        df: DataFrame to be written.
        full_table: Qualified name '<project>.<layer>.<table>' — project is 'nexo' or 'nexomart'.
        mode: 'overwrite' (default, replaces the table) or 'append'.
    """
    project, layer, table = full_table.split(".")

    # DataFrameWriterV2 (.writeTo().createOrReplace()) doesn't support truncate/replace against
    # the Hive-metastore-backed spark_catalog — same limitation write_gold_delta() works around
    # in spark/jobs/advworks/gold_layer_scripts/advgold_dim_productcategory.py. Use the classic
    # path-based writer + register-if-missing instead.
    namespace = f"spark_catalog.{project}_{layer}"
    bucket = f"s3a://deltadw/{project}/{layer}.db"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {namespace} LOCATION '{bucket}/'")
    table_path = f"{bucket}/{table}"
    target_table = f"{namespace}.{table}"

    writer = df.write.format("delta").option("path", table_path)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true").mode("overwrite")
    else:
        writer = writer.mode("append")
    writer.save()

    if not spark.catalog.tableExists(target_table):
        spark.sql(f"CREATE TABLE {target_table} USING delta LOCATION '{table_path}'")


def qualified_table_name(project: str, layer: str, schema: str, table: str) -> str:
    """Builds the compact 'full_table' encoding write_delta() expects as input
    ('<project>.<layer>.<schema>_<table>') — NOT a literal Spark-resolvable name. write_delta()
    is the only place that knows how to translate it into spark_catalog.<project>_<layer>.<table>.
    Anything that talks to Spark directly (DeltaTable.forName, spark.catalog.tableExists, a bare
    SELECT) needs resolved_table_name() below instead, or it hits the same "catalog not found"
    error registering a catalog literally named <project> would (see spark-defaults.conf.template)."""
    return f"{project}.{layer}.{schema}_{table}"


def resolved_table_name(project: str, layer: str, schema: str, table: str) -> str:
    """The literal identifier Spark can resolve: spark_catalog.<project>_<layer>.<schema>_<table>.
    Use this for DeltaTable.forName, spark.catalog.tableExists, or any direct SQL/read — anything
    that doesn't go through write_delta()."""
    return f"spark_catalog.{project}_{layer}.{schema}_{table}"
