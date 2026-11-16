"""
Delta Lake helpers: table naming and write patterns for the nexomart/nexo projects.
"""

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def write_delta(
    spark: SparkSession,
    df: DataFrame,
    full_table: str,
    partition_column: str | None = None,
    mode: str = "overwrite",
    options: dict[str, str] | None = None,
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
    # the Hive-metastore-backed spark_catalog. Use the classic path-based writer +
    # register-if-missing instead.
    namespace = f"spark_catalog.{project}_{layer}"
    bucket = f"s3a://deltadw/{project}/{layer}.db"

    spark.sql(f"""CREATE DATABASE IF NOT EXISTS {namespace}
              LOCATION '{bucket}/'""")

    table_path = f"{bucket}/{table}"
    target_table = f"{namespace}.{table}"

    if partition_column is None:
        raise ValueError("DeltaTableWriter needs partition_column")

    writer = (
        df.write.format("delta")
        .option("path", table_path)
        .partitionBy(partition_column)
        .options(**(options or {}))
    )

    if mode == "overwrite":
        writer = (
            writer.option("overwriteSchema", "true").mode("overwrite").options(**(options or {}))
        )
    else:
        writer = writer.mode("append").options(**(options or {}))
    writer.save()

    if not spark.catalog.tableExists(target_table):
        spark.sql(f"""CREATE TABLE {target_table} 
                        USING delta LOCATION '{table_path}' """)


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


def create_clustered_delta(
    spark, df, full_table: str, cluster_by: list[str], options: dict[str, str] | None = None
) -> None:
    """creates an empty liquid-clustered Delta table at the same location write_delta() uses."""

    project, layer, table = full_table.split(".")

    namespace = f"spark_catalog.{project}_{layer}"

    bucket = f"s3a://deltadw/{project}/{layer}.db"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {namespace} LOCATION '{bucket}/' ")
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"{project}_{layer}.{table}")
        .location(f"{bucket}/{table}")
        .addColumns(df.schema)
        .clusterBy(*cluster_by)
        .execute()
    )
