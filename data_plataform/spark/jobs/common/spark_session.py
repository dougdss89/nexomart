from pyspark.sql import SparkSession


def create_spark(app_name: str) -> SparkSession:
    """
    Returns a SparkSession with Delta Lake + Iceberg support.
    spark_catalog  → default catalog (unqualified table refs, Delta format, Hive Metastore)
    iceberglake    → Iceberg catalog backed by Hive Metastore (AdventureWorks pipeline)
    Both catalogs are pre-configured in spark-defaults.conf.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()
