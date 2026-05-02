from pyspark.sql import SparkSession


def create_spark(app_name: str) -> SparkSession:
    """
    Returns a SparkSession with Delta Lake support.
    spark_catalog  → default catalog (unqualified table refs, Delta format)
    unity          → Unity Catalog OSS (governed namespace, ACL, lineage)
    Both catalogs are pre-configured in spark-defaults.conf.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def create_spark_unity(app_name: str) -> SparkSession:
    """
    Returns a SparkSession scoped to the Unity Catalog OSS 'unity' catalog.
    Use when writing tables that need governance, ACL or Delta Sharing.
    """
    spark = create_spark(app_name)
    spark.sql("USE CATALOG unity")
    return spark
