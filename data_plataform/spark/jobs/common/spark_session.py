from pyspark.sql import SparkSession

def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.catalog.icebergdw", "org.apache.iceberg.spark.SparkCatalog")
        .getOrCreate()
    )