"""
Gold: s3a://silver/advworks_db/dim_productcategory (Delta)
    → Delta Lake spark_catalog.gold.gold_dim_productcategory (s3a://deltadw/)

Note: Unity Catalog Iceberg REST is read-only (no CREATE TABLE support).
Gold layer uses spark_catalog (DeltaCatalog + Hive metastore) instead.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc


def load_source_configurations(application_name: str):
    try:
        config = load_config()
        delta_paths = config.get("delta_path", {})
        validate_required_keys(delta_paths, ["silver_layer"], "delta_path")

        spark = (
            SparkSession.builder
            .appName(application_name)
            .getOrCreate()
        )

        return spark, delta_paths.get("silver_layer")

    except Exception as e:
        print(f"Failed to load configurations: {str(e)}")
        print_exc()
        return None, None


def read_silver_dimension(spark, silver_base: str, table_name: str):
    path = f"{silver_base}/advworks_db/{table_name}"
    return spark.read.format("delta").load(path)


def write_gold_delta(spark, dataframe, schema: str, table_name: str):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{schema}")
    full_table = f"spark_catalog.{schema}.{table_name}"
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table)
    )
    print(f"Gold: wrote Delta table {full_table}")


def spark_stop(spark):
    try:
        if spark:
            spark.stop()
    except Exception as e:
        print(f"Error stopping Spark: {str(e)}")


if __name__ == "__main__":
    spark = None
    try:
        spark, silver_base = load_source_configurations("gold_dim_productcategory")

        if not all([spark, silver_base]):
            raise RuntimeError("Failed to load configurations")

        df = read_silver_dimension(spark, silver_base, "dim_productcategory")
        df = df.withColumn("gold_load_date", current_timestamp())

        write_gold_delta(spark, df, "gold", "gold_dim_productcategory")
        print(f"Gold job completed — {df.count()} rows")

    except Exception as e:
        print(f"Job failed: {str(e)}")
        print_exc()
        exit(1)

    finally:
        spark_stop(spark)
