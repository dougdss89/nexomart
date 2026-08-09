"""
Silver: iceberglake.bronze.raw_dim_productcategory (Iceberg)
      → iceberglake.silver.dim_productcategory (Iceberg)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, current_timestamp
from configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc


def load_source_configurations(application_name: str):
    try:
        config = load_config()
        spark = (
            SparkSession.builder
            .appName(application_name)
            .getOrCreate()
        )

        return spark

    except Exception as e:
        print(f"Failed to load configurations: {str(e)}")
        print_exc()
        return None, None, None


def build_dimension(spark, table_name: str):
    try:
        df = spark.table(f"iceberglake.bronze.{table_name}")

        dim = df.select(
            (monotonically_increasing_id() + 1).cast("long").alias("category_sk"),
            col("category_id").alias("category_nk"),
            col("category_name"),
            col("category_identifier"),
            col("created_date"),
            col("inserted_date").alias("extracted_date"),
            current_timestamp().alias("dw_load_date"),
        )

        return dim

    except Exception as e:
        print(f"Failed to build dimension: {str(e)}")
        print_exc()
        return None


def write_silver_dimension(spark, dataframe, table_name: str):
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberglake.silver")
        (
            dataframe
            .writeTo(f"iceberglake.silver.{table_name}")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .createOrReplace()
        )
        print(f"Silver: wrote to iceberglake.silver.{table_name}")
    except Exception as e:
        print(f"Failed to write silver: {str(e)}")
        print_exc()
        exit(1)


def spark_stop(spark):
    try:
        if spark:
            spark.stop()
    except Exception as e:
        print(f"Error stopping Spark: {str(e)}")


if __name__ == "__main__":
    spark = None
    try:
        spark = load_source_configurations("silver_dim_productcategory")

        if not spark:
            raise RuntimeError("Failed to load configurations")

        dim = build_dimension(spark, "raw_dim_productcategory")

        if dim is None:
            raise RuntimeError("Failed to build dimension")

        dim.show(5)
        write_silver_dimension(spark, dim, "dim_productcategory")
        print(f"Silver job completed — {dim.count()} rows")

    except Exception as e:
        print(f"Job failed: {str(e)}")
        print_exc()
        exit(1)

    finally:
        spark_stop(spark)
