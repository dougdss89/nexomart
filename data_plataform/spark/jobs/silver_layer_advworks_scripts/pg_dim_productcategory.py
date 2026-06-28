"""
Silver: s3a://bronze/advworks_rawdb/raw_dim_productcategory (Delta)
      → s3a://silver/advworks_db/dim_productcategory (Delta)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, current_timestamp
from configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc


def load_source_configurations(application_name: str):
    try:
        config = load_config()
        delta_paths = config.get("delta_path", {})
        validate_required_keys(delta_paths, ["raw_icebergdb", "silver_layer"], "delta_path")

        spark = (
            SparkSession.builder
            .appName(application_name)
            .getOrCreate()
        )

        return spark, delta_paths.get("raw_icebergdb"), delta_paths.get("silver_layer")

    except Exception as e:
        print(f"Failed to load configurations: {str(e)}")
        print_exc()
        return None, None, None


def build_dimension(spark, bronze_base: str, table_name: str):
    try:
        path = f"{bronze_base}/advworks_rawdb/{table_name}"
        df = spark.read.format("delta").load(path)

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


def write_silver_dimension(dataframe, silver_base: str, table_name: str):
    try:
        path = f"{silver_base}/advworks_db/{table_name}"
        (
            dataframe.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(path)
        )
        print(f"Silver: wrote to {path}")
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
        spark, bronze_base, silver_base = load_source_configurations("silver_dim_productcategory")

        if not all([spark, bronze_base, silver_base]):
            raise RuntimeError("Failed to load configurations")

        dim = build_dimension(spark, bronze_base, "raw_dim_productcategory")

        if dim is None:
            raise RuntimeError("Failed to build dimension")

        dim.show(5)
        write_silver_dimension(dim, silver_base, "dim_productcategory")
        print(f"Silver job completed — {dim.count()} rows")

    except Exception as e:
        print(f"Job failed: {str(e)}")
        print_exc()
        exit(1)

    finally:
        spark_stop(spark)
