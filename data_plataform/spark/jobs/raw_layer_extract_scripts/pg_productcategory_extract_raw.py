"""
Bronze/Raw: s3a://landing/productcategory (Parquet) → s3a://bronze/advworks_rawdb/raw_dim_productcategory (Delta)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc


def load_source_connections():
    try:
        config = load_config()
        hadoop_paths = config.get("hadoop_paths", {})
        delta_paths  = config.get("delta_path", {})

        validate_required_keys(hadoop_paths, ["minio_landing"], "hadoop_paths")
        validate_required_keys(delta_paths,  ["raw_icebergdb"], "delta_path")

        spark = (
            SparkSession.builder
            .appName("pg_productcategory_raw")
            .getOrCreate()
        )

        return spark, hadoop_paths.get("minio_landing"), delta_paths.get("raw_icebergdb")

    except Exception as e:
        print(f"Failed to load connections: {str(e)}")
        print_exc()
        return None, None, None


def write_to_delta(dataframe, bronze_base: str, table_name: str):
    try:
        path = f"{bronze_base}/advworks_rawdb/{table_name}"
        (
            dataframe.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(path)
        )
        print(f"Bronze: wrote to {path}")
    except Exception as e:
        print(f"Failed to write Delta: {str(e)}")
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
        spark, minio_landing, bronze_base = load_source_connections()

        if not all([spark, minio_landing, bronze_base]):
            raise RuntimeError("Failed to load connections")

        df = spark.read.parquet(f"{minio_landing}/productcategory")

        df_clean = df.select(
            col("category_id").cast("int"),
            col("category_name").cast("string"),
            col("category_identifier").cast("string"),
            col("created_date").cast("timestamp"),
            col("inserted_date").cast("timestamp"),
            col("eltversion").cast("int"),
        )

        write_to_delta(df_clean, bronze_base, "raw_dim_productcategory")
        print(f"Bronze job completed — {df_clean.count()} rows")

    except Exception as e:
        print(f"Job failed: {str(e)}")
        print_exc()
        exit(1)

    finally:
        spark_stop(spark)
