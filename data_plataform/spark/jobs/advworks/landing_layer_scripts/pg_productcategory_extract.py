"""
Landing: PostgreSQL nexomarket_db.product.productcategory → s3a://landing/productcategory (Parquet)
"""

from sys import exit
from traceback import print_exc

from configconnections_loader import load_config, validate_required_keys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


def load_source_connections():
    try:
        config = load_config()
        hadoop_paths = config.get("hadoop_paths", {})
        validate_required_keys(hadoop_paths, ["minio_landing"], "hadoop_paths")

        spark = SparkSession.builder.appName("pg_productcategory_landing").getOrCreate()

        return spark, hadoop_paths.get("minio_landing")

    except Exception as e:
        print(f"Failed to create Spark Session: {str(e)}")
        print_exc()
        return None, None


def extract_productcategory(spark, minio_landing: str):
    try:
        jdbc_url = "jdbc:postgresql://pgprojeto:5432/nexomarket_db"
        jdbc_user = "admin"
        jdbc_pwd = "admin"
        driver = "org.postgresql.Driver"

        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "product.productcategory")
            .option("user", jdbc_user)
            .option("password", jdbc_pwd)
            .option("driver", driver)
            .load()
        )

        df = df.withColumn("inserted_date", current_timestamp()).withColumn("eltversion", lit(1))

        output_path = f"{minio_landing}/productcategory"
        df.repartition(1).write.mode("overwrite").parquet(output_path)

        print(f"Landing: wrote {df.count()} rows to {output_path}")
        return True

    except Exception as e:
        print(f"Error extracting productcategory: {str(e)}")
        print_exc()
        return False


def spark_stop(spark):
    try:
        if spark:
            spark.stop()
    except Exception as e:
        print(f"Error stopping Spark: {str(e)}")


if __name__ == "__main__":
    spark = None
    try:
        spark, minio_landing = load_source_connections()

        if not all([spark, minio_landing]):
            raise RuntimeError("Failed to load connections")

        ok = extract_productcategory(spark, minio_landing)
        if not ok:
            raise RuntimeError("Extraction failed")

        print("Landing job completed successfully")

    except Exception as e:
        print(f"Job failed: {str(e)}")
        print_exc()
        exit(1)

    finally:
        spark_stop(spark)
