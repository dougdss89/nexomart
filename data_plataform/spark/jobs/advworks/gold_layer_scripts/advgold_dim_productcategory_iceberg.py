"""
Gold Iceberg — dim_productcategory
Reads from the Silver layer (Iceberg), writes Iceberg via IcebergHiveCatalog (iceberglake).
Trino reads via the iceberglake catalog (iceberg connector + hive metastore).

Architecture:
  Silver Iceberg →  iceberglake.gold.gold_dim_productcategory
                        ↑ Iceberg (HiveCatalog → Hive Metastore)
                        ↑ Trino reads via trino/catalog/iceberglake.properties
"""

from sys import exit
from traceback import print_exc

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

SILVER_TABLE = "dim_productcategory"
GOLD_TABLE = "gold_dim_productcategory"
SCHEMA = "gold"


def create_spark():
    try:
        spark = SparkSession.builder.appName("gold_iceberg_dim_productcategory").getOrCreate()
        return spark
    except Exception as e:
        print(f"Failed to create Spark session: {e}")
        print_exc()
        return None


def spark_stop(spark):
    try:
        if spark:
            spark.stop()
    except Exception as e:
        print(f"Error stopping Spark: {e}")


if __name__ == "__main__":
    spark = None
    try:
        spark = create_spark()
        if not spark:
            raise RuntimeError("Failed to initialize")

        df = spark.table(f"iceberglake.silver.{SILVER_TABLE}").withColumn(
            "gold_load_date", current_timestamp()
        )

        # Creates the Iceberg namespace in the Hive Metastore (idempotent)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberglake.{SCHEMA}")

        # Writes Iceberg via IcebergHiveCatalog — registered in the Hive Metastore
        # Trino reads via trino/catalog/iceberglake.properties (iceberg + hive)
        (
            df.writeTo(f"iceberglake.{SCHEMA}.{GOLD_TABLE}")
            .tableProperty("write.format.default", "parquet")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )

        print(f"Iceberg: {df.count()} rows → iceberglake.{SCHEMA}.{GOLD_TABLE}")
        print("Gold Iceberg job completed successfully")

    except Exception as e:
        print(f"Job failed: {e}")
        print_exc()
        exit(1)

    finally:
        spark_stop(spark)
