"""
Gold Iceberg — dim_productcategory
Lê da camada Silver (Iceberg), escreve Iceberg via IcebergHiveCatalog (iceberglake).
Trino lê via catálogo iceberglake (iceberg connector + hive metastore).

Arquitetura:
  Silver Iceberg →  iceberglake.gold.gold_dim_productcategory
                        ↑ Iceberg (HiveCatalog → Hive Metastore)
                        ↑ Trino lê via trino/catalog/iceberglake.properties
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

SILVER_TABLE = "dim_productcategory"
GOLD_TABLE   = "gold_dim_productcategory"
SCHEMA       = "gold"


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

        df = (
            spark.table(f"iceberglake.silver.{SILVER_TABLE}")
            .withColumn("gold_load_date", current_timestamp())
        )

        # Cria namespace Iceberg no Hive Metastore (idempotente)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberglake.{SCHEMA}")

        # Escreve Iceberg via IcebergHiveCatalog — registrado no Hive Metastore
        # Trino lê via trino/catalog/iceberglake.properties (iceberg + hive)
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
