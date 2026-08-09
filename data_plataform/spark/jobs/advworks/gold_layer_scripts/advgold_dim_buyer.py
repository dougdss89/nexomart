from pyspark.sql import SparkSession
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
        return None, None


def read_silver_dimension(spark, table_name: str):
    try:
        print(f"Reading silver Iceberg: iceberglake.silver.{table_name}")
        return spark.table(f"iceberglake.silver.{table_name}")
    except Exception as e:
        print(f"Failed to read silver table {table_name}: {str(e)}")
        print_exc()
        return None


def write_gold_iceberg(spark, dataframe, catalog: str, schema: str, table_name: str):
    try:
        full_table = f"{catalog}.{schema}.{table_name}"
        print(f"Writing gold to Unity Catalog: {full_table}")

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{schema}")

        (
            dataframe
            .writeTo(full_table)
            .tableProperty("format-version", "2")
            .createOrReplace()
        )
        print(f"Successfully written {full_table}")
        spark.sql(f"SELECT count(*) as total_rows FROM {full_table}").show()

    except Exception as e:
        print(f"Failed to write gold table: {str(e)}")
        print_exc()
        exit(1)


def spark_stop(spark):
    try:
        if spark:
            spark.stop()
    except Exception as e:
        print(f"Error stopping Spark: {str(e)}")


if __name__ == '__main__':
    spark = None
    try:
        spark = load_source_configurations("gold_dim_buyer")

        if not spark:
            raise RuntimeError("Failed to load configurations")

        df = read_silver_dimension(spark, "dim_buyer")

        if df is None:
            raise RuntimeError("Failed to read silver dim_buyer")

        df.show(5)
        print(f"Silver schema: {df.schema.simpleString()}")

        write_gold_iceberg(spark, df, "nexo", "gold", "gold_dim_buyer")

        print("Gold layer job completed successfully")

    except Exception as e:
        print(f"Job failed: {str(e)}")
        print_exc()
        exit(1)

    finally:
        spark_stop(spark)
