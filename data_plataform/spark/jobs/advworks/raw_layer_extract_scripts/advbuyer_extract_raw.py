from pyspark.sql import SparkSession
from configconnections_loader import load_config, validate_required_keys
from pyspark.sql.functions import col
from sys import exit
from traceback import print_exc
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_source_connections():
    """ Load source connections from .env file and create a SparkSession. """
    try:
        config = load_config()

        hadoop_paths = config.get("hadoop_paths", {})

        validate_required_keys(
            config=hadoop_paths,
            required_keys=["minio_landing"],
            config_name="hadoop_paths"
        )

        spark = (
            SparkSession.builder
            .appName('raw_buyer_to_delta')
            .getOrCreate()
        )

        return spark, hadoop_paths.get("minio_landing", "")

    except (FileNotFoundError, KeyError) as e:
        print(f"Could not load connections from source: {str(e)}")
        print_exc()
        exit(1)

    except Exception as e:
        print(f"Unexpected error occurred: {str(e)}")
        print_exc()
        exit(1)


def extract_landing_buyer(spark, minio_landing, raw_table_name: str):
    """ Read Parquet data from the landing zone. """
    try:
        return spark.read.parquet(f"{minio_landing}/{raw_table_name}")
    except Exception as e:
        print(f"Failed to read landing data: {str(e)}")
        print_exc()
        exit(1)


def buyer_replace_schema(dataframe):
    return dataframe.select(
        col("businessentityid").cast("int").alias("businessentityid"),
        col("firstname").cast("string").alias("firstname"),
        col("lastname").cast("string").alias("lastname"),
        col("loginid").cast("string").alias("loginid"),
        col("birthdate").cast("date").alias("birthdate"),
        col("maritalstatus").cast("string").alias("maritalstatus"),
        col("gender").cast("string").alias("gender"),
        col("hiredate").cast("date").alias("hiredate"),
        col("jobtitle").cast("string").alias("jobtitle"),
        col("departmentid").cast("int").alias("departmentid"),
        col("name").cast("string").alias("name"),
        col("groupname").cast("string").alias("groupname"),
        col("payfrequency").cast("int").alias("payfrequency"),
        col("rate").cast("decimal(10,2)").alias("rate"),
        col("ratechangedate").cast("date").alias("salarychange_date"),
        col("startdate").cast("date").alias("startdate"),
        col("enddate").cast("date").alias("enddate"),
        col("salariedflag").cast("int").alias("salariedflag"),
        col("currentflag").cast("int").alias("currentflag"),
        col("inserted_date").cast("timestamp").alias("extracted_date"),
        col("modified_date").cast("timestamp").alias("modified_date"),
        col("eltversion").cast("int").alias("eltversion"),
    )


def write_to_iceberg(spark, dataframe, table_name: str):
    """ Write DataFrame to Iceberg at iceberglake.bronze.<table_name>. """
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberglake.bronze")
        (
            dataframe
            .writeTo(f"iceberglake.bronze.{table_name}")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .createOrReplace()
        )
        print(f"Table {table_name} written to Iceberg at iceberglake.bronze.{table_name}")
        logger.info(f"Table {table_name} written to Iceberg at iceberglake.bronze.{table_name}")
    except Exception as e:
        print(f"Failed to write table {table_name} to Iceberg: {str(e)}")
        print_exc()
        exit(1)

def spark_stop(spark):
    try:
        if spark:
            spark.stop()
            print("SparkSession stopped.")
    except Exception as e:
        print(f"Failed to stop SparkSession: {str(e)}")
        print_exc()


def main():
    spark, minio_landing = load_source_connections()
    raw_table = "raw_dimbuyer"

    dataframe = extract_landing_buyer(spark, minio_landing, "buyer")
    dataframe = buyer_replace_schema(dataframe)
    write_to_iceberg(spark=spark, dataframe=dataframe, table_name=raw_table)

    spark_stop(spark)
    logger.info("All functions executed successfully")


if __name__ == "__main__":
    main()
    logger.info("Spark job completed successfully")
