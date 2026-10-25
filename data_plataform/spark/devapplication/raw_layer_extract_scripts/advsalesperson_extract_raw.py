import logging
from sys import exit
from traceback import print_exc

from configconnections_loader import load_config, validate_required_keys
from pyspark.sql.functions import col
from startSparkSession import SparkSessionManager

# log config
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_source_connections():
    """Load source connections from .env file and create a SparkSession."""
    try:
        config = load_config()

        # Validating imported keys
        minio_landing = config.get("hadoop_paths", {})
        iceberg = config.get("hadoop_paths", {})
        iceberg_catalog = config.get("hadoop_paths", {})

        validate_required_keys(
            config=minio_landing, required_keys=["minio_landing"], config_name="hadoop_paths"
        )

        validate_required_keys(
            config=iceberg, required_keys=["raw_icebergdb"], config_name="hadoop_paths"
        )

        validate_required_keys(
            config=iceberg_catalog, required_keys=["iceberg_catalog"], config_name="hadoop_paths"
        )

        # create spark session

        session_manager = SparkSessionManager(app_name="extract_salesperson_landing")

        spark = session_manager.create_session()

        return (
            spark,
            minio_landing.get("minio_landing", ""),
            iceberg.get("raw_icebergdb", ""),
            iceberg_catalog.get("iceberg_catalog", ""),
        )  # , minio_storage

    except (FileNotFoundError, KeyError) as e:
        logger.error(f"Could not load connections from source: {str(e)}")
        print_exc()
        exit(1)

    except RuntimeError as e:
        logger.error(f"Failed to create Spark Session. Runtime Error: {str(e)}")
        print_exc()
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error occur. {str(e)}")
        print_exc()
        exit(1)


def extract_landing_data(spark, minio_landing, raw_table):
    try:
        dataframe = spark.read.parquet(f"{minio_landing}/{raw_table}")
        logger.info(f"Data extracted sucessfully for table: {raw_table}")

        return dataframe

    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {str(e)}")
        print_exc()
        exit(1)


def convert_salesperson_schema(dataframe):
    try:
        dataframe.select(
            col("businessentityid").cast("int").alias("salespersonid"),
            col("loginid").cast("int").alias("loginid"),
            col("firstname").cast("string").alias("firstname"),
            col("lastname").cast("string").alias("lastname"),
            col("gender").cast("string").alias("gender"),
            col("birthdate").cast("date").alias("birthdate"),
            col("hiredate").cast("date").alias("hiredate"),
            col("jobtitle").cast("string").alias("jobtitle"),
            col("maritalstatus").cast("string").alias("maritalstatus"),
            col("organizationlevel").cast("string").alias("organizationlevel"),
            col("departmentid").cast("int").alias("departmentid"),
            col("startdate").cast("date").alias("startdate"),
            col("enddate").cast("date").alias("enddate"),
            col("groupname").cast("string").alias("groupname"),
            col("departmentname").cast("string").alias("departmentname"),
            col("payfrequency").cast("int").alias("payfrequency"),
            col("rate").cast("decimal").alias("rate"),
            col("ratechangedate").cast("date").alias("ratechangedate"),
            col("commissionpct").cast("decimal").alias("commissionpct"),
            col("salesquota").cast("decimal").alias("salesquota"),
            col("bonus").cast("decimal").alias("bonus"),
            col("regionname").cast("string").alias("regionname"),
            col("continentname").cast("string").alias("continentname"),
            col("countryregioncode").cast("string").alias("countryregioncode"),
            col("territoryid").cast("int").alias("territoryid"),
            col("salariedflag").cast("string").alias("salariedflag"),
            col("currentflag").cast("string").alias("currentflag"),
        )
        return dataframe

    except Exception as e:
        logger.error(f"Failed to extract data from {dataframe}: {str(e)}")
        print_exc()
        exit(1)


def write_to_iceberg(dataframe, catalog: str, raw_icebergdb: str, table_name: str):
    """Write DataFrame to iceberg table."""
    try:
        raw_lakehouse = f"{catalog}.{raw_icebergdb}.{table_name}"

        (
            dataframe.write.format("iceberg")
            .mode("append")  # Use "append" to add data instead
            .saveAsTable(f"{raw_lakehouse}")
        )
        print(f"Table {table_name} written successfully to iceberg.")

        return logger.info(f"Table {table_name} written successfully to iceberg.")

    except Exception as e:
        print(f"Failed to write table {table_name} to iceberg: {str(e)}")
        print_exc()
        exit(1)


def spark_stop(spark):
    try:
        if spark:
            spark.stop()
            logger.info("SparkSession stoped successfully")

    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {str(e)}")
        print_exc()


def main():
    spark, minio_landing, iceberg, iceberg_catalog = load_source_connections()
    raw_table = "raw_dimsalesperson"

    dataframe = extract_landing_data(spark, minio_landing, "salesperson")

    dataframe = convert_salesperson_schema(dataframe)

    # write_salesperson_minio_storage(salesperson, miniopath, "salesperson")

    write_to_iceberg(
        dataframe=dataframe, catalog=iceberg_catalog, raw_icebergdb=iceberg, table_name=raw_table
    )

    spark_stop(spark)

    return logger.info("All functions have been executed successfully")


if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")
