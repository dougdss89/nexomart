import logging
from sys import exit
from traceback import print_exc

from configconnections_loader import load_config
from py4j.protocol import Py4JJavaError  # java exception error
from pyspark.sql.dataframe import DataFrame
from startSparkSession import SparkSessionManager

# setting log level
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# import .env file
def load_source_configurations(spark_application: str):
    try:
        load_config()

        spark_manager = SparkSessionManager(spark_application)

        spark = spark_manager.create_session()

        return spark

    except FileNotFoundError as e:
        logger.error(f"Could not load connections from source: {str(e)}")
        print_exc()
        exit(1)

    except KeyError as e:
        logger.error(f"key value pair error. Value not found. Verify: {str(e)}")
        print_exc()
        exit(1)

    except RuntimeError as e:
        logger.error(f"Failed to create Spark Session. Runtime Error: {str(e)}")
        print_exc()
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error occurred. {str(e)}")
        print_exc()
        exit(1)

    logger.info(f"Spark Session created: {spark}")


def getCreateSurrogateKey(spark, table_name: str) -> DataFrame:
    try:
        dataframe = spark.table(f"iceberglake.bronze.{table_name}")

        dataframe.createTempView("temp_currency")

        dataframe = spark.sql("""

                            select
                                row_number() over(order by currencyrateid) as currencyrate_key,
                                *
                            from temp_currency
                        """)

        logger.info(f"The data source were successfully extracted: {dataframe}")

        return dataframe

    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)

    except RuntimeError as e:
        logger.error(f"Invalid runtime error: {str(e)}")
        print_exc(e)
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)


def writeCurrencyRateDimension(spark, dataframe: DataFrame, table_name: str) -> DataFrame:
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberglake.silver")
        (
            dataframe.writeTo(f"iceberglake.silver.{table_name}")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .createOrReplace()
        )

        logger.info(
            f"Dimension {table_name} was written successfully to iceberg iceberglake.silver.{table_name}"
        )

        return logger.info(f"Table writing successfully completed to iceberg: {table_name}")

    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {table_name} to iceberg: {str(e)}")
        print_exc()
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error while writing table {table_name} to iceberg: {str(e)}")
        print_exc()
        exit(1)


def sparkStop(spark) -> str:
    try:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped successfully")

        return f"{spark} Spark Session stopped"

    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {str(e)}")
        print_exc()


def main():
    try:
        application_name = "dimension_currencyrate_table"
        table_name = "raw_dimcurrencyrate"
        dimension_table = "dim_currencyrate"

        spark = load_source_configurations(application_name)

        dataframe = getCreateSurrogateKey(spark, table_name)

        writeCurrencyRateDimension(spark, dataframe, table_name=dimension_table)

        logger.info("All functions have been executed successfully")

        return "Success"

    except FileNotFoundError as e:
        logger.error(f"Configuration file or resource not found: {str(e)}")
        return "FileNotFoundError"

    except Py4JJavaError as e:
        logger.error(f"Java/Scala error during Spark operation: {str(e)}")
        return "Py4JJavaError"

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return "UnexpectedError"

    finally:
        # Ensures the Spark session is closed, even in case of error
        if "spark" in locals():
            sparkStop(spark)


if __name__ == "__main__":
    result = main()
    if result != "Success":
        exit(1)
    logger.info("Spark Job completed successfully")
