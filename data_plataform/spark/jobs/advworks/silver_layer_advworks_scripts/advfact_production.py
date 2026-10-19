import logging
from sys import exit
from traceback import print_exc

from configconnections_loader import load_config
from py4j.protocol import Py4JJavaError  # java exception error
from pyspark.sql.dataframe import DataFrame
from startSparkSession import SparkSessionManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# import env file


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


# surrogate key
def getCreateSurrogateKey(spark, table_name: str) -> DataFrame:
    try:
        dataframe = spark.table(f"iceberglake.bronze.{table_name}")

        dataframe.createTempView("temp_fact_production")

        dataframe = spark.sql("""

                                select

                                    row_number() over (order by workorderid) as fact_productionkey,
                                    cast(actualcost as decimal(12,2)) as actualcost_ds,
                                    cast(plannedcost as decimal(12,2)) as plannedcost_ds,
                                    *

                                from temp_fact_production
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


# fill null values
def fillNullValues(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.fillna(
            {
                "scrapreasonid": 0,
                "locationid": 0,
                "actualresourcehrs": 0.00,
                "plannedcost": 0.00,
                "plannedcost_ds": 0.00,
                "actualcost_ds": 0.00,
                "actualcost": 0.00,
                "scheduledstartdate": "2100-12-31 00:00:00",
                "scheduledenddate": "2100-12-31 00:00:00",
                "actualstartdate": "2100-12-31 00:00:00",
                "actualenddate": "2100-12-31 00:00:00",
            }
        )
        return dataframe

    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)

    except RuntimeError as e:
        logger.error(f"Invalid runtime error: {str(e)}")
        print_exc(e)
        exit(1)

    except TypeError as e:
        logger.error(f"Invalid datatype schema. {str(e)}")
        print_exc(e)
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)


# organize columns


def cleanerfactDataFrame(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.select(
            "fact_productionkey",
            "workorderid",
            "productid",
            "orderqty",
            "stockedqty",
            "scrappedqty",
            "scrapreasonid",
            "locationid",
            "actualresourcehrs",
            "plannedcost",
            "plannedcost_ds",
            "actualcost",
            "actualcost_ds",
            "scheduledstartdate",
            "scheduledenddate",
            "actualstartdate",
            "actualenddate",
            "startdate",
            "enddate",
            "duedate",
            "inserted_date",
            "modified_date",
            "eltversion",
        )

        logger.info(f"The data source were successfully transformed: {dataframe}")

        return dataframe

    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)

    except RuntimeError as e:
        logger.error(f"Invalid runtime error: {str(e)}")
        print_exc(e)
        exit(1)

    except TypeError as e:
        logger.error(f"Invalid datatype schema. {str(e)}")
        print_exc(e)
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)


def writeProductionFact(spark, dataframe: DataFrame, fact_name: str) -> DataFrame:
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberglake.silver")
        (
            dataframe.writeTo(f"iceberglake.silver.{fact_name}")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .createOrReplace()
        )

        logger.info(
            f"Dimension {fact_name} was written successfully to Iceberg at iceberglake.silver.{fact_name}"
        )

        return f"Dimension writing successfully completed to Iceberg: {fact_name}"

    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {fact_name} to Iceberg: {str(e)}")
        print_exc()
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error while writing table {fact_name} to Iceberg: {str(e)}")
        print_exc()
        exit(1)


# closing spark session
def sparkStop(spark) -> str:
    try:
        if spark:
            spark.stop()

        return f"Spark Session stoped Successfully: {spark}"

    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {str(e)}")
        print_exc(e)


# main functions
def main():
    table_name = "raw_fact_production"
    fact_name = "fact_production"
    application_name = "fact_production_table"

    try:
        spark = load_source_configurations(application_name)

        dataframe = getCreateSurrogateKey(spark, table_name)
        logger.info(f"Surrogate Key created: {dataframe.schema}")

        dataframe = fillNullValues(dataframe)
        logger.info(f"fillNullValues schema: {dataframe}")

        dataframe = cleanerfactDataFrame(dataframe)
        logger.info(f"cleanerfactDataFrame schema: {dataframe}")

        writeProductionFact(spark, dataframe, fact_name=fact_name)
        logger.info(f"writeProductionFact schema: {dataframe.printSchema()}")

        sparkStop(spark)

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
    logger.info("Sparkjob completed successfully")
