import logging
from sys import exit
from traceback import print_exc

import pyspark.sql.functions as F
from configconnections_loader import load_config
from py4j.protocol import Py4JJavaError  # java exception error
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DecimalType
from startSparkSession import SparkSessionManager

# logging
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


# create surrogate key
def getCreateSurrogateKey(spark, table_name: str) -> DataFrame:
    try:
        dataframe = spark.table(f"iceberglake.bronze.{table_name}")

        dataframe.createTempView("temp_salesperson")

        dataframe = spark.sql("""

                        select
                            row_number() over (order by businessentityid) as salespersonkey,
                            *
                        from temp_salesperson

                    """)
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


# transform salesperson gender
def transformSalesPersonGender(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.withColumn(
            "gender", F.when((F.col("gender")).like("M"), "Male").otherwise("Female")
        )

        logger.info("Gender column transformed successfully")
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


# transform territory
def transformSalesPersonTerritory(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.withColumn(
            "territoryid",
            F.when(
                (F.col("territoryid").isNull())
                & (F.col("CountryRegionCode").isNull())
                & (F.col("jobtitle").like("North American%")),
                F.lit(1),
            )
            .when(
                (F.col("territoryid").isNull())
                & (F.col("CountryRegionCode").isNull())
                & (F.col("jobtitle").like("Pacific Sales%")),
                F.lit(9),
            )
            .when(
                (F.col("territoryid").isNull())
                & (F.col("CountryRegionCode").isNull())
                & (F.col("jobtitle").like("European Sales%")),
                F.lit(8),
            )
            .otherwise(F.col("territoryid")),
        )

        logger.info("Territory column transformed successfully")
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


def transformSalesPersonCountryCode(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.withColumn(
            "countryregioncode",
            F.when(
                (F.col("territoryid").isNotNull()) & (F.col("jobtitle").like("North American%")),
                F.lit("US"),
            )
            .when(
                (F.col("territoryid").isNotNull()) & (F.col("jobtitle").like("European Sales%")),
                F.lit("EU"),
            )
            .when(
                (F.col("territoryid").isNotNull()) & (F.col("jobtitle").like("Pacific Sales%")),
                F.lit("UA"),
            )
            .otherwise(F.col("countryregioncode")),
        )

        logger.info("Country Code column transformed successfully")
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


def transfromSalespersonRegion(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.withColumn(
            "regionname",
            F.when(
                (F.col("JobTitle").like("North American%")) & (F.col("continentname").isNull()),
                F.lit("USA HQ"),
            )
            .when(
                (F.col("JobTitle").like("Pacific Sales%")) & (F.col("continentname").isNull()),
                F.lit("Sidney HQ"),
            )
            .when(
                (F.col("JobTitle").like("European Sales%")) & (F.col("continentname").isNull()),
                F.lit("Berlim HQ"),
            )
            .otherwise(F.col("continentname")),
        )
        logger.info("Sales Region transformed successfully")
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


# transform salesperson quota
def transformSalesPersonQuota(dataframe: DataFrame) -> DataFrame:
    try:
        max_sales_quota = dataframe.selectExpr("cast(max(salesquota) as float)").collect()[0][0]

        dataframe = dataframe.withColumn(
            "salesquota",
            F.when(
                (F.col("jobtitle").like("North American%")) & (F.col("salesquota").isNull()),
                F.lit(max_sales_quota * 0.70 + max_sales_quota),
            )
            .when(
                (F.col("jobtitle").like("European Sales%")) & (F.col("salesquota").isNull()),
                F.lit(max_sales_quota * 0.65 + max_sales_quota),
            )
            .when(
                (F.col("jobtitle").like("Pacific Sales%")) & (F.col("salesquota").isNull()),
                F.lit(max_sales_quota * 0.55 + max_sales_quota),
            )
            .otherwise(F.col("salesquota")),
        )
        logger.info("Column SalesQuota has been transformed successfully")
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


# calculate commission
def calculateSalesPersonCommission(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.withColumn(
            "commissionpct",
            F.when(
                (F.col("jobtitle").like("North American%")) & (F.col("commissionpct") == 0.00),
                F.lit(0.05),
            )
            .when(
                (F.col("jobtitle").like("European Sales%")) & (F.col("commissionpct") == 0.00),
                F.lit(0.04),
            )
            .when(
                (F.col("jobtitle").like("Pacific Sales%")) & (F.col("commissionpct") == 0.00),
                F.lit(0.04),
            )
            .otherwise(F.col("commissionpct")),
        )

        logger.info("Commission percentage calculated sucessfully")
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


def transfromSalespersonFlag(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.selectExpr(
            "*",
            """
                            case
                                when SalariedFlag = 1 then 'Yes'
                                else 'No'
                            end as issalaried

                        """,
            """
                            case
                                when CurrentFlag = 1 then 'Yes'
                                else 'No'
                            end as isactive

                        """,
        )
        logger.info("Flags transformed sucessfully")
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


# calculate time
def calculateSalesPersonYear(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.withColumn(
            "age", F.year(F.current_date()) - F.year(F.col("birthdate"))
        ).withColumn("yearsincompany", F.year(F.current_date()) - F.year(F.col("hiredate")))

        logger.info("Time calculated Successfully")
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


def transformSalesPersonMaritalStatus(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.withColumn(
            "maritalstatus",
            F.when((F.col("maritalstatus")).like("M"), "Married").otherwise("Single"),
        )

        logger.info("Marital status function executed Successfully")
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


def transformSalesPersonContinentSales(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.withColumn(
            "continentname",
            F.when(
                (F.col("regionname").like("USA HQ")) & (F.col("continentname").isNull()),
                "North America",
            )
            .when(
                (F.col("regionname").like("Sidney HQ")) & (F.col("continentname").isNull()),
                "Pacific",
            )
            .when(
                (F.col("regionname").like("Berlim HQ")) & (F.col("continentname").isNull()),
                "Europe",
            )
            .otherwise(F.col("continentname")),
        )

        logger.info("Continent Sales function executed Successfully")
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


def transformSalesPersonEndDate(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.withColumn(
            "enddate",
            F.when(
                (F.col("enddate").isNull()) & (F.col("isactive") == "Yes"), "2100-12-31"
            ).otherwise(F.col("enddate")),
        )

        logger.info("End Date function executed Successfully")
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


def transformSalesPersonLogin(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.withColumn("loginid", F.split(F.col("loginid"), r"\\")[1])

        logger.info("Login function executed Successfully")
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


def dropSalesPersonColumns(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.drop(F.col("salariedflag")).drop(F.col("currentflag"))

        logger.info("Drop coluns function executed Successfully")
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


def reorganizeColumns(dataframe: DataFrame) -> DataFrame:
    try:
        dataframe = dataframe.select(
            F.col("salespersonkey"),
            F.col("businessentityid").alias("salespersonid"),
            F.col("loginid"),
            F.col("firstname"),
            F.col("lastname"),
            F.col("gender"),
            F.col("birthdate"),
            F.col("hiredate"),
            F.col("age"),
            F.col("jobtitle"),
            F.col("maritalstatus"),
            F.col("organizationlevel"),
            F.col("departmentid"),
            F.col("startdate"),
            F.col("enddate"),
            F.col("yearsincompany"),
            F.col("groupname"),
            F.col("departmentname").alias("department"),
            F.col("payfrequency"),
            F.col("rate").cast(DecimalType(5, 2)),
            F.col("ratechangedate"),
            F.col("commissionpct").alias("commision").cast(DecimalType(5, 2)),
            F.col("salesquota").alias("salespersonquota").cast(DecimalType(12, 2)),
            F.col("bonus").cast(DecimalType(12, 2)),
            F.col("territoryid"),
            F.col("regionname").alias("salesregion"),
            F.col("continentname").alias("continent"),
            F.col("countryregioncode").alias("countrycode"),
            F.col("issalaried"),
            F.col("isactive"),
            F.col("inserted_date"),
            F.col("modified_date"),
            F.col("eltversion"),
        )

        logger.info("Columns reorganize function executed Successfully")
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


def writeSalesPersondDimension(spark, dataframe: DataFrame, table_name: str) -> DataFrame:
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberglake.silver")
        (
            dataframe.writeTo(f"iceberglake.silver.{table_name}")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .createOrReplace()
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

        return f"Spark Session stoped Successfully: {spark}"

    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {str(e)}")
        print_exc(e)


def main():
    try:
        application_name = "dimension_salesperson_table"
        table_name = "raw_dimsalesperson"
        dimension_table = "dim_salesperson"

        spark = load_source_configurations(application_name)

        dataframe = getCreateSurrogateKey(spark, table_name)
        logger.info(f"Dimension surrogate key created: {dataframe.schema}")

        dataframe = transformSalesPersonGender(dataframe)
        logger.info(f"Column Gender processed successfully: {dataframe.schema}")

        dataframe = transformSalesPersonTerritory(dataframe)
        logger.info(f"Territory Column processed successfully: {dataframe.schema}")

        dataframe = transformSalesPersonCountryCode(dataframe)
        logger.info(f"Country code column processed successfully: {dataframe.schema}")

        dataframe = transfromSalespersonRegion(dataframe)
        logger.info(f"Region column processed successfully: {dataframe.schema}")

        dataframe = transformSalesPersonQuota(dataframe)
        logger.info(f"SalesQuota column calculated successfully: {dataframe.schema}")

        dataframe = calculateSalesPersonCommission(dataframe)
        logger.info("SalesPerson Commission calculated successfully")

        dataframe = transfromSalespersonFlag(dataframe)
        logger.info(f"Flag column processed successfully: {dataframe.schema}")

        dataframe = calculateSalesPersonYear(dataframe)
        logger.info(f"Year column processed successfully: {dataframe.schema}")

        dataframe = transformSalesPersonMaritalStatus(dataframe)
        logger.info(f"Maritalstatus column processed successfully: {dataframe.schema}")

        dataframe = transformSalesPersonContinentSales(dataframe)
        logger.info(f"Continent Sales column processed successfully: {dataframe.schema}")

        dataframe = transformSalesPersonEndDate(dataframe)
        logger.info(f"End Date column processed successfully: {dataframe.schema}")

        dataframe = transformSalesPersonLogin(dataframe)
        logger.info(f"Login column processed successfully: {dataframe.schema}")

        dataframe = dropSalesPersonColumns(dataframe)
        logger.info(f"Drop columns successfully: {dataframe.schema}")

        dataframe = reorganizeColumns(dataframe)
        logger.info(f"Reorganize columns processed successfully: {dataframe.schema}")

        writeSalesPersondDimension(spark, dataframe, table_name=dimension_table)
        logger.info(f"""dataframe has been writted successfully: {dataframe.schema}
                        on path: iceberglake.silver.{dimension_table}""")

        sparkStop(spark)
        logger.info("Spark Session stopped")

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
