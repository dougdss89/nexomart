
from startSparkSession import SparkSessionManager
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType
from pyspark.sql.dataframe import DataFrame
from configconnections_loader import validate_required_keys, load_config
from sys import exit
from traceback import print_exc
import logging


from py4j.protocol import Py4JJavaError #java exception error


#logging
logging.basicConfig(level = logging.INFO, format = "%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

#import .env file
def load_source_configurations(spark_application: str):
    
    try:
        config = load_config()
        

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

#create surrogate key
def getCreateSurrogateKey(spark, table_name: str) -> DataFrame:

    try:
        
        dataframe = spark.table(f"iceberglake.bronze.{table_name}")
        
        dataframe.createTempView("temp_salesperson")

        dataframe = (spark.sql(""" 
                               
                        select
                            row_number() over (order by businessentityid) as salespersonkey,    
                            * 
                        from temp_salesperson
                        
                    """)
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
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)

#transform salesperson gender
def transformSalesPersonGender(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                        .withColumn("gender",
                                        when((col("gender")).like("M"), "Male")
                                        .otherwise("Female"))
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

#transform territory
def transformSalesPersonTerritory(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe.withColumn(
                                "territoryid",
                                    when(
                                        (col("territoryid").isNull()) & (col("CountryRegionCode").isNull()) & (col("jobtitle").like("North American%")),
                                        lit(1))
                                    .when(
                                        (col("territoryid").isNull()) & (col("CountryRegionCode").isNull()) & (col("jobtitle").like("Pacific Sales%")),
                                        lit(9))
                                    .when(
                                        (col("territoryid").isNull()) & (col("CountryRegionCode").isNull()) & (col("jobtitle").like("European Sales%")),
                                        lit(8))
                                .otherwise(col("territoryid")))
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
        dataframe = (dataframe.withColumn(
                            "countryregioncode",
                                when(
                                    (col("territoryid").isNotNull()) & (col("jobtitle").like("North American%")),
                                    lit("US"))
                                .when(
                                    (col("territoryid").isNotNull()) & (col("jobtitle").like("European Sales%")),
                                    lit("EU"))
                                .when(
                                    (col("territoryid").isNotNull()) & (col("jobtitle").like("Pacific Sales%")),
                                    lit("UA"))
                            .otherwise(col("countryregioncode")))
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
        dataframe = (dataframe.withColumn(
                                "regionname",
                                    when(
                                        (col("JobTitle").like("North American%")) & (col("continentname").isNull()),
                                        lit("USA HQ"))
                                    .when(
                                        (col("JobTitle").like("Pacific Sales%")) & (col("continentname").isNull()),
                                        lit("Sidney HQ"))
                                    .when(
                                        (col("JobTitle").like("European Sales%")) & (col("continentname").isNull()),
                                        lit("Berlim HQ"))
                                .otherwise(col("continentname")))
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
        
#transform salesperson quota
def transformSalesPersonQuota(dataframe: DataFrame) -> DataFrame:
    
    try:
        max_sales_quota = dataframe.selectExpr("cast(max(salesquota) as float)").collect()[0][0]

        dataframe = (dataframe
                        .withColumn(
                                "salesquota",
                                    when((col("jobtitle").like("North American%")) & (col("salesquota").isNull()),
                                            lit(max_sales_quota * 0.70 + max_sales_quota))

                                    .when((col("jobtitle").like("European Sales%")) & (col("salesquota").isNull()),
                                            lit(max_sales_quota * 0.65 + max_sales_quota))

                                    .when((col("jobtitle").like("Pacific Sales%")) & (col("salesquota").isNull()),
                                            lit(max_sales_quota * 0.55 + max_sales_quota))

                                .otherwise(col("salesquota")))
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

#calculate commission
def calculateSalesPersonCommission(dataframe:DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe 
                        .withColumn(
                            "commissionpct",
                                
                                when((col("jobtitle").like('North American%')) & (col("commissionpct")== 0.00), lit(0.05))
                                .when((col("jobtitle").like('European Sales%')) & (col("commissionpct")== 0.00), lit(0.04))                
                                .when((col("jobtitle").like('Pacific Sales%')) & (col("commissionpct")== 0.00), lit(0.04))
                            .otherwise(col("commissionpct")))
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
        
def transfromSalespersonFlag(dataframe:DataFrame) -> DataFrame:
    
    try:
        dataframe = dataframe.selectExpr("*",           
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
                            
                        """)
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
        
#calculate time
def calculateSalesPersonYear(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                        .withColumn("age", year(current_date()) - year(col("birthdate"))) 
                
                        .withColumn("yearsincompany", year(current_date()) - year(col("hiredate")))
            )

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
        dataframe = (dataframe
                        .withColumn("maritalstatus",
                            when((col("maritalstatus")).like("M"), "Married")
                            
                        .otherwise("Single"))
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
        dataframe = (dataframe
                        .withColumn("continentname",
                                    when((col("regionname").like("USA HQ")) & (col("continentname").isNull()), "North America")
                                    .when((col("regionname").like("Sidney HQ")) & (col("continentname").isNull()), "Pacific")
                                    .when((col("regionname").like("Berlim HQ")) & (col("continentname").isNull()), "Europe")
                                .otherwise(col("continentname")))
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
        dataframe = (dataframe
                        .withColumn("enddate", 
                                        when((col("enddate").isNull()) & (col("isactive") == "Yes"), "2100-12-31")
                                    .otherwise(col("enddate")))
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
        dataframe = (dataframe
                        .withColumn("loginid", 
                                    split(col("loginid"),  r"\\")[1])
                    )

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
        dataframe = (dataframe
                        .drop((col("salariedflag")))
                        .drop((col("currentflag")))
                    )
        
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
        dataframe = (dataframe.select(
                        col("salespersonkey"),
                        col("businessentityid").alias("salespersonid"),
                        col("loginid"),
                        col("firstname"),
                        col("lastname"),
                        col("gender"),
                        col("birthdate"),
                        col("hiredate"),
                        col("age"),
                        col("jobtitle"),
                        col("maritalstatus"),
                        col("organizationlevel"),
                        col("departmentid"),
                        col("startdate"),
                        col("enddate"),
                        col("yearsincompany"),
                        col("groupname"),
                        col("departmentname").alias("department"),
                        col("payfrequency"),
                        col("rate").cast(DecimalType(5, 2)),
                        col("ratechangedate"),
                        col("commissionpct").alias("commision").cast(DecimalType(5,2)),
                        col("salesquota").alias("salespersonquota").cast(DecimalType(12,2)),
                        col("bonus").cast(DecimalType(12,2)),
                        col("territoryid"),
                        col("regionname").alias("salesregion"),
                        col("continentname").alias("continent"),
                        col("countryregioncode").alias("countrycode"),
                        col("issalaried"),
                        col("isactive"),
                        col("inserted_date"),
                        col("modified_date"),
                        col("eltversion"))
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
            dataframe
            .writeTo(f"iceberglake.silver.{table_name}")
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
        # Garante que a sessão do Spark seja encerrada, mesmo em caso de erro
        if 'spark' in locals():
            sparkStop(spark)
            
if __name__ == "__main__":
    result = main()
    if result != "Success":
        exit(1)
    logger.info("Sparkjob completed successfully")