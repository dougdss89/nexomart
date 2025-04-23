
from startSparkSession import SparkSessionManager
from pyspark.sql.functions import *
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
        
        iceberg = config.get("hadoop_paths", {})
        iceberg_db = config.get("hadoop_paths", {})
        iceberg_catalog = config.get("hadoop_paths", {})
        
        validate_required_keys(
            config = iceberg,
            required_keys= ["raw_icebergdb", "silver_icebergdb", "iceberg_catalog"],
            config_name = "hadoop_paths"
        )
        
        spark_manager = SparkSessionManager(spark_application)
        
        spark = spark_manager.create_session()
        
        return spark, iceberg.get("raw_icebergdb", ""), iceberg_db.get("silver_icebergdb", ""), iceberg_catalog.get("iceberg_catalog", "")

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
def getCreateSurrogateKey(spark, catalog:str, iceberg:str, table_name:str) -> DataFrame:

    
    try:
        
        raw_lakehouse = f"{catalog}.{iceberg}.{table_name}"
        
        dataframe = spark.read.format("iceberg").load(raw_lakehouse)
        
        dataframe.createTempView("temp_salesperson")

        dataframe = (spark.sql(""" 
                               
                        select
                            row_number() over (order by salespersonid) as salespersonkey,    
                            * 
                        from temp_salesperson
                        
                    """))
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
                        .selectExpr('*',
                            
                    """ 
                         case
                            when gender = 'M' then 'Male'
                            else 'Female'
                        end as gender_new
                    """
                    ))
        
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
                        "territoryid_new",
                        when(
                            (col("TerritoryID").isNull()) & (col("CountryRegionCode").isNull()) & (col("JobTitle").like("North American%")),
                            lit(1))
                        .when(
                            (col("TerritoryID").isNull()) & (col("CountryRegionCode").isNull()) & (col("JobTitle").like("Pacific Sales%")),
                            lit(9))
                        .when(
                            (col("TerritoryID").isNull()) & (col("CountryRegionCode").isNull()) & (col("JobTitle").like("European Sales%")),
                            lit(8))
                        .otherwise(col("TerritoryID"))
                    ))
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
                        "countrycode_new",
                        when(
                            (col("TerritoryID").isNull()) & (col("JobTitle").like("North American%")),
                            lit("US"))
                        .when(
                            (col("TerritoryID").isNull()) & (col("JobTitle").like("European Sales%")),
                            lit("EU"))
                        .when(
                            (col("TerritoryID").isNull()) & (col("JobTitle").like("Pacific Sales%")),
                            lit("UA"))
                        .otherwise(col("CountryRegionCode"))
                    ))
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
                        "salesregion_new",
                        when(
                            (col("JobTitle").like("North American%")) & (col("ContinentName").isNull()),
                            lit("USA HQ"))
                        .when(
                            (col("JobTitle").like("Pacific Sales%")) & (col("ContinentName").isNull()),
                            lit("Sidney HQ"))
                        .when(
                            (col("JobTitle").like("European Sales%")) & (col("ContinentName").isNull()),
                            lit("Berlim HQ"))
                        .otherwise(col("ContinentName"))
                    ))
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

        dataframe = (dataframe.withColumn(
                        "salesquota_new",
                        when((col("jobtitle").like("North American%")) & (col("salesquota").isNull()),
                                lit(max_sales_quota * 0.70 + max_sales_quota))

                        .when((col("jobtitle").like("European Sales%")) & (col("salesquota").isNull()),
                                lit(max_sales_quota * 0.65 + max_sales_quota))

                        .when((col("jobtitle").like("Pacific Sales%")) & (col("salesquota").isNull()),
                                lit(max_sales_quota * 0.55 + max_sales_quota))

                        .otherwise(col("salesquota"))
                    ))
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
        dataframe = (dataframe .withColumn(
                        "commission_new",
                         when((col("jobtitle").like('North American%')) & (col("commissionpct")== 0.00), lit(0.05))                
                        .when((col("jobtitle").like('European Sales%')) & (col("commissionpct")== 0.00), lit(0.04))                
                        .when((col("jobtitle").like('Pacific Sales%')) & (col("commissionpct")== 0.00), lit(0.04))
                    ))
        
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
                                        """
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
        
#calculate time
def calculateSalesPersonAge(dataframe: DataFrame) -> DataFrame:
    
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

def writeSalesPersondDimension(dataframe:DataFrame, catalog:str, silver_icebergdb:str, table_name:str) -> DataFrame:
    
    try:
        lakehouse = f"{catalog}.{silver_icebergdb}.{table_name}"

        (
            dataframe
                .write
                .mode("append")
                .format("iceberg")
                .saveAsTable(f"{lakehouse}")
        )
        
        return logger.info(f"Table writing successfully completed to iceberg: {table_name}")

    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {table_name} to iceberg: {str(e)}")
        return "Py4JJavaError"

    except Exception as e:
        logger.error(f"Unexpected error while writing table {table_name} to iceberg: {str(e)}")
        return "UnexpectedError"
    
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
        
        
        spark, iceberg, iceberg_db, iceberg_catalog = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, iceberg_catalog, iceberg, table_name)
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
        logger.info(f"SalesPerson Commission calculated successfully")
        
        dataframe = transfromSalespersonFlag(dataframe)
        logger.info(f"Flag column processed successfully: {dataframe.schema}")
        
        dataframe = calculateSalesPersonAge(dataframe)
        logger.info(f"Age column processed successfully: {dataframe.schema}")
        
        writeSalesPersondDimension(dataframe, catalog=iceberg_catalog, silver_icebergdb= iceberg_db, table_name= dimension_table)
        logger.info(f"""dataframe has been writted successfully: {dataframe.schema} 
                        on path: {iceberg_db}.{dimension_table}""")
        
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
    main()
    logger.info("Sparkjob completed successfully") 