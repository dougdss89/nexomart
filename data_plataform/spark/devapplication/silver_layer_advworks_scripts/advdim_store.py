from startSparkSession import SparkSessionManager
import pyspark.sql.functions as F
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
        
        dataframe.createTempView("temp_store")

        dataframe = (spark.sql(""" 
                            select 
                                 row_number() over (order by storeid) as storekey,
                                *
                            from temp_store
                            where personid is not null
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
        
#transform businesstype
def transformStoreBusinessType(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                .withColumn("businesstype", regexp_replace("businesstype", r"(?i)\bbm\b", "Bike Mall"))
                .withColumn("businesstype", regexp_replace("businesstype", r"(?i)\bos\b", "Online store"))
                .withColumn("businesstype", regexp_replace("businesstype", r"(?i)\bbs\b", "Bike store"))
                .withColumn("yearsinactivity", year(current_date()) - col("yearopened"))
            ) 
        
        logger.info("Business Type column transformed successfully")
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

#annual revenue
def transformStoreRevenue(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = dataframe.withColumn(
                    "storesize",
                        when(col("annualrevenue") <= 30000.00, "Local store")
                    .when((col("annualrevenue") > 30000.00) & (col("annualrevenue") < 80000.00), "Small store")
                    .when((col("annualrevenue") > 80000.00) & (col("annualrevenue") < 100000.00), "Medium store")
                    .when((col("annualrevenue") > 100000.00) & (col("annualrevenue") <= 150000.00), "Large store")
                .otherwise("Global store")
            )
                
        logger.info("Business Type column transformed successfully")
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
        
#brands
def transformStoreBrands(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = dataframe.withColumn(
                        "brandExpertise",
                        when((col("brands") == "3"), "Senior")
                        .when((col("brands") >= 1) & (col("brands") <= 2), "Medium Knowledge")
                        .otherwise("Expert")
                    )
        logger.info("Brand column transformed successfully")
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
        
#organize columns
def cleanerStoreDataframe(dataframe: DataFrame) -> DataFrame:
    
    try:
        store = (dataframe.select(      
                                "storekey",			"storeid",
                                "PersonID",			"CustomerID",
                                "AccountNumber", 	"storename",
                                "FirstName",		"LastName",
                                "AnnualSales",		"AnnualRevenue",
                                "BankName",			"businesstype",
                                "YearOpened",		"Specialty",
                                "SquareFeet",		"Brands",
                                "Internet",			"NumberEmployees",
                                "storeID",	"territoryid",
                                "continentname",	"regionname",
                                "CountryRegionCode","yearsinactivity",
                                "storesize",		"brandExpertise",	
                                "inserted_date",	"modified_date",
                                "eltversion"
                            ).withColumnRenamed("territoryid", "countryid")
                             .withColumnRenamed("personid", "clientid")
                             .withColumnRenamed("storeid", "vendorid")
                        )
        logger.info("Columns transformed successfully")
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
        
def writeStoreDimension(dataframe:DataFrame, catalog:str, silver_icebergdb:str, table_name:str) -> DataFrame:
    
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
        application_name = "dimension_store_table"
        table_name = "raw_dimstore"
        dimension_table = "dim_store"
        
        
        spark, iceberg, iceberg_db, iceberg_catalog = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, iceberg_catalog, iceberg, table_name)
        logger.info(f"Dimension surrogate key created: {dataframe.schema}")
        
        dataframe = transformStoreBusinessType(dataframe)
        logger.info(f"Column Business Type processed successfully: {dataframe.schema}")
        
        dataframe = transformStoreRevenue(dataframe)
        logger.info(f"Revenue Column processed successfully: {dataframe.schema}")
        
        dataframe = transformStoreBrands(dataframe)
        logger.info(f"Brand column processed successfully: {dataframe.schema}")
        
        dataframe = cleanerStoreDataframe(dataframe)
        logger.info(f"Columns processed successfully: {dataframe.schema}")

        writeStoreDimension(dataframe, catalog=iceberg_catalog, silver_icebergdb= iceberg_db, table_name= dimension_table)
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
        # Ensures the Spark session is closed, even in case of error
        if 'spark' in locals():
            sparkStop(spark)
            
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully") 