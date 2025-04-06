from startSparkSession import SparkSessionManager
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
from configconnections_loader import validate_required_keys, load_config
from sys import exit
from traceback import print_exc
import logging

from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError #java exception error
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

#logging
logging.basicConfig(level = logging.INFO, format = "%(asctime)s - %(format)s - %(message)s")
logger = logging.getLogger(__name__)

#import .env file
def load_source_configurations(spark_application: str):
    
    try:
        config = load_config()
        
        iceberg = config.get("hadoop_paths", {})
        iceberg_db = config.get("hadoop_paths", {})
        
        validate_required_keys(
            config = iceberg,
            required_keys= ["iceberg", "iceberg_db"],
            config_name = "hadoop_paths"
        )
        
        spark_manager = SparkSessionManager(spark_application)
        
        spark = spark_manager.create_session()
        
        return spark, iceberg.get("raw_icebergdb", ""), iceberg_db.get("silver_icebergdb", "")

    except FileNotFoundError as e:
        logger.error(f"Could not load connections from source: {e}")
        print_exc()
        exit(1)
        
    except KeyError as e:
        logger.error(f"key value pair error. Value not found. Verify: {e}")
        print_exc()
        exit(1)
        
    except RuntimeError as e:
        logger.error(f"Failed to create Spark Session. Runtime Error: {e}")
        print_exc()
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occurred. {e}")
        print_exc()
        exit(1)
        
    logger.info(f"Spark Session created: {spark}")

#create surrogate key
def getCreateSurrogateKey(spark, iceberg:str, table_name:str) -> str:
    
    try:
        
        raw_lakehouse = f"{iceberg}.{table_name}"
        
        dataframe = spark.read.format("iceberg").load(raw_lakehouse)
        
        dataframe.createTempView("temp_store")

        dataframe = (spark.sql(f""" 
                            select 
                                 row_number() over (order by storeid) as storekey,
                                *
                            from temp_store
                            where personid is not null
                    """))
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {e}")
        print_exc(e)
        exit(1)
        
    except RuntimeError as e:
        logger.error(f"Invalid runtime error: {e}")
        print_exc(e)
        exit(1)   
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {e}")
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
        logger.error(f"Connection timeout. Please reconnect: {e}")
        print_exc(e)
        exit(1)
                
    except RuntimeError as e:
        logger.error(f"Invalid runtime error: {e}")
        print_exc(e)
        exit(1)
        
    except TypeError as e:
        logger.error(f"Invalid datatype schema. {e}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {e}")
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
        logger.error(f"Connection timeout. Please reconnect: {e}")
        print_exc(e)
        exit(1)
                
    except RuntimeError as e:
        logger.error(f"Invalid runtime error: {e}")
        print_exc(e)
        exit(1)
        
    except TypeError as e:
        logger.error(f"Invalid datatype schema. {e}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {e}")
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
        logger.error(f"Connection timeout. Please reconnect: {e}")
        print_exc(e)
        exit(1)
                
    except RuntimeError as e:
        logger.error(f"Invalid runtime error: {e}")
        print_exc(e)
        exit(1)
        
    except TypeError as e:
        logger.error(f"Invalid datatype schema. {e}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {e}")
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
        logger.error(f"Connection timeout. Please reconnect: {e}")
        print_exc(e)
        exit(1)
                
    except RuntimeError as e:
        logger.error(f"Invalid runtime error: {e}")
        print_exc(e)
        exit(1)
        
    except TypeError as e:
        logger.error(f"Invalid datatype schema. {e}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {e}")
        print_exc(e)
        exit(1)
        
def writeStoreDimension(dataframe:DataFrame, silver_icebergdb:str, table_name:str) -> str:
    
    try:
        lakehouse = f"{silver_icebergdb}.{table_name}"

        (
            dataframe
                .write
                .mode("append")
                .format("iceberg")
                .saveAsTable(f"{lakehouse}")
        )
        
        return f"Table writing successfully completed to iceberg: {table_name}"
    

    
    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {table_name} to iceberg: {e}")
        return "Py4JJavaError"
    
    except NoCredentialsError as e:
        logger.error(f"Missing credentials for MinIO/S3: {e}")
        return "NoCredentialsError"
    
    except PartialCredentialsError as e:
        logger.error(f"Incomplete credentials for MinIO/S3: {e}")
        return "PartialCredentialsError"
    
    except Exception as e:
        logger.error(f"Unexpected error while writing table {table_name} to iceberg: {e}")
        return "UnexpectedError"

def sparkStop(spark) -> str:
    try:
        if spark:
            spark.stop()

        return f"Spark Session stoped Successfully: {spark}"
    
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {e}")
        print_exc(e)
        
def main():
    
    try:
        application_name = "dimension_store_table"
        table_name = "raw_dimstore"
        dimension_table = "dim_store"
        
        
        spark, iceberg, iceberg_db = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, iceberg= iceberg,table_mane= table_name)
        logger.info(f"Dimension surrogate key created: {dataframe.schema}")
        
        dataframe = transformStoreBusinessType(dataframe)
        logger.info(f"Column Business Type processed successfully: {dataframe.schema}")
        
        dataframe = transformStoreRevenue(dataframe)
        logger.info(f"Revenue Column processed successfully: {dataframe.schema}")
        
        dataframe = transformStoreBrands(dataframe)
        logger.info(f"Brand column processed successfully: {dataframe.schema}")
        
        dataframe = cleanerStoreDataframe(dataframe)
        logger.info(f"Columns processed successfully: {dataframe.schema}")

        writeStoreDimension(dataframe, silver_icebergdb= iceberg_db, table_name= dimension_table)
        logger.info(f"dataframe has been writted successfully: {dataframe.printSchema()} 
                        on path: {iceberg_db}.{dimension_table}")
        
        sparkStop(spark)
        logger.info("Spark Session stopped")
        
        logger.info("All functions have been executed successfully")
        return "Success"
    
    except FileNotFoundError as e:
        logger.error(f"Configuration file or resource not found: {e}")
        return "FileNotFoundError"
    

    
    except Py4JJavaError as e:
        logger.error(f"Java/Scala error during Spark operation: {e}")
        return "Py4JJavaError"
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return "UnexpectedError"
    
    finally:
        # Garante que a sessão do Spark seja encerrada, mesmo em caso de erro
        if 'spark' in locals():
            sparkStop(spark)
            
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully") 