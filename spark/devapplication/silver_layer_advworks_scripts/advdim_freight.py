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
def load_source_configurations(spark_application:str):
    
    try:
        
        config = load_config()
        
        iceberg = config.get("hadoop_paths", {})
        iceberg_db = config.get("hadoop_paths", {})
        
        validate_required_keys(
            config= iceberg,
            required_keys= ["iceberg", "iceberg_db"],
            config_name= "hadoop_paths"
        )
        
        spark_manager = SparkSessionManager(spark_application)
        
        spark = spark_manager.create_session()
        
        logging.info(f"All key values configurations has been imported successfully")
        
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
    
#create surr key
def getCreateSurrogateKey(spark, iceberg:str, file_path: str, table_name: str) -> DataFrame:
    
    try:
        
        raw_lakehouse = f"{iceberg}.{table_name}"
        
        dataframe = spark.read.format("iceberg").load(raw_lakehouse)
        
        dataframe.createTempView("temp_freight")
        
        dataframe = (spark.sql(f""" 
                               
                        select
                            row_number() over(order by shipmethodid) as shipmethod_key,
                            cast(shipbase as decimal(12,2)) as shipbase_ds,
                            cast(shipbase as decimal(12,2)) as shiprate_ds, 
                            *
                       from temp_freight
                    """))
        
        logger.info(f"The data source were successfully extracted: {dataframe}")
        
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
        
def transformFreightCategory(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe = (
                    dataframe
                        .withColumn("category", split(col("name"), "-", 2)[1])
                        .withColumn("carrier",  split(col("name"), "-", 2)[0])
            ).fillna({"category": "BASIC"})
        
        logger.info(f"Dataframe transformed successfully: {dataframe}")
        
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
        
def cleanerFreightDataframe(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe= (dataframe
                        .select(
                            "shipmethod_key",   "shipmethodid",
                            "carrier",          "category",
                            "shipbase",         "shipbase_ds",
                            "shiprate",         "shiprate_ds",
                            "inserted_date",    "modified_date",
                            "eltversion"))
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
        
def writeFreightDimension(dataframe:DataFrame, silver_icebergdb:str, table_name:str) -> str:
    
    try:
        lakehouse = f"{silver_icebergdb}.{table_name}"
        
        (
            dataframe
                .write
                .mode("append")
                .format("iceberg")
                .saveAsTable(f"{lakehouse}")
        )
        
        logger.info(f"Table {table_name} was written successfully to iceberg {lakehouse}")
        
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
        application_name = "dimension_freight_table"
        table_name = "raw_dimfreigth"
        dimension_table = "dim_freight"
        
        
        spark, iceberg, iceberg_db = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, iceberg= iceberg, table_name= table_name)
        logger.info(f"Dimension surrogate key created: {dataframe.schema}")
        
        dataframe = transformFreightCategory(dataframe)
        logger.info(f"Freight Category column created: {dataframe.schema}")
        
        writeFreightDimension(dataframe= dataframe, silver_icebergdb= iceberg_db, table_name= dimension_table)
        logger.info(f"dataframe has been writted successfully: {dataframe.schema} 
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