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

#create log configurations

logging. basicConfig(level = logging.INFO, format= "%(asctime)s - %(levelname)s - %(message)%")
logger = logging.getLogger(__name__)

#import .env config

#import .env file
def load_source_configurations(spark_application: str):
    
    try:
        config = load_config()
        
        bronze_delta = config.get("delta_path", {})
        silver_delta = config.get("delta_path", {})
        
        validate_required_keys(
            config = bronze_delta,
            required_keys= ["bronze_delta", "silver_delta"],
            config_name = "delta_path"
        )
        
        spark_manager = SparkSessionManager(spark_application)
        
        spark = spark_manager.create_session()
        
        return spark, bronze_delta.get("bronze_layer", ""), silver_delta.get("silver_layer", "")

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
    
# starting dimension transformation

def getCreateSurrogateKey(spark, bronze_delta:str, file_path:str, table_name:str) -> DataFrame:
    
    try:
        minio_bronze_path = f"{bronze_delta}/{file_path}/{table_name}"

        dataframe = spark.sql(f""" 
                              select 
                                currencycode,
                                currencyname,
                                countryregioncode,
                                countryname,
                                inserted_date,
                                modified_date,
                                eltversion
                            from {minio_bronze_path}
                        """)
        
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
        
        
def writeCurrencyCodeDimension(dataframe:DataFrame, silver_layer:str, file_path:str, table_name:str) -> str:
    
    try:
        minio_path = f"{silver_layer}/{file_path}/{table_name}"

        (
            dataframe
                .write
                .mode("append")
                .format("delta")
                .save(f"{minio_path}")
        )
        
        return f"Table writing successfully completed to delta: {table_name}"
    
    except AnalysisException as e:
        logger.error(f"Analysis error while writing table {table_name} to delta: {e}")
        return "AnalysisException"
    
    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {table_name} to delta: {e}")
        return "Py4JJavaError"
    
    except NoCredentialsError as e:
        logger.error(f"Missing credentials for MinIO/S3: {e}")
        return "NoCredentialsError"
    
    except PartialCredentialsError as e:
        logger.error(f"Incomplete credentials for MinIO/S3: {e}")
        return "PartialCredentialsError"
    
    except Exception as e:
        logger.error(f"Unexpected error while writing table {table_name} to delta: {e}")
        return "UnexpectedError"
    
def sparkStop(spark) -> str:
    
    try:
        if spark:
            spark.stop()
            logger.info("SparkSession stoped successfully")
            
        return f"{spark} Spark Session Stoped"
            
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {e}")
        print_exc()
        
def main() -> str:
    
    try:
        application_name = "dimension_CurrencyCode_table"
        table_name = "raw_dimcurrencycode"
        dimension_table = "dim_currencycode"
        folder_path = "advworks"
        
        spark, bronze_delta, silver_delta = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, bronze_delta, folder_path, table_name)
        
        writeCurrencyCodeDimension(dataframe, silver_delta, folder_path, dimension_table)
        
        logger.info("All functions have been executed sucessfuly")
        
        return "Success"
    
    except FileNotFoundError as e:
        logger.error(f"Configuration file or resource not found: {e}")
        return "FileNotFoundError"
    
    except AnalysisException as e:
        logger.error(f"Data analysis error: {e}")
        return "AnalysisException"
    
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
    logger.info("Sparkjob completed sucessfully")
    
