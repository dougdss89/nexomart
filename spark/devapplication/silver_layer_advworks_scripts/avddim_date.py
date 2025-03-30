from os import write
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)% - %(message)s")
logger = logging.getLogger(__name__)

def load_source_configurations():
    
    try:
        config = load_config()
        
        iceberg_rawdb = config.get("hadoop_paths", {})
        iceberg_db = config.get("hadoop_paths", {})
        
        validate_required_keys(
            config = iceberg_rawdb,
            required_keys= ["raw_icebergdb", "silver_icebergdb"],
            config_name= "hadoop_paths"
        )

        spark_manager = SparkSessionManager(app_name= "dimension_date_table")
        
        spark = spark_manager.create_session()
    
        logging.info(f"All key values configurations has been imported successfully")
        
        return spark, iceberg_rawdb.get("raw_icebergdb", ""), iceberg_db.get("silver_icebergdb", "")
    
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
        logger.error(f"Unexpected error occur. {e}")
        print_exc()
        exit(1)
        
    logger.info(f"Spark Session created: {spark}")
    
#create surrogate key

def getCreateSurrogateKey(spark,  bronze_delta: str, path_name:str, table_name: str,) -> DataFrame:
    
    try:
        
        minio_bronze_path = f"{bronze_delta}/{path_name}/{table_name}"
        
        dataframe = spark.read.format("delta").load(minio_bronze_path)
        
        dataframe.createTempView("temp_dimdate")
        
        dataframe = spark.sql(f""" 
                                select 
                                
                                    *
                                    
                                from temp_dimdate
                                order by datekey
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
        
def writeDateDimension(dataframe:DataFrame, silver_layer:str, file_path:str, table_name:str) -> str:
    
    try:
        minio_path = f"{silver_layer}/{file_path}/{table_name}"
        
        (
            dataframe
                .write
                .mode("append")
                .format("iceberg")
                .save(f"{minio_path}")
        )
        
        return f"Table writing successfully completed to delta: {table_name}"
               
    except AnalysisException as e:
        logger.error(f"Analysis error while writing table {table_name} to Iceberg: {e}")
        return "AnalysisException"
    
    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {table_name} to Iceberg: {e}")
        return "Py4JJavaError"
    
    except NoCredentialsError as e:
        logger.error(f"Missing credentials for MinIO/S3: {e}")
        return "NoCredentialsError"
    
    except PartialCredentialsError as e:
        logger.error(f"Incomplete credentials for MinIO/S3: {e}")
        return "PartialCredentialsError"
    
    except Exception as e:
        logger.error(f"Unexpected error while writing table {table_name} to Iceberg: {e}")
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
        
        table_name = "raw_dimdate"
        dimension_name = "dim_date"
        application_name = "dimension_dimdate_table"
        folder_path = "advworks" 
    
        spark, bronze_delta, silver_delta = load_source_configurations(application_name)

        dataframe = getCreateSurrogateKey(spark, bronze_delta, folder_path, table_name)
        logger.info(f"SurrogateKey schema: {dataframe.schema}")
                
        writeDateDimension(dataframe, silver_delta, folder_path, dimension_name)
        
        sparkStop(spark)
        
        logger.info("All functions have been executed successfully")
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
            
if __name__ == "_main__":
    
    main()
    logger.info("Sparkjob completed successfully") 