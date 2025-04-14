from os import write
from startSparkSession import SparkSessionManager
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
from configconnections_loader import validate_required_keys, load_config
from sys import exit
from traceback import print_exc
import logging


from py4j.protocol import Py4JJavaError #java exception error

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)% - %(message)s")
logger = logging.getLogger(__name__)

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

def getCreateSurrogateKey(spark,  iceberg: str, path_name:str, table_name: str,) -> DataFrame:
    
    try:
        
        raw_lakehouse = f"{iceberg}/{path_name}/{table_name}"
        
        dataframe = spark.read.format("iceberg").load(raw_lakehouse)
        
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
        
def writeDateDimension(dataframe:DataFrame, catalog:str, silver_icebergdb:str, table_name:str) -> DataFrame:
    
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
        logger.error(f"Java/Scala error while writing table {table_name} to Iceberg: {e}")
        return "Py4JJavaError"

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
        dimension_table = "dim_date"
        application_name = "dimension_dimdate_table"
         
    
        spark, iceberg, iceberg_db, iceberg_catalog = load_source_configurations(application_name)

        dataframe = getCreateSurrogateKey(spark, iceberg_catalog, iceberg, table_name)
        logger.info(f"SurrogateKey schema: {dataframe.schema}")
                
        writeDateDimension(dataframe, catalog=iceberg_catalog, silver_icebergdb= iceberg_db, table_name= dimension_table)
        
        sparkStop(spark)
        
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
            
if __name__ == "_main__":
    
    main()
    logger.info("Sparkjob completed successfully") 