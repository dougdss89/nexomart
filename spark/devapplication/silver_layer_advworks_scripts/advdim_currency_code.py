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

def load_source_configurations():
    
    try:
        
        config = load_config()
        
        iceberg_rawdb = config.get("hadoop_paths", {})
        iceberg_db = config.get("hadoop_paths", {})
        
        #validating key-values pair
        
        validate_required_keys(
            
            config = iceberg_rawdb,
            required_keys= ["raw_icebergdb", "silver_icebergdb"],
            config_name= "hadoop_paths"
        )
        
        spark_manager = SparkSessionManager(app_name= "dimension_currency_code_table")
        
        spark = spark_manager.create_session()
        
        logger.info(f"All keys values settings have been imported")
        
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
    
# starting dimension transformation

def getCreateSurrogateKey(spark, iceberg_rawdb, table_name) -> DataFrame:
    
    try:
        
        dataframe = spark.sql(f""" 
                              select 
                                currencycode,
                                currencyname,
                                countryregioncode,
                                countryname,
                                inserted_date,
                                modified_date,
                                eltversion
                            from icebergtb.{iceberg_rawdb}.{table_name}
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
        
def writeCurrencyCodeDimension(dataframe: DataFrame, iceberg_db, dimension_name) -> str:
    
    try:
        
        (
            dataframe
                .write
                .format("iceberg")
                .mode("append")
                .saveAsTable(f"{iceberg_db}.{dimension_name}")
        )

        logger.info(f"Dimension {dimension_name} was written successfully to Iceberg {iceberg_db}")
        
        return f"Dimension writing successfully completed to Iceberg: {dimension_name}"
    
    except AnalysisException as e:
        logger.error(f"Analysis error while writing table {dimension_name} to Iceberg: {e}")
        return "AnalysisException"
    
    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {dimension_name} to Iceberg: {e}")
        return "Py4JJavaError"
    
    except NoCredentialsError as e:
        logger.error(f"Missing credentials for MinIO/S3: {e}")
        return "NoCredentialsError"
    
    except PartialCredentialsError as e:
        logger.error(f"Incomplete credentials for MinIO/S3: {e}")
        return "PartialCredentialsError"
    
    except Exception as e:
        logger.error(f"Unexpected error while writing table {dimension_name} to Iceberg: {e}")
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
    
    table_name = "raw_dimcurrencycode"
    dimension_name = "dim_currencycode"
    
    try:
        
        spark, iceberg_rawdb, iceberg_db = load_source_configurations()
        
        dim_currencycode = getCreateSurrogateKey(spark, iceberg_rawdb, table_name)
        
        writeCurrencyCodeDimension(dim_currencycode, iceberg_db, dimension_name)
        
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
    
