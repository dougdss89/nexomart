from venv import logger
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

#setting the elt log level
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger=logging.getLogger("__name__")

#load .env file configurations

def load_source_configurations(spark_application: str):
    
    try:
        config = load_config()
        
        iceberg = config.get("hadoop_paths", {})
        iceberg_db = config.get("hadoop_paths", {})
        
        validate_required_keys(
            config = iceberg,
            required_keys= ["raw_icebergdb", "silver_icebergdb"],
            config_name = "hadoop_paths"
        )
        
        session_manager = SparkSessionManager(spark_application)
        
        spark = session_manager.create_session()
        
        logger.info("All key values settings have been imported")
        
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
        logger.error(f"Unexpected error occur. {e}")
        print_exc()
        exit(1)
        
    logger.info(f"Spark Session created: {spark}")
    
# start dimension transformation

def getCreateSurrogateKey(spark, iceberg:str, table_name:str) -> str:
    
    try:
        
        raw_lakehouse = f"{iceberg}.{table_name}"
        
        dataframe = spark.read.format("iceberg").load(raw_lakehouse)
        
        dataframe.createTempView("temp_countryregion")
        
        dataframe = spark.sql(f""" 
                        select 
                            row_number() over(order by territoryid) as countrykey,
                            territoryid as countryid,
                            group as continent,
                            name as countryregion_name,

                            case
                                when countryregioncode = "US" then "United States"
                                when countryregioncode = "CA" then "Canada"
                                when countryregioncode = "FR" then "France"
                                when countryregioncode = "DE" then "Germany"
                                when countryregioncode = "AU" then "Australia"
                                when countryregioncode = "GB" then "Great Britain"
                            else countryregioncode 
                            end as countryname,

                            extracted_date,
                            coalesce(modified_date, '2999-12-31') as modified_date,
                            eltversion
                            
                        from temp_countryregion
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
        
def writeCountryRegionDimension(dataframe:DataFrame, silver_icebergdb:str, table_name:str) -> str:
    
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
            logger.info("SparkSession stoped successfully")
            
        return f"{spark} Spark Session Stoped"
            
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {e}")
        print_exc(e)
        
def main():
    
    table_name = "raw_dimcountryregion"
    dimension_table = "dim_countryregion"
    application_name = "dimension_countryregion_table"
     
    
    try:
        
        spark, iceberg, iceberg_db = load_source_configurations(application_name)
        
        dim_countryregion = getCreateSurrogateKey(spark, iceberg, table_name)
        
        writeCountryRegionDimension(dim_countryregion, iceberg_db, dimension_table)
        
        logger.info("All functions has been executed successfully")
        
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