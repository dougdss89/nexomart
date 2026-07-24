
from startSparkSession import SparkSessionManager
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
from configconnections_loader import validate_required_keys, load_config
from sys import exit
from traceback import print_exc
import logging


from py4j.protocol import Py4JJavaError #java exception error


#setting the elt log level
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger=logging.getLogger("__name__")

#load .env file configurations

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
    
# start dimension transformation

def getCreateSurrogateKey(spark, table_name: str) -> DataFrame:

    
    try:
        
        dataframe = spark.table(f"iceberglake.bronze.{table_name}")
        
        dataframe.createTempView("temp_countryregion")
        
        dataframe = spark.sql(""" 
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
        
def writeCountryRegionDimension(spark, dataframe: DataFrame, table_name: str) -> DataFrame:
    
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
        logger.error(f"Java/Scala error while writing table {table_name} to Iceberg: {str(e)}")
        print_exc()
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error while writing table {table_name} to Iceberg: {str(e)}")
        print_exc()
        exit(1)
    
def sparkStop(spark) -> str:
    
    try:
        if spark:
            spark.stop()
            logger.info("SparkSession stoped successfully")
            
        return f"{spark} Spark Session Stoped"
            
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {str(e)}")
        print_exc(e)
        
def main():
    
    table_name = "raw_dimcountryregion"
    dimension_table = "dim_countryregion"
    application_name = "dimension_countryregion_table"
     
    
    try:
        
        spark = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, table_name)
        
        writeCountryRegionDimension(spark, dataframe, table_name=dimension_table)
        
        logger.info("All functions has been executed successfully")
        
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