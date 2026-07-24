from startSparkSession import SparkSessionManager
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
from configconnections_loader import validate_required_keys, load_config
from sys import exit
from traceback import print_exc
import logging
from py4j.protocol import Py4JJavaError #java exception error

#logging
logging.basicConfig(level = logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#import .env file
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
    
#create surr key
def getCreateSurrogateKey(spark, table_name: str) -> DataFrame:

    
    try:
        
        dataframe = spark.table(f"iceberglake.bronze.{table_name}")
        
        dataframe.createTempView("temp_freight")
        
        dataframe = (spark.sql(""" 
                               
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
        
def transformFreightCategory(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe = (
                    dataframe
                        .withColumn("category", split(col("name"), "-", 2)[1])
                        .withColumn("carrier",  split(col("name"), "-", 2)[0])
            ).fillna({"category": "BASIC"})
        
        logger.info(f"""dataframe transformed successfully: {dataframe}""")
        
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
        
def writeFreightDimension(spark, dataframe: DataFrame, table_name: str) -> DataFrame:
    
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberglake.silver")
        (
            dataframe
            .writeTo(f"iceberglake.silver.{table_name}")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .createOrReplace()
        )
        
        logger.info(f"Table {table_name} was written successfully to iceberg {path}")
        
        return logger.info(f"Table writing successfully completed to iceberg: {table_name}")

    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {table_name} to iceberg: {str(e)}")
        print_exc()
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error while writing table {table_name} to iceberg: {str(e)}")
        print_exc()
        exit(1)
    
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
        application_name = "dimension_freight_table"
        table_name = "raw_dimfreight"
        dimension_table = "dim_freight"
        
        
        spark = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, table_name)
        logger.info(f"Dimension surrogate key created: {dataframe.schema}")
        
        dataframe = transformFreightCategory(dataframe)
        logger.info(f"Freight Category column created: {dataframe.schema}")
        
        writeFreightDimension(spark, dataframe, table_name=dimension_table)
        logger.info(f"""dataframe has been writted successfully: {dataframe.schema}
                        on path: iceberglake.silver.{dimension_table}""")
        
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
    
    result = main()
    if result != "Success":
        exit(1)
    logger.info("Sparkjob completed successfully")