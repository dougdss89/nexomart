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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(format)s - %(message)s")
logger = logging.getLogger(__name__)

#import env file

def load_source_configurations():
    
    config = load_config()
    
    try:
        
        iceberg_rawdb = config.get("hadoop_paths", {})
        iceberg_db = config.get("hadoop_paths", {})
        
        validate_required_keys(
            config = iceberg_rawdb,
            required_keys=["raw_icebergdb", "silver_icebergdb"],
            config_name= "hadoop_paths"
        )
        
        spark_manager = SparkSessionManager(app_name= "fact_production_table")
        
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
    
#surrogate key
def getCreateSurrogateKey(spark, iceberg_rawdb: str,  table_name: str) -> DataFrame:
    
    try:
        dataframe = spark.sql(f""" 
                              
                                select 
                                
                                    row_number() over (order by workorderid) as fact_productionkey, 
                                    cast(actualcost as decimal(12,2)) as actualcost_ds,
                                    cast(plannedcost as decimal(12,2)) as plannedcost_ds,
                                    *

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
        
#fill null values
def fillNullValues(dataframe:DataFrame) -> DataFrame:
    
    try:
        dataframe = dataframe.fillna({
                                        "scrapreasonid":      0,
                                        "locationid":         0,
                                        "actualresourcehrs":  0.00,
                                        "plannedcost":        0.00,
                                        "plannedcost_ds":     0.00,
                                        "actualcost_ds":      0.00,
                                        "actualcost":         0.00,
                                        "scheduledstartdate": "2100-12-31 00:00:00",
                                        "scheduledenddate":   "2100-12-31 00:00:00",
                                        "actualstartdate":    "2100-12-31 00:00:00",
                                        "actualenddate":      "2100-12-31 00:00:00"}
                                    )
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

def cleanerfactDataFrame(dataframe:DataFrame) -> DataFrame:
    
    try:
        
        dataframe = (dataframe
                            .select(
                                "fact_productionkey",   "workorderid",
                                "productid",            "orderqty",
                                "stockedqty",           "scrappedqty",
                                "scrapreasonid",        "locationid",
                                "actualresourcehrs",    "plannedcost",
                                "plannedcost_ds",       "actualcost",
                                "actualcost_ds",        "scheduledstartdate",
                                "scheduledenddate",     "actualstartdate",
                                "actualenddate",        "startdate",
                                "enddate",              "duedate",
                                "inserted_date",        "modified_date",
                                "eltversion"))
        
        logger.info(f"The data source were successfully transformed: {dataframe}")
        
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
        
def writeProductionFact(dataframe: DataFrame, iceberg_db:str, fact_name: str) -> str:
    
    try:
        
        (
            dataframe
                .write
                .mode("append")
                .format("iceberg")
                .saveAsTable(f"{iceberg_db}.{fact_name}")
        )
        
        logger.info(f"Dimension {fact_name} was written successfully to Iceberg {iceberg_db}")
        
        return f"Dimension writing successfully completed to Iceberg: {fact_name}"
        
    except AnalysisException as e:
        logger.error(f"Analysis error while writing table {fact_name} to Iceberg: {e}")
        return "AnalysisException"
    
    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {fact_name} to Iceberg: {e}")
        return "Py4JJavaError"
    
    except NoCredentialsError as e:
        logger.error(f"Missing credentials for MinIO/S3: {e}")
        return "NoCredentialsError"
    
    except PartialCredentialsError as e:
        logger.error(f"Incomplete credentials for MinIO/S3: {e}")
        return "PartialCredentialsError"
    
    except Exception as e:
        logger.error(f"Unexpected error while writing table {fact_name} to Iceberg: {e}")
        return "UnexpectedError"
   
#closing spark session 
def sparkStop(spark) -> str:
    
    try:
        if spark:
            spark.stop()
            
        return f"Spark Session stoped Successfully: {spark}"
    
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {e}")
        print_exc(e)
        
#main functions
def main():
    table_name = "raw_fact_production"
    fact_name = "fact_production"
    
    try:
        spark, iceberg_rawdb, iceberg_db = load_source_configurations()
        
        dataframe = getCreateSurrogateKey(spark, iceberg_rawdb, table_name)
    
        dataframe = fillNullValues(dataframe)
        logger.info(f"fillNullValues schema: {dataframe}")
        
        dataframe = cleanerfactDataFrame(dataframe)
        logger.info(f"cleanerfactDataFrame schema: {dataframe}")
               
        writeProductionFact(dataframe, iceberg_db, fact_name)
        logger.info(f"writeProductionFact schema: {dataframe.schema}")
        
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

if __name__ == "__main__":
    
    main()
    logger.info("Sparkjob completed successfully") 