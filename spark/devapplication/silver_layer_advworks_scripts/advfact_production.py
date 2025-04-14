from startSparkSession import SparkSessionManager
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
from configconnections_loader import validate_required_keys, load_config
from sys import exit
from traceback import print_exc
import logging


from py4j.protocol import Py4JJavaError #java exception error


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(format)s - %(message)s")
logger = logging.getLogger(__name__)

#import env file

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
    
#surrogate key
def getCreateSurrogateKey(spark, catalog:str, iceberg:str, table_name:str) -> DataFrame:

    
    try:
        raw_lakehouse = f"{catalog}.{iceberg}.{table_name}"

        dataframe = spark.read.format("iceberg").load(raw_lakehouse)
        
        dataframe.createTempView("temp_fact_production")
        
        dataframe = spark.sql(f""" 
                              
                                select 
                                
                                    row_number() over (order by workorderid) as fact_productionkey, 
                                    cast(actualcost as decimal(12,2)) as actualcost_ds,
                                    cast(plannedcost as decimal(12,2)) as plannedcost_ds,
                                    *

                                from temp_fact_production
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
        
def writeProductionFact(dataframe:DataFrame, catalog:str, silver_icebergdb:str, fact_name:str) -> DataFrame:
    
    try:
        lakehouse = f"{catalog}.{silver_icebergdb}.{fact_name}"
        (
            dataframe
                .write
                .mode("append")
                .format("iceberg")
                .saveAsTable(f"{lakehouse}")
        )
        
        logger.info(f"Dimension {fact_name} was written successfully to Iceberg {silver_icebergdb}")
        
        return f"Dimension writing successfully completed to Iceberg: {fact_name}"
        
    except AnalysisException as e:
        logger.error(f"Analysis error while writing table {fact_name} to Iceberg: {e}")
        return "AnalysisException"
    
    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {fact_name} to Iceberg: {e}")
        return "Py4JJavaError"

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
    application_name = "fact_production_table"
    
    try:
        spark, iceberg, iceberg_db, iceberg_catalog = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, iceberg_catalog, iceberg, table_name)
    
        dataframe = fillNullValues(dataframe)
        logger.info(f"fillNullValues schema: {dataframe}")
        
        dataframe = cleanerfactDataFrame(dataframe)
        logger.info(f"cleanerfactDataFrame schema: {dataframe}")
               
        writeProductionFact(dataframe, catalog=iceberg_catalog, iceberg_db=iceberg_db, fact_name=fact_name)
        logger.info(f"writeProductionFact schema: {dataframe.schema}")
        
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

if __name__ == "__main__":
    
    main()
    logger.info("Sparkjob completed successfully") 