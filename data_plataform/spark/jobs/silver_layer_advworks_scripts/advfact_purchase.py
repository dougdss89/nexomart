from startSparkSession import SparkSessionManager
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
from configconnections_loader import validate_required_keys, load_config
from sys import exit
from traceback import print_exc
import logging

from py4j.protocol import Py4JJavaError #java exception error


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

#import .env file

def load_source_configurations(spark_application: str):
    
    try:
        config = load_config()
        
        delta_paths = config.get("delta_path", {})
        
        validate_required_keys(
            config=delta_paths,
            required_keys=["raw_icebergdb", "silver_layer"],
            config_name="delta_path"
        )
        
        spark_manager = SparkSessionManager(spark_application)
        
        spark = spark_manager.create_session()
        
        return spark, delta_paths.get("raw_icebergdb", ""), delta_paths.get("silver_layer", "")

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
    
    
#create surrogate key
def getCreateSurrogateKey(spark, bronze_base: str, table_name: str) -> DataFrame:
    
    try:
        path = f"{bronze_base}/advworks_rawdb/{table_name}"
        
        dataframe = spark.read.format("delta").load(path)
        
        dataframe.createTempView("temp_fact_purchase")
        
        dataframe = spark.sql(""" 
                                select 
                                    row_number() over(order by purchaseorderid) as fact_purchasekey,
                                    cast(unitprice as decimal(12, 2)) as unitprice_ds,
                                    cast(freight as decimal(12,2)) as freight_ds,
                                    cast(taxamt as decimal(12,2)) as taxamt_ds,
                                    cast(linetotal as decimal(12, 2)) as linetotal_ds,
                                    cast(subtotal as decimal(12, 2)) as subtotal_ds,
                                    cast(totaldue  as decimal(12, 2)) as totaldue_ds,
                                    
                                    *
                                from temp_fact_purchase
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
        
#cleaning columns
def cleanerFactDataframe(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe = (dataframe
                        .select(
                            "fact_purchasekey",         "PurchaseOrderID",
                            "PurchaseOrderDetailID",    "revisionnumber",
                            "status",                   "employeeid",
                            "vendorid",                 "orderdate",
                            "shipdate",                 "duedate",
                            "shipmethodid",             "productid",
                            "orderqty",                 "receivedqty",
                            "rejectedqty",              "stockedqty",
                            "unitprice",                "unitprice_ds",
                            "freight",                  "freight_ds",
                            "taxamt",                   "taxamt_ds",
                            "linetotal",                "linetotal_ds",
                            "subtotal",                 "subtotal_ds",
                            "totaldue",                 "totaldue_ds",
                            "inserted_date",            "modified_date",
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
        
def writePurchaseFact(dataframe: DataFrame, silver_base: str, fact_name: str) -> DataFrame:
    
    try:
        path = f"{silver_base}/advworks_db/{fact_name}"
        (
            dataframe
                .write
                .mode("overwrite")
                .format("delta")
                .option("mergeSchema", "true")
                .save(path)
        )
        
        logger.info(f"Table {fact_name} was written successfully to iceberg {path}")
        
        return f"Table writing successfully completed to iceberg: {fact_name}"
        
    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {fact_name} to iceberg: {str(e)}")
        return "Py4JJavaError"

    except Exception as e:
        logger.error(f"Unexpected error while writing table {fact_name} to iceberg: {str(e)}")
        return "UnexpectedError"
    
#stop spark session
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
        application_name = "fact_purchase_table"
        table_name = "raw_fact_purchase"
        fact_name = "fact_purchase"
        
        spark, bronze_base, silver_base = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, bronze_base, table_name)
        logger.info(f"Surrogate Key created: {dataframe.schema}")
        
        dataframe = cleanerFactDataframe(dataframe)
        logger.info(f"""dataframe table has been cleaned: {dataframe.schema}""")
        
        writePurchaseFact(dataframe, silver_base=silver_base, fact_name=fact_name)
        logger.info(f"""dataframe has been writted successfully: {dataframe.printSchema()} 
                        on path: {silver_base}.{fact_name}""")
        
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
    
    main()
    logger.info("Sparkjob completed successfully") 