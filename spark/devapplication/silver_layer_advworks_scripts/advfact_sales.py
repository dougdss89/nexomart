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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

#import env file

def load_source_configurations():
    
    try:
        config = load_config()
        
        bronze_delta = config.get("delta_path", {})
        silver_delta = config.get("delta_path", {})
        
        validate_required_keys(
            config = bronze_delta,
            required_keys= ["bronze_layer", "silver_layer"],
            config_name = "delta_path"
        )
        
        #creating spark_session
        
        spark_manager = SparkSessionManager("fact_sales_table")
        
        spark = spark_manager.create_session()
        
        spark_manager = SparkSessionManager(app_name= "fact_production_table")
        
        spark = spark_manager.create_session()
        
        logging.info(f"All key values configurations has been imported successfully")
        
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
    
#creating surrogate fact_sales key

def getCreateSurrogateKey(spark, path_name:str, bronze_delta: str, table_name: str,) -> DataFrame:
    
    try:
        
        minio_bronze_path = f"{bronze_delta}/{path_name}/{table_name}"
        
        dataframe = spark.read.format("delta").load(minio_bronze_path)
        
        dataframe.createTempView("temp_fact_sales")
        
        dataframe = spark.sql(f""" 
                                select 
                                    *, 
                                    row_number() over(order by salesorderid) as fact_saleskey,
                                    cast(unitprice as decimal(12,2)) as  unitprice_ds,
                                    cast(linetotal as decimal(12,2)) as  linetotal_ds,
                                    cast(freight as decimal(12,2)) as freight_ds
                                    
                                from temp_fact_sales
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
        
def fillNullValues(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                        .fillna({"salesreasonid": 0, 
                            "currencyrateid": 0, 
                            "salespersonid": 0,
                            "carriertrackingnumber": "online order",
                            "purchaseordernumber": "online order"})
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
        
def cleanerFactDataframe(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe = (
                    dataframe
                        .select(
                            "fact_saleskey",            "SalesOrderID",
                            "SalesOrderDetailID",       "accountnumber",
                            "carriertrackingnumber",    "purchaseordernumber",
                            "salesordernumber",         "OrderDate",
                            "ShipDate",                 "DueDate",
                            "salesreasonid",            "SalesPersonID",
                            "ProductID",                "CustomerID",
                            "ShipMethodID",             "TerritoryID",
                            "SpecialOfferID",           "currencyrateid",
                            "orderqty",                 "unitprice",
                            "unitprice_ds",             "unitpricediscount",
                            "taxamt",                   "freight",
                            "freight_ds",               "onlineorderflag",
                            "LineTotal",                "linetotal_ds",
                            "inserted_date",            "modified_date",
                            "eltversion")
        )

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
        
def writeSalesFact(dataframe: DataFrame,  silver_layer: str, folder_path:str, fact_name: str) -> str:
    
    try:
        
        minio_path = f"{silver_layer}/{folder_path}/{fact_name}"
        (
            dataframe
                .write
                .format("delta")
                .mode("append")
                .save(minio_path)
        )
        

        logger.info(f"Dimension {fact_name} was written successfully to Delta {minio_path}")
        
        return f"Dimension writing successfully completed to delta: {fact_name}"
    
    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {fact_name} to delta: {e}")
        return "Py4JJavaError"
    
    except NoCredentialsError as e:
        logger.error(f"Missing credentials for MinIO/S3: {e}")
        return "NoCredentialsError"
    
    except PartialCredentialsError as e:
        logger.error(f"Incomplete credentials for MinIO/S3: {e}")
        return "PartialCredentialsError"
    
    except Exception as e:
        logger.error(f"Unexpected error while writing table {fact_name} to delta: {e}")
        return "UnexpectedError"
    
def sparkStop(spark) -> str:
    
    try:
        
        if spark:
            spark.stop()
            
        return f"Spark Session stoped Successfully: {spark}"
    
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {e}")
        print_exc(e)
        
def main() -> str:
    
    try:
        table_name = "raw_fact_sales"
        fact_name = "fact_sales"
        folder_path = "advworks"

        spark, bronze_delta, silver_delta = load_source_configurations()
        
        dataframe =  getCreateSurrogateKey(spark, folder_path, bronze_delta, table_name)
        logger.info(f"getSurrogateKey schema: {dataframe.schema}")
        
        dataframe = fillNullValues(dataframe)
        logger.info(f"fillNullValues schema: {dataframe.schema}")
        
        dataframe = cleanerFactDataframe(dataframe)
        logger.info(f"Cleaner table successfully completed: {dataframe.schema}")
        
        writeSalesFact(dataframe, silver_layer=silver_delta, folder_path=folder_path, fact_name=fact_name)
        logger.info(f"""dataframe has been writted successfully: {fact_name}. 
                            Storage path: {silver_delta}/{folder_path}/{fact_name}""")
        
        sparkStop(spark)
        logger.info(f"Spark Session stopped")
        
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