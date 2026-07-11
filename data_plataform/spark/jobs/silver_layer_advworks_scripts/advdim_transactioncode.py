from startSparkSession import SparkSessionManager
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
from configconnections_loader import validate_required_keys, load_config
from sys import exit
from traceback import print_exc
import logging


from py4j.protocol import Py4JJavaError #java exception error


#logging
logging.basicConfig(level = logging.INFO, format = "%(asctime)s - %(levelname)s - %(message)s")
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

#create surrogate key
def getCreateSurrogateKey(spark, table_name: str) -> DataFrame:

    
    try:
        
        dataframe = spark.table(f"iceberglake.bronze.{table_name}")
        
        dataframe.createTempView("temp_transactioncode")

        dataframe = (spark.sql(""" 
                           select 
                                row_number() over (order by salesorderid) as transactioncode_key,
                                *
                            from temp_transactioncode
                        """))
        
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
        
#transform address
def transformTransactionCodeNumber(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                        .selectExpr("*", 
                            "coalesce(purchaseordernumber, 'onlineorder') as purchasenumber",
                            "coalesce(carriertrackingnumber, 'onlineorder') as carriernumber",
                            "coalesce(creditcardapprovalcode, 'cash payment') as approvalcode",
                                    
                            """ 
                            case
                                when onlineorderflag = 'true' then 1
                                else 0
                            end as isonlineorder
                                    
                            """ 
                        ))

        
        logger.info("Transaction Number column transformed successfully")
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


#organize columns
def cleanerTransactionCodeDataframe(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe.select("transactioncode_key",
                        "SalesOrderID",
                        "purchasenumber",
                        "salesordernumber",
                        "carriernumber",
                        "accountnumber",
                        "approvalcode",
                        "isonlineorder",
                        "inserted_date",
                        "modified_date",
                        "eltversion"
        )
        
        logger.info("Columns transformed successfully")
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
        
def writetransactioncodeDimension(spark, dataframe: DataFrame, table_name: str) -> DataFrame:
    
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
        logger.error(f"Java/Scala error while writing table {table_name} to iceberg: {str(e)}")
        return "Py4JJavaError"

    except Exception as e:
        logger.error(f"Unexpected error while writing table {table_name} to iceberg: {str(e)}")
        return "UnexpectedError"

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
        application_name = "dimension_transactioncode_table"
        table_name = "raw_dimtransactioncode"
        dimension_table = "dim_transactioncode"
        
        
        spark = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, table_name)
        logger.info(f"Dimension surrogate key created: {dataframe.schema}")
        
        dataframe = transformTransactionCodeNumber(dataframe)
        logger.info(f"Column Business Type processed successfully: {dataframe.schema}")
        
        dataframe = cleanerTransactionCodeDataframe(dataframe)
        logger.info(f"Revenue Column processed successfully: {dataframe.schema}")
        
        writetransactioncodeDimension(spark, dataframe, table_name=dimension_table)
        logger.info(f"""dataframe has been writted successfully: {dataframe.printSchema()} 
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
    main()
    logger.info("Sparkjob completed successfully") 