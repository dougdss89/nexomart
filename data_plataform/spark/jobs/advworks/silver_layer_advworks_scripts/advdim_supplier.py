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
        
        dataframe.createTempView("temp_supplier")

        dataframe = (spark.sql(""" 
                            select 
                                row_number() over(order by businessentityid) as supplierkey,
                                businessentityid as supplierid,
                                productid as productseller_id,
                                *
                            from temp_supplier
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
def transformSupplierAddress(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (
                dataframe
                .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bln\b","Lane"))
                .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\brd\b","Road"))
                .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bDr\b","Drive"))
                .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bCt\b","Center"))
                .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bst\b","Street"))
                .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bblvd\b", "Boulevard"))
                .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bave\b","Avenue"))
                .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bpk\b","Parkway"))
                .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bterr\b", "Terrace"))
                .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bpl\b","Plaza"))
                .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bmt\b","Mountain"))
                .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bn.\b","North"))
            )
        
        logger.info("Address column transformed successfully")
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

#annual revenue
def transformSupplierType(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                    .withColumn("suppliername", regexp_replace("name", r"(?i)\bcorp\b", "Corporation"))
                    .withColumn("suppliername", regexp_replace("name", r"(?i)\bco\b", "Company"))
                    .withColumn("suppliername", regexp_replace("name", r"(?i)\binc\b", "Incorporation"))
                    .drop(col("businessentityid"))
                    .drop(col("productid")))
        
        logger.info("Supplier Type column transformed successfully")
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
        
#brands
def dropSupplierAddress(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe 
                    .withColumn("addressnumber", regexp_extract("addressline1", r"(\d+ )", 1))
                    .withColumn("supplieraddress", regexp_replace("addressline1", r"[^a-zA-Z ]", ""))
                    .withColumn("addressline", coalesce(col("addressline2"), lit("Not Available")))
                    .drop(col("addressline1"))
                    .drop(col("addressline2"))
                    
                    
        )
        logger.info("Supplier Type column transformed successfully")
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
def createSupplierRate(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                        .selectExpr('*',
                                    
                            'coalesce(onorderqty, 0) as orderqty',
                            
                            """
                            case 
                                when creditrating = 1 then 'Premier'
                                when creditrating between 2 and 3 then 'Super class'
                                when creditrating = 4 then 'Normal'
                                when creditrating = 5 then 'Attention'
                            else 'No Classification'
                            end as ratingclass
                            """,
                            
                            """
                            case
                                when preferredvendorstatus = 1 then 'Yes'
                                else  'No'
                            end as ispreferred
                            """
                ))
        logger.info("Rating column transformed successfully")
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
        
def cleanerSupplierDataframe(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe.select(
            "supplierkey",          "supplierid",
            "productseller_id",     "accountnumber",
            "name",                 "contacttype",
            "firstname",            "lastname",
            "phonenumber",          "phonenumbertype",
            "creditrating",         "preferredvendorstatus",
            "averageleadtime",      "standardprice",
            "lastreceiptcost",      "lastreceiptdate",
            "minorderqty",          "maxorderqty",
            "onorderqty",           "unitmeasurecode",
            "stateprovincename",    "countryregionname",
            "city",                 "postalcode",
            "inserted_date",        "modified_date",
            "eltversion",           "suppliername",
            "addressnumber",        "supplieraddress",
            "addressline",          "orderqty",
            "ratingclass",          "ispreferred")
        
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
        
def writeSupplierDimension(spark, dataframe: DataFrame, table_name: str) -> DataFrame:
    
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
        application_name = "dimension_supplier_table"
        table_name = "raw_dimsupplier"
        dimension_table = "dim_supplier"
        
        
        spark = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, table_name)
        logger.info(f"Dimension surrogate key created: {dataframe.schema}")
        
        dataframe = transformSupplierAddress(dataframe)
        logger.info(f"Column Business Type processed successfully: {dataframe.schema}")
        
        dataframe = transformSupplierType(dataframe)
        logger.info(f"Revenue Column processed successfully: {dataframe.schema}")
        
        dataframe = dropSupplierAddress(dataframe)
        logger.info(f"Brand column processed successfully: {dataframe.schema}")
        
        dataframe = createSupplierRate(dataframe)
        logger.info(f"Columns processed successfully: {dataframe.schema}")

        dataframe = cleanerSupplierDataframe(dataframe)
        logger.info(f"Columns processed successfully: {dataframe.schema}")

        writeSupplierDimension(spark, dataframe, table_name=dimension_table)
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
        # Ensures the Spark session is closed, even in case of error
        if 'spark' in locals():
            sparkStop(spark)
            
if __name__ == "__main__":
    result = main()
    if result != "Success":
        exit(1)
    logger.info("Sparkjob completed successfully")