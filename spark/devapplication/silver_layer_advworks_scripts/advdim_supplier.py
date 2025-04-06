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

#logging
logging.basicConfig(level = logging.INFO, format = "%(asctime)s - %(format)s - %(message)s")
logger = logging.getLogger(__name__)

#import .env file
def load_source_configurations(spark_application: str):
    
    try:
        config = load_config()
        
        iceberg = config.get("hadoop_paths", {})
        iceberg_db = config.get("hadoop_paths", {})
        
        validate_required_keys(
            config = iceberg,
            required_keys= ["iceberg", "iceberg_db"],
            config_name = "hadoop_paths"
        )
        
        spark_manager = SparkSessionManager(spark_application)
        
        spark = spark_manager.create_session()
        
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
        logger.error(f"Unexpected error occurred. {e}")
        print_exc()
        exit(1)
        
    logger.info(f"Spark Session created: {spark}")

#create surrogate key
def getCreateSurrogateKey(spark, iceberg:str, table_name:str) -> str:
    
    try:
        
        raw_lakehouse = f"{iceberg}.{table_name}"
        
        dataframe = spark.read.format("iceberg").load(raw_lakehouse)
        
        dataframe.createTempView("temp_supplier")

        dataframe = (spark.sql(f""" 
                            select 
                                row_number() over(order by businessentityid) as supplierkey,
                                businessentityid as supplierid,
                                productid as productseller_id,
                                *
                            from temp_supplier
                        """))
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
        
def writeSupplierDimension(dataframe:DataFrame, silver_icebergdb:str, table_name:str) -> str:
    
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
        logger.error(f"Java/Scala error while writing table {table_name} to iceberg: {e}")
        return "Py4JJavaError"
    
    except NoCredentialsError as e:
        logger.error(f"Missing credentials for MinIO/S3: {e}")
        return "NoCredentialsError"
    
    except PartialCredentialsError as e:
        logger.error(f"Incomplete credentials for MinIO/S3: {e}")
        return "PartialCredentialsError"
    
    except Exception as e:
        logger.error(f"Unexpected error while writing table {table_name} to iceberg: {e}")
        return "UnexpectedError"

def sparkStop(spark) -> str:
    try:
        if spark:
            spark.stop()

        return f"Spark Session stoped Successfully: {spark}"
    
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {e}")
        print_exc(e)
        
def main():
    
    try:
        application_name = "dimension_supplier_table"
        table_name = "raw_dimsupplier"
        dimension_table = "dim_supplier"
        
        
        spark, iceberg, iceberg_db = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, iceberg= iceberg,table_mane= table_name)
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

        writeSupplierDimension(dataframe, silver_icebergdb= iceberg_db, table_name= dimension_table)
        logger.info(f"dataframe has been writted successfully: {dataframe.printSchema()} 
                        on path: {iceberg_db}.{dimension_table}")
        
        sparkStop(spark)
        logger.info("Spark Session stopped")
        
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