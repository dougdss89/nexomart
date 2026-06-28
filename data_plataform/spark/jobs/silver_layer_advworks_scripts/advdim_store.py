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
        
        dataframe.createTempView("temp_store")

        dataframe = (spark.sql(""" 
                            select 
                                 row_number() over (order by storeid) as storekey,
                                *
                            from temp_store
                            where personid is not null
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
        
#transform businesstype
def transformStoreBusinessType(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                .withColumn("businesstype", regexp_replace("businesstype", r"(?i)\bbm\b", "Bike Mall"))
                .withColumn("businesstype", regexp_replace("businesstype", r"(?i)\bos\b", "Online store"))
                .withColumn("businesstype", regexp_replace("businesstype", r"(?i)\bbs\b", "Bike store"))
                .withColumn("yearsinactivity", year(current_date()) - col("yearopened"))
            ) 
        
        logger.info("Business Type column transformed successfully")
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
def transformStoreRevenue(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = dataframe.withColumn(
                    "storesize",
                        when(col("annualrevenue") <= 30000.00, "Local store")
                    .when((col("annualrevenue") > 30000.00) & (col("annualrevenue") < 80000.00), "Small store")
                    .when((col("annualrevenue") > 80000.00) & (col("annualrevenue") < 100000.00), "Medium store")
                    .when((col("annualrevenue") > 100000.00) & (col("annualrevenue") <= 150000.00), "Large store")
                .otherwise("Global store")
            )
                
        logger.info("Business Type column transformed successfully")
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
def transformStoreBrands(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = dataframe.withColumn(
                        "brandExpertise",
                        when((col("brands") == "3"), "Senior")
                        .when((col("brands") >= 1) & (col("brands") <= 2), "Medium Knowledge")
                        .otherwise("Expert")
                    )
        logger.info("Brand column transformed successfully")
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
def cleanerStoreDataframe(dataframe: DataFrame) -> DataFrame:
    
    try:
        store = (dataframe.select(      
                                "storekey",			"storeid",
                                "PersonID",			"CustomerID",
                                "AccountNumber", 	"storename",
                                "FirstName",		"LastName",
                                "AnnualSales",		"AnnualRevenue",
                                "BankName",			"businesstype",
                                "YearOpened",		"Specialty",
                                "SquareFeet",		"Brands",
                                "Internet",			"NumberEmployees",
                                "storeID",	"territoryid",
                                "continentname",	"regionname",
                                "CountryRegionCode","yearsinactivity",
                                "storesize",		"brandExpertise",	
                                "inserted_date",	"modified_date",
                                "eltversion"
                            ).withColumnRenamed("territoryid", "countryid")
                             .withColumnRenamed("personid", "clientid")
                             .withColumnRenamed("storeid", "vendorid")
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
        
def writeStoreDimension(dataframe: DataFrame, silver_base: str, table_name: str) -> DataFrame:
    
    try:
        path = f"{silver_base}/advworks_db/{table_name}"

        (
            dataframe
                .write
                .mode("overwrite")
                .format("delta")
                .option("mergeSchema", "true")
                .save(path)
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
        application_name = "dimension_store_table"
        table_name = "raw_dimstore"
        dimension_table = "dim_store"
        
        
        spark, bronze_base, silver_base = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, bronze_base, table_name)
        logger.info(f"Dimension surrogate key created: {dataframe.schema}")
        
        dataframe = transformStoreBusinessType(dataframe)
        logger.info(f"Column Business Type processed successfully: {dataframe.schema}")
        
        dataframe = transformStoreRevenue(dataframe)
        logger.info(f"Revenue Column processed successfully: {dataframe.schema}")
        
        dataframe = transformStoreBrands(dataframe)
        logger.info(f"Brand column processed successfully: {dataframe.schema}")
        
        dataframe = cleanerStoreDataframe(dataframe)
        logger.info(f"Columns processed successfully: {dataframe.schema}")

        writeStoreDimension(dataframe, silver_base=silver_base, table_name=dimension_table)
        logger.info(f"""dataframe has been writted successfully: {dataframe.schema} 
                        on path: {silver_base}.{dimension_table}""")
        
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