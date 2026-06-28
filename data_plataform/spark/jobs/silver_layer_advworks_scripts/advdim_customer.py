from startSparkSession import SparkSessionManager
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
from configconnections_loader import validate_required_keys, load_config
from sys import exit
from traceback import print_exc
import logging


from py4j.protocol import Py4JJavaError #java exception error


logging.basicConfig(level=logging.INFO, format = "%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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
    
#create surrogate key column
def getCreateSurrogateKey(spark, bronze_base: str, table_name: str) -> DataFrame:

    
    try:
        
        path = f"{bronze_base}/advworks_rawdb/{table_name}"
        
        dataframe = spark.read.format("delta").load(path)
        
        dataframe.createOrReplaceTempView("temp_customer")        
        
        dataframe = spark.sql("""
                                select 
                                
                                    row_number() over(order by business_id) as customer_key,
                                    * 
                                from temp_customer
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
       
#transform and cleaning address column 
def transformAddressCustomer(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe = (dataframe
                        .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bln\b",   "Lane"))
                        .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\brd\b",   "Road"))
                        .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bDr\b",   "Drive"))
                        .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bCt\b",   "Center"))
                        .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bst\b",   "Street"))
                        .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bblvd\b", "Boulevard"))
                        .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bave\b",  "Avenue"))
                        .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bpk\b",   "Parkway"))
                        .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bterr\b", "Terrace"))
                        .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bpl\b",   "Plaza"))
                        .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bmt\b",   "Mountain"))
                        .withColumn("addressline1", regexp_replace("addressline1", r"(?i)\bn.\b",   "North"))
                )

        dataframe= (dataframe
                        .withColumn("addressnumber", regexp_extract("addressline1", r"(\d+)", 1))
                        .withColumn("custaddress",   regexp_replace("addressline1", r"\d+",   ""))
                    )


        dataframe = (dataframe 
                        .withColumn("custaddress", regexp_replace("custaddress", r"[^a-zA-Z ]", ""))

                        )

        dataframe = (dataframe
                        .select('*').withColumn("custaddress", trim("custaddress"))
                    )
        logger.info(f"The data source were successfully transformed: {dataframe}")
        
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
  
#cleaning complementary customer address      
def transformComplementaryCustomerAddress(dataframe) -> DataFrame:
    
    try:
        
        dataframe = (dataframe
                        .select("*", "addressline2",
                            coalesce(dataframe.addressline2, lit("Not Available"), "addressline2")   
                            .alias("complement_address"))
                        )           
        
        logger.info(f"The data source were successfully transformed: {dataframe}")
        
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
        
#cleaning and conforming demographic customer data
def transformCustomerDemographicData(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe = (
            dataframe
                .withColumn("customer_gender", 
                            when(col("gender") == "M", "Male").otherwise("Female"))
                .withColumn("ishomeowner", 
                            when(col("HomeOwnerFlag") == 1, "yes").otherwise("no"))
                .withColumn("ismarried", 
                            when(col("maritalstatus") == "M", "Married").otherwise("Single"))
                .withColumn("iscarowner", 
                            coalesce(col("numbercarsowned"), lit("0")))
            )
        logger.info(f"The data source were successfully transformed: {dataframe}")
        
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
        
#age column adjustment
def transformCustomerAge(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe = (dataframe
                        .withColumn("age", year(current_date())- year(col("birthdate")))
                    )
        
        logger.info(f"The data source were successfully transformed: {dataframe}")
        
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
  
#order dataset column      
def cleanCustomerDataframe(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe = dataframe.select(
                            "customer_key",         "business_id",
                            "customer_id",          "firstname",
                            "lastname",             "persontype",
                            "emailpromotion",       "typephone",
                            "phonenumber",          "postalcode",
                            "addresstype",          "emailaddress",
                            "territory_id",         "countryname",
                            "stateprovince_id",     "countryregioncode",
                            "stateprovincecode",    "statename",
                            "city",                 "custaddress",
                            "addressnumber",        "complement_address",
                            "totalpurchase_ytd",    "DateFirstPurchase",
                            "BirthDate",            "age",
                            "ismarried",            "customer_gender",
                            "minincome",            "maxincome",
                            "iscarowner",           "ishomeowner",
                            "TotalChildren",        "NumberChildrenAtHome",
                            "Education",            "Occupation",
                            "HomeOwnerFlag",        "NumberCarsOwned",
                            "extracted_date",       "modified_date",
                            "eltversion")
        
        
        logger.info(f"The data source were successfully transformed: {dataframe}")
        
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
        
def writeCustomerDimension(dataframe: DataFrame, silver_base: str, table_name: str) -> DataFrame:
    
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
        application_name = "dimension_customer_table"
        table_name = "raw_dimcustomer"
        dimension_table = "dim_customer"
        
        
        spark, bronze_base, silver_base = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, bronze_base, table_name)
        logger.info(f"Dimension surrogate key created: {dataframe.schema}")
    
        dataframe = transformAddressCustomer(dataframe)
        logger.info(f"AddressCustomer schema: {dataframe.schema}")
        
        dataframe = transformComplementaryCustomerAddress(dataframe)
        logger.info(f"ComplementaryCustomerAddress schema: {dataframe.schema}")
        
        dataframe = transformCustomerDemographicData(dataframe)
        logger.info(f"CustomerDemographicData schema: {dataframe.schema}")
        
        dataframe = transformCustomerAge(dataframe)
        logger.info(f"CustomerAge schema: {dataframe.schema}")
        
        dataframe = cleanCustomerDataframe(dataframe)
        logger.info(f"CleanCustomerDataframe schema: {dataframe.schema}")
        
        writeCustomerDimension(dataframe, silver_base=silver_base, table_name=dimension_table)
        logger.info(f"""dataframe has been writted successfully: {dataframe.schema} 
                        on path: {silver_base}.{dimension_table}""")
        
        sparkStop(spark)
        
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