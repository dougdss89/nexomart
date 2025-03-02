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

logging.basicConfig(level=logging.INFO, format = "%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_source_configurations():
    
    config  = load_config()
    
    try:
        
        iceberg_rawdb = config.get("hadoop_paths", {})
        iceberg_db = config.get("hadoop_paths", {})
        
        validate_required_keys(
            
            config = iceberg_db,
            required_keys= ["raw_icebergdb", "silver_icebergdb"],
            config_name= "hadoop_paths"
        )

        spark_manager = SparkSessionManager("dimension_customer_table")
        
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
    
#create surrogate key column
def getCreateSurrogateKey(spark, iceberg_rawdb, table_name) -> DataFrame:
    
    try:
        
        dataframe = spark.sql(f"""
                                select 
                                
                                    row_number() over(order by business_id) as customer_key,
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
       
#transform and cleaning address column 
def transformAddressCustomer(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe = (dataframe
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

        dataframe= (dataframe
                        .withColumn("addressnumber", regexp_extract("addressline1", r"(\d+)", 1))
                        .withColumn("custaddress",   regexp_replace("addressline1", r"\d+", ""))
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
        
#age column adjustment
def transformCustomerAge(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe = (dataframe
                        .withColumn("age", year(current_date())- year(col("birthdate")))
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
        
def writeCustomerDimension(dataframe:DataFrame, iceberg_db: str, dimension_name:str) -> str:
    
    try:
        
        (
            dataframe
                .write
                .mode("append")
                .format("iceberg")
                .saveAsTable(f"{iceberg_db}.{dimension_name}")
            
        )
        logger.info(f"Dimension {dimension_name} was written successfully to Iceberg {iceberg_db}")
        
        return f"Dimension writing successfully completed to Iceberg: {dimension_name}"
        
    except AnalysisException as e:
        logger.error(f"Analysis error while writing table {dimension_name} to Iceberg: {e}")
        return "AnalysisException"
    
    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {dimension_name} to Iceberg: {e}")
        return "Py4JJavaError"
    
    except NoCredentialsError as e:
        logger.error(f"Missing credentials for MinIO/S3: {e}")
        return "NoCredentialsError"
    
    except PartialCredentialsError as e:
        logger.error(f"Incomplete credentials for MinIO/S3: {e}")
        return "PartialCredentialsError"
    
    except Exception as e:
        logger.error(f"Unexpected error while writing table {dimension_name} to Iceberg: {e}")
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
    table_name = "raw_dimcustomer"
    dimension_table = "dim_customer"
    
    try:
        spark, iceberg_rawdb, iceberg_db = load_source_configurations()
        
        dataframe = getCreateSurrogateKey(spark, iceberg_rawdb, table_name)
    
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
        
        writeCustomerDimension(dataframe, iceberg_db, dimension_table)
        
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