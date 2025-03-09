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

#create logging configurations
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

#import .env file
def load_source_configurations(spark_application: str):
    
    try:
        config = load_config()
        
        bronze_delta = config.get("delta_path", {})
        silver_delta = config.get("delta_path", {})
        
        validate_required_keys(
            config = bronze_delta,
            required_keys= ["bronze_layer", "silver_layer"],
            config_name = "delta_path"
        )
        
        spark_manager = SparkSessionManager(spark_application)
        
        spark = spark_manager.create_session()
        
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
        
# starting dimension transformation

def getCreateSurrogateKey(spark, bronze_delta:str, file_path:str, table_name:str) -> str:
    
    try:
        
        minio_bronze_path = f"{bronze_delta}/{file_path}/{table_name}"
        
        dataframe = spark.sql(f""" 
                            select 
                              
                                row_number() over (order by businessentityid) as buyer_key, 
                                * 
                            from {minio_bronze_path}""")
        
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
        
    except RuntimeError as e:
        logger.error(f"Invalid runtime error: {e}")
        print_exc(e)
        exit(1)
        
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {e}")
        print_exc(e)
        exit(1)
        
def transformBuyerDimension(spark, bronze_delta:str, file_path:str, table_name:str) -> DataFrame:
    
    try:
        minio_bronze_path = f"{bronze_delta}/{file_path}/{table_name}"
        dataframe = spark.sql(f""" 
                        SELECT
                            row_number() over (order by businessentityid) as buyer_key,
                            businessentityid AS buyer_id,    firstname,
                            lastname,                        birthdate,
                            
                            CAST(DATE_DIFF(CURRENT_DATE, birthdate) / 365 AS INT) AS age,

                            CASE
                                WHEN maritalstatus = 'M' THEN 'Married'
                                ELSE 'Single'
                            END AS maritalstatus,

                            CASE
                                WHEN gender = 'F' THEN 'Female'
                                ELSE 'Male'
                            END AS gender,

                            hiredate,
                            
                            CAST(DATE_DIFF(CURRENT_DATE, hiredate) / 365 AS INT) AS years_in_company,
                            
                            departmentid,                    name AS department_name,
                            groupname AS department_manager, jobtitle,
                            payfrequency,                    rate,
                            salarychange_date,               startdate,

                            CASE
                                WHEN enddate IS NULL AND startdate = salarychange_date THEN COALESCE(enddate, '2999-12-31')
                                WHEN enddate IS NULL AND (DAY(startdate) - DAY(salarychange_date) = 1) THEN salarychange_date
                                WHEN enddate IS NULL AND MAX(salarychange_date) < MAX(startdate) THEN COALESCE(enddate, '2999-12-31')
                                ELSE enddate
                            END AS enddate,

                            salariedflag AS is_salaried,    currentflag AS is_current,
                            
                            extracted_date,
                            
                            COALESCE(modified_date, '2999-12-31') AS modified_date,
                            
                            eltversion
                            
                        from {minio_bronze_path}
                        
                        GROUP BY
                            businessentityid, firstname,            lastname,
                            birthdate,        maritalstatus,        gender,
                            hiredate,         departmentid,         name,
                            groupname,        jobtitle,             payfrequency,
                            rate,             salarychange_date,    startdate,
                            enddate,          salariedflag,         currentflag,
                            extracted_date,    modified_date,        eltversion
                    """)
        
        logger.info(f"Dimension transformations has been sucessfully completed")
        
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
        
        
def writeBuyerDimension(dataframe:DataFrame, silver_layer:str, file_path:str, table_name:str) -> str:
    
    try:
        minio_path = f"{silver_layer}/{file_path}/{table_name}"

        (
            dataframe
                .write
                .mode("append")
                .format("delta")
                .save(f"{minio_path}")
        )
        
        return f"Table writing successfully completed to delta: {table_name}"
    
    except AnalysisException as e:
        logger.error(f"Analysis error while writing table {table_name} to delta: {e}")
        return "AnalysisException"
    
    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {table_name} to delta: {e}")
        return "Py4JJavaError"
    
    except NoCredentialsError as e:
        logger.error(f"Missing credentials for MinIO/S3: {e}")
        return "NoCredentialsError"
    
    except PartialCredentialsError as e:
        logger.error(f"Incomplete credentials for MinIO/S3: {e}")
        return "PartialCredentialsError"
    
    except Exception as e:
        logger.error(f"Unexpected error while writing table {table_name} to delta: {e}")
        return "UnexpectedError"
    
def sparkStop(spark) -> str:
    
    try:
        if spark:
            spark.stop()
            logger.info("SparkSession stoped successfully")
            
        return f"{spark} Spark Session Stoped"
            
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {e}")
        print_exc()
        
def main():
    
    application_name = "dimension_Warehouse_table"
    table_name = "raw_dimbuyer"
    dimension_table = "dim_buyer"
    folder_path = "advworks"
    
    try:
        
        spark, bronze_delta, silver_delta = load_source_configurations(application_name)
        
        getCreateSurrogateKey(spark, bronze_delta, folder_path, table_name)
        
        dataframe = transformBuyerDimension(spark, bronze_delta, table_name)
        
        writeBuyerDimension(dataframe, silver_layer= silver_delta, file_path= folder_path, table_name= dimension_table)
        logger.info(f"""dataframe has been writted successfully: {dataframe.printSchema()} 
                        on path: {silver_delta}/{folder_path}/{dimension_table}""")
        
        logger.info("All functions have been executed successfully ")
        
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