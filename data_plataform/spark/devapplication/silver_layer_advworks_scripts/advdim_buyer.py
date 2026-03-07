from startSparkSession import SparkSessionManager
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
from configconnections_loader import validate_required_keys, load_config
from sys import exit
from traceback import print_exc
import logging

from py4j.protocol import Py4JJavaError #java exception error

#create logging configurations
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

#import .env file
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
        
# starting dimension transformation

def getCreateSurrogateKey(spark, catalog:str, iceberg:str, table_name:str) -> DataFrame:
    
    try:
        
        raw_lakehouse = f"{catalog}.{iceberg}.{table_name}"
        
        dataframe = spark.read.format("iceberg").load(raw_lakehouse)
                
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
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)
        
def transformBuyerDimension(dataframe: DataFrame, spark) -> DataFrame:
    
    try:
        
        dataframe.createOrReplaceTempView("buyer_view")
        
        dataframe = spark.sql(""" 
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
                            
                        from buyer_view
                        
                        GROUP BY
                            businessentityid, firstname,            lastname,
                            birthdate,        maritalstatus,        gender,
                            hiredate,         departmentid,         name,
                            groupname,        jobtitle,             payfrequency,
                            rate,             salarychange_date,    startdate,
                            enddate,          salariedflag,         currentflag,
                            extracted_date,    modified_date,        eltversion
                    """)  # noqa: F541
        
        logger.info(f"Dimension transformations has been sucessfully completed")  # noqa: F541
        
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
        
        
def writeBuyerDimension(spark, dataframe:DataFrame, catalog:str, silver_icebergdb:str, table_name:str) -> DataFrame:
    
    try:
        lakehouse = f"{catalog}.{silver_icebergdb}.{table_name}"
        
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{silver_icebergdb}")
        
        (
            dataframe
                .write
                .mode("append")
                .format("iceberg")
                .saveAsTable(f"{lakehouse}")
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
            logger.info("SparkSession stoped successfully")
            
        return f"{spark} Spark Session Stoped"
            
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {str(e)}")
        print_exc()
        
def main():
    
    application_name = "dimension_buyer_table"
    table_name = "raw_dimbuyer"
    dimension_table = "dim_buyer"
    
    
    try:
        
        spark, iceberg, iceberg_db, iceberg_catalog = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, iceberg_catalog, iceberg, table_name)
        
        dataframe = transformBuyerDimension(dataframe, spark)
        
        writeBuyerDimension(spark=spark, dataframe=dataframe, catalog=iceberg_catalog, silver_icebergdb= iceberg_db, table_name= dimension_table)
        
        logger.info(f"""dataframe has been writted successfully: {dataframe.schema} 
                        on path: {iceberg_db}.{dimension_table}""")
        
        logger.info("All functions have been executed successfully ")
        
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