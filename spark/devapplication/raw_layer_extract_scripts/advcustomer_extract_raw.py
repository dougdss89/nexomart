import logging
from configconnections_loader import load_config, validate_required_keys
from startSparkSession import SparkSessionManager
from sys import exit
from traceback import print_exc

#log config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_source_connections():
    
    """ 
        Load key-values from .env file to create Spark Session
    """
    
    try:
        config = load_config()
        
        hadoop_landing = config.get("hadoop_paths", {}).get("landing", "")
        iceberg_rawdb = config.get("hadoop_paths", {}).get("raw_icebergdb", "")
        minio_storage = config.get("hadoop_paths", {}).get("minio_landing", "")
        
        #validated imported keys
        
        validate_required_keys(
            config = config.get("hadoop_paths", {}),
            required_keys= ["landing", "raw_icebergdb", "minio_landing"],
            config_name="hadoop_paths"
        )
        
        #create spark session
        
        session_manager = SparkSessionManager(
            app_name="extract_customer_landing"
        ).create_session()
        
        spark = session_manager
        
        return spark, hadoop_landing, iceberg_rawdb, minio_storage
    
    except (FileNotFoundError, KeyError) as e:
        logger.error(f"Could not load connections from source: {e}")
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
        
def extract_landing_data(spark, hadoop_landing, raw_table):
    try:
        customer_df = (spark
                        .read
                        .parquet(f"{hadoop_landing}/{raw_table}")
                    )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return customer_df
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
def convert_customer_schema(dataframe):
    
    try:
        dataframe.selectExpr(
                            "cast(businessentityid as int) as businessentity_id",
                            "cast(customerid as int) as customer_id",
                            "cast(firstname as string) as firstname",
                            "cast(lastname as string) as lastname",
                            "cast(persontype as string) as persontype",
                            "cast(emailpromotion as string) as emailpromotion",
                            "cast(typephone as string) as typephone",
                            "cast(phonenumber as string) as phonenumber",
                            "cast(postalcode as string) as postalcode",
                            "cast(addresstype as string) as addresstype",
                            "cast(emailaddress as string) as emailaddress",
                            "cast(territoryid as int) as territory_id",
                            "cast(CountryRegionName as string) as countryname",
                            "cast(stateprovinceid as int) as stateprovince_id",
                            "cast(countryregioncode as string) as countryregioncode",
                            "cast(stateprovincecode as string) as stateprovincecode",
                            "cast(statename as string) as statename",
                            "cast(city as string) as statename",
                            "cast(addressline1 as string) as addressline1",
                            "cast(addressline2 as string) as addressline2",
                            "cast(totalpurchaseytd as float) as totalpurchase_ytd",
                            "cast(datefirstpurchase as date) as datefirstpurchase",
                            "cast(birthdate as date) as birthdate",
                            "cast(maritalstatus as date) as maritalstatus",
                            "cast(yearlyincome as string) as yearlyincome",
                            "cast(gender as string) as gender",
                            "cast(totalchildren as int) as totalchildren",
                            "cast(numberchildrenathome as int) as totalchildren",
                            "cast(education as string) as education"            ,
                            "cast(occupation as string) as occupation",
                            "cast(homeownerflag as string) as homeownerflag",
                            "cast(numbercarsowned as string) as numbercarsowned",
                            "cast(inserted_date as timestamp) as extracted_date",
                            "cast(modified_date as timestamp) as modified_date",
                            "cast(eltversion as int) as eltversion"
                            
        )
        
        logger.info(f"Data extracted sucessfully for table: {dataframe}")
        
        return dataframe
    
    except Exception as e:
        logger.error(f"Error to convert dataframe schema {dataframe}. Please verify error log: {e}")
        print_exc()
        exit(1)
        
def write_customer_minio_storage(data, minio_storage, table):
    
    try:
        ((
            data
                .write
                .mode("append")
                .parquet(f"{minio_storage}/{table}")
        ))
        
        return logger.info(f"Data successfully written to: {minio_storage}/{table}")
    

    except Exception as e:
        logger.info(f"Fail to persist data on {minio_storage}. Verify: {e}")
        print_exc()
        exit(1)
        
def write_to_iceberg(customer_df, iceberg_rawdb, table_name):
    
    try:
        (
            customer_df
                .write
                .format("Iceberg")
                .mode("overwrite")
                .saveAsTable(f"{iceberg_rawdb}.{table_name}")
        )
        logger.info(f"Table {table_name} written successfully to Iceberg")
        
    except Exception as e:
        logger.error(f"Failed to write table {table_name} to Iceberg table: {e}")
        print_exc()
        exit(1)
        
def spark_stop(spark):
    try:
        if spark:
            spark.stop()
            logger.info("SparkSession stoped successfully")
            
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {e}")
        print_exc()
        
        
def main():
    
    spark, hadoop_landing, iceberg_rawdb, miniopath = load_source_connections()
    
    customer_df = extract_landing_data(spark, hadoop_landing, "customer")
    
    customer  = convert_customer_schema(customer_df)
    
    write_customer_minio_storage(customer, miniopath, "customer")
    
    write_to_iceberg(customer, f"{iceberg_rawdb}", "raw_dimcustomer")
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")