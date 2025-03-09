import logging
from configconnections_loader import load_config, validate_required_keys
from pyspark.sql import functions as F
from startSparkSession import SparkSessionManager
from sys import exit
from traceback import print_exc

#log config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_source_connections():
    """ Load source connections from .env file and create a SparkSession. """
    try:
        config = load_config()

        # Validating imported keys
        minio_landing = config.get("hadoop_paths", {})
        bronze_delta = config.get("delta_path", {})

        validate_required_keys(
            config=minio_landing,
            required_keys=["minio_landing"],
            config_name="hadoop_paths"
        )
        
        validate_required_keys(
            config=bronze_delta,
            required_keys=["bronze_layer"],
            config_name="delta_path"
        )
        
        #create spark session
        
        session_manager = SparkSessionManager(
            app_name="extract_customer_landing"
        ).create_session()
        
        spark = session_manager
        
        return spark, minio_landing.get("minio_landing", ""), bronze_delta.get("bronze_layer", "") #, minio_storage
    
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
        
def extract_landing_data(spark, minio_landing, raw_table):
    try:
        customer_df = (spark
                        .read
                        .parquet(f"{minio_landing}/{raw_table}")
                    )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return customer_df
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
def convert_customer_schema(dataframe):
    
    try:
        # Aplica as transformações utilizando select
        dataframe = dataframe.select(
            F.col("businessentityid").cast("int").alias("business_id"),
            F.col("customerid").cast("int").alias("customer_id"),
            F.col("firstname").cast("string").alias("firstname"),
            F.col("lastname").cast("string").alias("lastname"),
            F.col("persontype").cast("string").alias("persontype"),
            F.col("emailpromotion").cast("int").alias("emailpromotion"),
            F.col("typephone").cast("string").alias("typephone"),
            F.col("phonenumber").cast("string").alias("phonenumber"),
            F.col("postalcode").cast("string").alias("postalcode"),
            F.col("addresstype").cast("string").alias("addresstype"),
            F.col("emailaddress").cast("string").alias("emailaddress"),
            F.col("territoryid").cast("int").alias("territory_id"),
            F.col("CountryRegionName").cast("string").alias("countryname"),
            F.col("stateprovinceid").cast("int").alias("stateprovince_id"),
            F.col("countryregioncode").cast("string").alias("countryregioncode"),
            F.col("stateprovincecode").cast("string").alias("stateprovincecode"),
            F.col("statename").cast("string").alias("statename"),
            F.col("YearlyIncome").cast("string").alias("YearlyIncome"),
            F.col("city").cast("string").alias("city"),  # Corrigido o alias
            F.col("addressline1").cast("string").alias("addressline1"),
            F.col("addressline2").cast("string").alias("addressline2"),
            F.col("TotalPurchaseYTD").cast("float").alias("totalpurchase_ytd"),
            F.col("DateFirstPurchase").cast("date").alias("datefirstpurchase"),
            F.col("BirthDate").cast("date").alias("birthdate"),
            F.col("MaritalStatus").cast("string").alias("maritalstatus"),  # Corrigido para string
            F.col("Gender").cast("string").alias("gender"),
            F.col("TotalChildren").cast("int").alias("totalchildren"),
            F.col("NumberChildrenAtHome").cast("int").alias("numberchildrenathome"),  # Corrigido o alias
            F.col("Education").cast("string").alias("education"),
            F.col("Occupation").cast("string").alias("occupation"),
            F.col("HomeOwnerFlag").cast("string").alias("homeownerflag"),
            F.col("NumberCarsOwned").cast("int").alias("numbercarsowned"),
            F.col("inserted_date").cast("timestamp").alias("extracted_date"),
            F.col("modified_date").cast("timestamp").alias("modified_date"),
            F.col("eltversion").cast("int").alias("eltversion")
        ).withColumn(
                "minincome",
                F.coalesce(
                    F.trim(F.expr("substring(YearlyIncome, 1, instr(YearlyIncome, '-') - 1)")),
                    F.lit("0")
                ).cast("float")
            ).withColumn(
                "maxincome",
                F.coalesce(
                    F.trim(F.expr("substring(YearlyIncome, instr(YearlyIncome, '-') + 1, 20)")),
                    F.lit("0")
                ).cast("float")
            )
        logger.info(f"Data extracted sucessfully for table: {dataframe}")
        
        return dataframe
    
    except Exception as e:
        logger.error(f"Error to convert dataframe schema {dataframe}. Please verify error log: {e}")
        print_exc()
        exit(1)
        
# def write_customer_minio_storage(data, minio_storage, table):
    
#     try:
#         ((
#             data
#                 .write
#                 .mode("append")
#                 .parquet(f"{minio_storage}/{table}")
#         ))
        
#         return logger.info(f"Data successfully written to: {minio_storage}/{table}")
    

#     except Exception as e:
#         logger.info(f"Fail to persist data on {minio_storage}. Verify: {e}")
#         print_exc()
#         exit(1)
        
def write_to_delta(df, bronze_layer:str, file_path:str, table_name:str):
    """ Write DataFrame to delta table. """
    try:
        minio_path = f"{bronze_layer}/{file_path}/{table_name}"

        (
            df
            .write.format("delta")
            .mode("append")  # Use "append" to add data instead
            .save(f"{minio_path}")
        )
        return(f"Table {table_name} written successfully to Delta.")
        
    except Exception as e:
        print(f"Failed to write table {table_name} to Delta: {e}")
        print_exc()
        exit(1)
        
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
    
    spark, minio_landing, bronze_delta = load_source_connections()
    filepath = "advworks"
    raw_table = "raw_dimcustomer"
    
    customer_df = extract_landing_data(spark, minio_landing, "customer")
    
    dataframe  = convert_customer_schema(customer_df)
    
    # write_customer_minio_storage(customer, miniopath, "customer")
    
    write_to_delta(dataframe, bronze_delta, filepath,  raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")