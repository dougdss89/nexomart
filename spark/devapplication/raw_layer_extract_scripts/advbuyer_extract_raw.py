from pyspark.sql import SparkSession
from configconnections_loader import load_config, validate_required_keys
from pyspark.sql.functions import col
from sys import exit
from traceback import print_exc

def load_source_connections():
    """ Load source connections from .env file and create a SparkSession. """
    try:
        config = load_config()

        # Validating imported keys
        hadoop_landing = config.get("hadoop_paths", {}).get("landing")
        iceberg_rawdb = config.get("hadoop_paths", {}).get("raw_icebergdb")

        validate_required_keys(
            config=config.get("hadoop_paths", {}),
            required_keys=["landing", "raw_icebergdb"],
            config_name="hadoop_paths"
        )

        # Creating SparkSession
        spark = (
            SparkSession.builder
            .appName('extract_buyer_landing')
            .enableHiveSupport()
            .getOrCreate()
        )

        return spark, hadoop_landing, iceberg_rawdb

    except (FileNotFoundError, KeyError) as e:
        print(f"Could not load connections from source: {e}")
        print_exc()
        exit(1)

    except RuntimeError as e:
        print(f"Failed to create SparkSession. Runtime error: {e}")
        print_exc()
        exit(1)

    except Exception as e:
        print(f"Unexpected error occurred. Please verify: {e}")
        print_exc()
        exit(1)
        

def extract_landing_buyer(spark, hadoop_landing, raw_table_mame):
    """ Extract buyer data from the landing zone. """
    try:
        df_buyer_raw = spark.read.parquet(f"{hadoop_landing}{raw_table_mame}")
        
        return df_buyer_raw
    
    except Exception as e:
        print(f"Failed to extract buyer data: {e}")
        print_exc()
        exit(1)

def buyer_replace_schema(dataframe):
    
# Aplicar as transformações de tipo nas colunas
    buyer_dataframe = dataframe.select(
                            col("businessentityid").cast("int").alias("businessentityid"),
                            col("firstname").cast("string").alias("firstname"),
                            col("lastname").cast("string").alias("lastname"),
                            col("loginid").cast("string").alias("loginid"),
                            col("birthdate").cast("date").alias("birthdate"),
                            col("maritalstatus").cast("string").alias("maritalstatus"),
                            col("gender").cast("string").alias("gender"),
                            col("hiredate").cast("date").alias("hiredate"),
                            col("jobtitle").cast("string").alias("jobtitle"),
                            col("departmentid").cast("int").alias("departmentid"),
                            col("name").cast("string").alias("name"),
                            col("groupname").cast("string").alias("groupname"),
                            col("payfrequency").cast("int").alias("payfrequency"),
                            col("rate").cast("decimal(10,2)").alias("rate"),
                            col("ratechangedate").cast("date").alias("salarychange_date"),
                            col("startdate").cast("date").alias("startdate"),
                            col("enddate").cast("date").alias("enddate"),
                            col("salariedflag").cast("int").alias("salariedflag"),
                            col("currentflag").cast("int").alias("currentflag")
                        )
    return buyer_dataframe

    
def write_to_iceberg(df, iceberg_rawdb, table_name):
    """ Write DataFrame to Iceberg table. """
    try:
        (
            df.repartition(5)
            .write.format("iceberg")
            .mode("overwrite")  # Use "append" to add data instead
            .saveAsTable(f"iceberg_table.{iceberg_rawdb}.{table_name}")
        )
        print(f"Table {table_name} written successfully to Iceberg.")
        
    except Exception as e:
        print(f"Failed to write table {table_name} to Iceberg: {e}")
        print_exc()
        exit(1)

def spark_stop(spark):
    """ Stop the SparkSession. """
    try:
        if spark:
            spark.stop()
            print("SparkSession stopped successfully.")
            
    except Exception as e:
        print(f"Failed to stop SparkSession: {e}")
        print_exc()

def main():
    spark, hadoop_landing, iceberg_rawdb = load_source_connections()

    # Extract data from landing zone
    buyer_df = extract_landing_buyer(spark, hadoop_landing, "buyer")
    
    buyer_dataframe = buyer_replace_schema(buyer_df)

    # Write data to Iceberg
    write_to_iceberg(buyer_dataframe, iceberg_rawdb, "raw_dimbuyer")

    # Stop SparkSession
    spark_stop(spark)

if __name__ == "__main__":
    main()
