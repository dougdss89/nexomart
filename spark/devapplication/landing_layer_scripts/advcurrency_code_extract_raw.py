from pyspark.sql import SparkSession
from configconnections_loader import load_config, validate_required_keys
import sys
import traceback

def load_source_connections():
    
    """ load .env file"""
    
    try:
        print("trying load configurations")
        config = load_config()
        
        print(f"Configuration loaded")
        
    except FileNotFoundError as e:
        print(f"File .env not founded: {e}. Error to load configuration variable")
        traceback.print_exc()
        return None
    
    except KeyError as e:
        print(f"Missing some key-value pair: {e}")
        traceback.print_exc()
        return None
    
        #load properties, configurations and jars file
    try:
        spark_jars = config["spark_jars"]
        mssql_adventureworks = config["mssql_sources"]["adventureworks"]
        mssql_properties = mssql_adventureworks["properties"]
        minio_storage = config.get("hadoop_paths", {}).get("minio_landing", "")
    
        #check if variable was loaded
        if not spark_jars or 'mssql_spark_jars' not in spark_jars or 'postgresql_jar' not in spark_jars:
            print("Missing spark jars configuration file and path")
            return None, None, None
        
        if not mssql_adventureworks:
            print(f"Adventureworks sources and properties wasn't loaded. Missing properties {mssql_adventureworks}")
            return None, None, None
        
        if not mssql_properties:
            print(f"We detected certain dependencies on {mssql_properties} that was not loaded. Reload {mssql_adventureworks} and retry it")
            return None, None, None
        
    #load properties, spark configuration and session
    
        spark = (SparkSession
                    .builder
                    .appName('extract_currency_code_raw')
                    .config('spark.jars', f"{spark_jars.get('mssql_spark_jars', '')},{spark_jars.get('postgresql_jar', '')}")
                    .config('spark.driver.extraClassPath', f"{spark_jars.get('mssql_spark_jars', '')},{spark_jars.get('postgresql_jar', '')}")
                    .config('spark.executor.extraClassPath', f"{spark_jars.get('mssql_spark_jars', '')},{spark_jars.get('postgresql_jar', '')}")
                    .enableHiveSupport()
                    .getOrCreate()
                )
        return spark, mssql_properties, mssql_adventureworks, minio_storage
    
    except Exception as e:
        print(f'Fail to create SparkSession: {e}')
        traceback.print_exc()
        return None
    
def extract_currencycode_raw(spark, mssql_properties, mssql_adventureworks):
    
    if not all ([spark, mssql_properties, mssql_adventureworks]):
        print("Missing some connection and session variables. Invalid Parameters")
        
        return None
    
    try:
        df_currency_code_raw = (spark
                                    .read
                                    .format('jdbc')
                                    .option('url', mssql_adventureworks['url'])
                                    .option('driver', mssql_properties['driver'])
                                    .option('user', mssql_properties['user'])
                                    .option('password', mssql_properties['password'])
                                    .option('query', 
                                            
                                            """ 
                                                select 
                                                    scr.currencycode,
                                                    scr.[name] as currencyname,
                                                    crc.CountryRegionCode,
                                                    pcr.[name] as countryname,
                                                    getdate() as inserted_date,
                                                    '9999-31-12 00:00:00.000' as modified_date,
                                                    '1' as eltversion
                                                from sales.currency as scr
                                                    inner join
                                                    sales.countryregioncurrency as crc
                                                on scr.CurrencyCode = crc.CurrencyCode
                                                    inner join
                                                person.countryregion as pcr
                                                on crc.CountryRegionCode = pcr.CountryRegionCode
                                            
                                            """
                                )).load()
        
        return df_currency_code_raw
    
    except Exception as e:
        print("Failed to load datasource to dataframe")
        traceback.print_exc()
        
        return None
    
def stop_spark(spark):
    if spark:
        spark.stop()
        
def main():
    
    #load source connections to main
    try:
        spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()
        
    #verifying spark
        if not spark:
            print("Fail to stablishes Spark Session")
            sys.exit(1)
            
    #extract and check if datasources was loaded to dataframe
    
        write_currencycode_rate = extract_currencycode_raw(spark, mssql_properties, mssql_adventureworks)
        
        if write_currencycode_rate:
            write_currencycode_rate.repartition(3).write.mode('overwrite').parquet(f"{minio_landing}/currencycode")
            
            print("Dataframe loaded successfully")
            
        print(f"Data frame was loaded successfully on Datalake {minio_landing}")
        
    except Exception as e:
        print("Main script was not properly configurated. Verify")
        traceback.print_exc()
        
        return None
    
    finally:
        #closing spark session
        stop_spark(spark)
        print("Spark Session closed")
        
if __name__ == '__main__':
    main() #executing script 