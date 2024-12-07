from pyspark.sql import SparkSession
from configconnections_loader import load_config
import sys
import traceback

def load_source_connections():
    
    """ load .env file"""
    
    try:
        print("trying to load configurations")
        config = load_config()
        
        print("Configuration loaded")
        
    except FileNotFoundError as e:
        print("File .env was not founded on path")
        traceback.print_exc()
        
        return None
    
    except KeyError as e:
        print("Missing some keys-value pairs: {e}")
        traceback.print_exc()
        
        return None
    
    try:
        spark_jars = config['spark_jars']
        mssql_adventureworks = config['mssql_sources']['adventureworks']
        mssql_properties = mssql_adventureworks['properties']
        
        #check if variables was loaded
        
        if not spark_jars or 'mssql_spark_jars' not in spark_jars or 'postgresql_jar' in spark_jars:
            print(f"Missing jar file: {spark_jars}")
            
            return None
        
        if not mssql_adventureworks:
            print("Missing Adventure Works sources and properties")
            return None
        
        if not mssql_properties:
            print(f"We detected certain dependencies on {mssql_properties} that was not loaded. Reload {mssql_adventureworks} and retry it")
            return None
        
        #creating spark_session
        spark = (SparkSession
                    .builder
                    .appName('currency_code_extract_raw')
                        .config('spark.jars', f"{spark_jars.get('mssql_spark_jars', '')},{spark_jars.get('postregsql_jar')}")
                        .config('spark.driver.extraClassPath', f"{spark_jars.get('mssql_spark_jars', '')},{spark_jars.get('postregsql_jar')}")
                        .config('spark.executor.extraClassPath', f"{spark_jars.get('mssql_spark_jars', '')},{spark_jars.get('postregsql_jar')}")
                    .enableHiveSupport()
                    .getOrCreate()
                    )
        
        return spark, mssql_properties, mssql_adventureworks
    
    except Exception as e:
        print("Fail to create SparkSession")
        traceback.print_exc()
        
        return None
    
def extract_currencycode_raw(spark, mssql_properties, mssql_adventureworks):
    
    if not all ([spark, mssql_properties, mssql_adventureworks]):
        print("Missing some connection and session variables. Invalid Parameters")
        
        return None
    
    try:
        df_currency_code_raw = (spark
                                    .read
                                    .fomat('jdbc')
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
                                                    pcr.[name] as countryname
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
        spark, mssql_properties, mssql_adventureworks = load_source_connections()
        
    #verifying spark
        if not spark:
            print("Fail to stablishes Spark Session")
            sys.exit()
            
    #extract and check if datasources was loaded to dataframe
    
        write_currency_rate = extract_currencycode_raw(spark, mssql_properties, mssql_adventureworks)
        
        if write_currency_rate:
            write_currency_rate.repartition(3).write.mode('overwrite').parquet('hdfs://ha-cluster/raw/currencycode')
            
            print("Dataframe loaded successfully")
            
        print("Error to load Dataframe on Data lake. Fail to extract data")
        
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