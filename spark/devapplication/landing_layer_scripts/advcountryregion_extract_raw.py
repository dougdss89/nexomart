from pyspark.sql import SparkSession
from modules.configconnections_loader import load_config, validate_required_keys
import sys
import traceback

def load_source_connections():
    
    """ load source connections from .env file"""
    
    try:
        print('Trying to load configuration')
        config = load_config()
        print(f"Configuration loaded: {config}")
        
    except FileNotFoundError as e:
        print(f'File .env not founded: {e}')
        traceback.print_exc()
        
        return None, None, None
    
    except KeyError as e:
        print(f"Missing some key configs: {e}")
        traceback.print_exc()
        
        return None, None, None
    
    except Exception as e:
        print(f"unexpected error while load configuration: {e}")
        traceback.print_exc()
        
        return None, None, None
    
    #load properties, configurations and jars file
    try:
        spark_jars = config["spark_jars"]
        mssql_adventureworks = config["mssql_sources"]["adventureworks"]
        mssql_properties = mssql_adventureworks["properties"]
        hadoop_landing = config.get("hadoop_paths", {}).get("landing", '')
    
        #check if variable was loaded
        if not spark_jars or 'mssql_spark_jars' not in spark_jars:
            print("Missing spark jars configuration file and path")
            return None, None, None
        
        if not mssql_adventureworks:
            print(f"Adventureworks sources and properties wasn't loaded. Missing properties {mssql_adventureworks}")
            return None, None, None
        
        if not mssql_properties:
            print(f"We detected certain dependencies on {mssql_properties} that was not loaded. Reload {mssql_adventureworks} and retry it")
            return None, None, None
        

    #create spark session
        spark = (SparkSession
                    .builder
                    .appName('extract_country_region_raw')
                        .config('spark.jars', f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config('spark.driver.extraClassPath', f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config('spark.executor.extraClassPath', f"{spark_jars.get('mssql_spark_jars', '')}")
                    .enableHiveSupport()
                    .getOrCreate()
                )
        return spark, mssql_properties, mssql_adventureworks, hadoop_landing

    except Exception as e:
        print(f"Fail to create SparkSession {spark}")
        traceback.print_exc()
        return None, None, None
    
spark, mssql_properties, mssql_adventureworks, hadoop_landing = load_source_connections()

def extract_raw_countryregion():
    
    df_countryregion = (spark.read
                            .format('jdbc')
                            .option('url', mssql_adventureworks['url'])
                            .option('driver', mssql_properties['driver'])
                            .option('user', mssql_properties['user'])
                            .option('password', mssql_properties['password'])
                            .option('query', 
                                    
                                    """
                                        select distinct

                                            sst.territoryid,
                                            sst.[name],
                                            sst.[group],
                                            sst.countryregioncode,
                                            getdate() as inserted_date,
											'9999-31-12 00:00:00.000' as modified_date,
											'1' as eltversion

                                        from sales.salesterritory as sst

                                        inner join
                                            person.countryregion as pcr

                                        on pcr.countryregioncode = sst.countryregioncode

                                        left join
                                            person.stateprovince as psp

                                        on psp.countryregioncode = pcr.countryregioncode
                                    """
                                    )).load()
    
    return df_countryregion

write_country_region = extract_raw_countryregion()

write_country_region.repartition(1).write.mode('overwrite').parquet(f"{hadoop_landing}/countryregion")

def spark_stop(spark):
    
    if spark:
        spark.stop()
            
    
if __name__ == '__main__':
    
    load_source_connections()
    extract_raw_countryregion()
    spark_stop(spark)