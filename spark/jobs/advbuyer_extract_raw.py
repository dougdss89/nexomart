from pyspark.sql import SparkSession
from configconnections_loader import load_config
import sys
import traceback

def load_source_connections():
    
    """ load source connections from .env file """
    
    try:
        config = load_config()
        
    except(FileNotFoundError, KeyError) as e:
        return(f"Could not load connections from source: {e}")
        # return None, None, None
    
    try:
        spark_jars = config["spark_jars"]
        mssql_adventureworks = config["mssql_sources"]["adventureworks"]
        mssql_properties = mssql_adventureworks["properties"]
        mssql_url = mssql_adventureworks["url"]
        
    except KeyError as e:
        return(f'Missing required configuration: {e}')
        # return None, None, None
    
    #creating sparkSession
    
    spark = (SparkSession
                .builder
                .appName('buyer_extract_raw')
                    .config('spark.jars', config['spark_jars']['mssql_spark_jars'] + "," + config['spark_jars']['postgresql_jar'])
                    .config('spark.driver.extraClassPath', config['spark_jars']['mssql_spark_jars'] + "," + config['spark_jars']['postgresql_jar'])
                    .config('spark.executor.extraClassPath', config['spark_jars']['mssql_spark_jars'] + "," + config['spark_jars']['postgresql_jar'])
                .enableHiveSupport()
                .getOrCreate()
            )
    
    return spark, mssql_properties, mssql_adventureworks
        
spark, mssql_properties, mssql_adventureworks = load_source_connections()

def extract_raw_buyer():
    
    df_buyer_raw = (spark
                        .read
                        .format('jdbc')
                        .option('url', mssql_adventureworks['url'])
                        .option('driver', mssql_properties['driver'])
                        .option('user', mssql_properties['user'])
                        .option('password', mssql_properties['password'])
                        .option('query', 
                                
                                """ 
                                select distinct
                                    hre.businessentityid,
                                    pp.firstname,
                                    pp.lastname,
                                    hre.loginid,
                                    hre.birthdate,
                                    hre.maritalstatus,
                                    hre.gender,
                                    hre.hiredate,
                                    hre.jobtitle,
                                    hrd.departmentid,
                                    hrd.[name],
                                    hrd.groupname,
                                    hpe.payfrequency,
                                    hpe.rate,
                                    hpe.ratechangedate,
                                    hrdh.startdate,
                                    hrdh.enddate,
                                    hre.salariedflag,
                                    hre.currentflag
                                        
                                from  humanresources.employee as hre
                                    left join
                                    humanresources.employeepayhistory as hpe
                                on hre.businessentityid = hpe.businessentityid
                                    left join
                                    humanresources.employeedepartmenthistory as hrdh
                                on hre.businessentityid = hrdh.businessentityid
                                    left join
                                    person.person as pp
                                on hre.businessentityid = pp.businessentityid
                                    left join
                                    humanresources.Department as hrd
                                on hrd.departmentid = hrdh.departmentid
                                where hre.jobtitle = 'buyer' or hre.jobtitle like 'purchasing%'

                                """
                            )).load()
    return df_buyer_raw

write_buyer_dataframe = extract_raw_buyer()

write_buyer_dataframe.repartition(5).write.mode('overwrite').parquet("hdfs://ha-cluster/raw/buyer/")

def spark_stop(spark):
    
    try:
        if spark:
            spark.stop()
            
    except Exception:
        print('Spark Session not founded')
        
if __name__ == '__main__':
    load_source_connections()
    extract_raw_buyer()
    spark_stop()