from pyspark.sql import SparkSession
from modules.configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

def load_source_connections():
    
    """ load source connections from .env file """
    
    try:
        config = load_config()
        
    except(FileNotFoundError, KeyError) as e:
        print(f"Could not load connections from source: {e}")
        return None, None, None
    
    #validating imported keys
    try:
        spark_jars = config.get("spark_jars", {})
        mssql_adventureworks = config.get("mssql_sources", {}).get("adventureworks", {})
        mssql_properties = mssql_adventureworks.get("properties", {})
        hadoop_landing = config.get("hadoop_paths", {})


        validate_required_keys(config=spark_jars, 
                               required_keys=["mssql_spark_jars", "postgresql_jar"],
                               config_name="spark_jars")
        
        #validade properties key
        validate_required_keys(config= mssql_adventureworks,
                               required_keys=["properties"],
                               config_name="mssql_adventureworks")
        
        validate_required_keys(config=hadoop_landing,
                               required_keys=["landing"],
                               config_name="hadoop_paths")
    
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
        
        return spark, mssql_properties, mssql_adventureworks, hadoop_landing
    
    
    except RuntimeError as e:
        print(f"Fail to create Spark Session. Run time error: {e}")
        print_exc()
        return None
    
    except Exception as e:
        print(f"Unexpected error ocurred. Please verify: {e}")
        print_exc()
        exit(1)
        return None, None, None
        
spark, mssql_properties, mssql_adventureworks, hadooop_landing = load_source_connections()

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

write_buyer_dataframe.repartition(5).write.mode('overwrite').parquet(f"hdfs://ha-cluster/landing/buyer")

def spark_stop(spark):
    
    try:
        if spark:
            spark.stop()
            
    except Exception:
        print('Spark Session not founded')
        
if __name__ == '__main__':
    load_source_connections()
    extract_raw_buyer()
    spark_stop(spark)