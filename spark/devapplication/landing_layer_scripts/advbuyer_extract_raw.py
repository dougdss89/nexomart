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
        return None, None, None, None
    
    try:
        # Obter as configurações necessárias
        spark_jars = config.get("spark_jars", {})
        mssql_sources = config.get("mssql_sources", {})
        mssql_adventureworks = mssql_sources.get("adventureworks", {})
        hadoop_paths = config.get("hadoop_paths", {})
        
        # Validar spark_jars
        validate_required_keys(
            config=spark_jars,
            required_keys=["mssql_spark_jars", "postgresql_jar"],
            config_name="spark_jars"
        )
        
        # Validar mssql_adventureworks
        validate_required_keys(
            config=mssql_adventureworks,
            required_keys=["url", "database", "properties"],
            config_name="mssql_adventureworks"
        )
        
        # Validar properties dentro de mssql_adventureworks
        validate_required_keys(
            config=mssql_adventureworks["properties"],
            required_keys=["user", "password", "driver"],
            config_name="mssql_properties"
        )
        
        # Validar hadoop_paths
        validate_required_keys(
            config=hadoop_paths,
            required_keys=["landing"],
            config_name="hadoop_paths"
        )
        
        # Criar SparkSession
        spark = (SparkSession.builder
                .appName('extract_buyer_raw')
                .config('spark.jars', spark_jars['mssql_spark_jars'])
                .config('spark.driver.extraClassPath', spark_jars['mssql_spark_jars'])
                .config('spark.executor.extraClassPath', spark_jars['mssql_spark_jars'])
                .enableHiveSupport()
                .getOrCreate())
        
        return (
            spark,
            mssql_adventureworks["properties"],
            mssql_adventureworks,
            hadoop_paths["landing"]
        )
    
    except RuntimeError as e:
        print(f"Failed to create Spark Session. Runtime error: {e}")
        print_exc()
        return None, None, None, None
    
    except Exception as e:
        print(f"Unexpected error occurred. Please verify: {e}")
        print_exc()
        exit(1)

def extract_raw_buyer(spark, mssql_adventureworks, mssql_properties):
    """Extract buyer data from source"""
    try:
        df_buyer_raw = (spark.read
                       .format('jdbc')
                       .option('url', mssql_adventureworks['url'])
                       .option('driver', mssql_properties['driver'])
                       .option('user', mssql_properties['user'])
                       .option('password', mssql_properties['password'])
                       .option('query', """
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
                               hre.currentflag,
                               getdate() as inserted_date,
                               '9999-31-12 00:00:00.000' as modified_date,
                               '1' as eltversion
                           from humanresources.employee as hre
                           left join humanresources.employeepayhistory as hpe
                               on hre.businessentityid = hpe.businessentityid
                           left join humanresources.employeedepartmenthistory as hrdh
                               on hre.businessentityid = hrdh.businessentityid
                           left join person.person as pp
                               on hre.businessentityid = pp.businessentityid
                           left join humanresources.Department as hrd
                               on hrd.departmentid = hrdh.departmentid
                           where hre.jobtitle = 'buyer' 
                               or hre.jobtitle like 'purchasing%'
                       """).load())
        return df_buyer_raw
    except Exception as e:
        print(f"Error extracting buyer data: {e}")
        print_exc()
        return None

def spark_stop(spark):
    """Stop Spark session safely"""
    try:
        if spark:
            spark.stop()
    except Exception as e:
        print(f'Error stopping Spark session: {e}')

if __name__ == '__main__':
    try:
        # Carregar conexões
        spark, mssql_properties, mssql_adventureworks, hadoop_landing = load_source_connections()
        
        if not all([spark, mssql_properties, mssql_adventureworks, hadoop_landing]):
            raise RuntimeError("Failed to load required connections")
            
        # Extrair dados
        df_buyer = extract_raw_buyer(spark, mssql_adventureworks, mssql_properties)
        
        if df_buyer is None:
            raise RuntimeError("Failed to extract buyer data")
            
        # Escrever dados
        df_buyer.repartition(5).write.mode('overwrite').parquet(f"{hadoop_landing}/buyer")
        
        print("Job completed successfully")
        
    except Exception as e:
        print(f"Job failed: {e}")
        print_exc()
        exit(1)
        
    finally:
        spark_stop(spark)