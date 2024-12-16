from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from sys import exit
from traceback import print_exc
from spark.jobs.configconnections_loader import load_config, validate_required_keys


def load_source_connections():
    
    print("Import .env file and creating Spark Session")
    
    #loading configurations and validated it
    try:
        config = load_config()
        print("File .env loaded")
        
        
    except FileExistsError as e:
        print("File .env not founded: {e}")
        
    except KeyError as e:
        print("Missing some key-values pair: {e}")
        
    #importing key-values from .env
    #validating data imported
    try:
        spark_jars = config.get('spark_jars', {})
        mssql_adventureworks = config.get("mssql_sources", {}).get("adventureworks", {})
        mssql_properties = mssql_adventureworks.get("properties", {})
        
        validate_required_keys(config= spark_jars,
                               required_keys=['mssql_spark_jars', 'postgresql_jar'],
                               config_name='spark_jars')
        
        validate_required_keys(config= mssql_adventureworks,
                               required_keys= ['properties'],
                               config_name= 'mssql_adventureworks')
        
        #creating spark session
        
        spark = (SparkSession
                    .builder
                        .appName('extract_salesperson_raw')
                        .config('spark.jars', f"{spark_jars.get('mssql_spark_jars')}, {spark_jars.get('postgresql_jar')}")
                        .config('spark.executor.extraClassPath', f"{spark_jars.get('mssql_spark_jars')}, {spark_jars.get('postgresql_jar')}")
                        .config('spark.driver.extraClassPath', f"{spark_jars.get('mssql_spark_jars')}, {spark_jars.get('postgresql_jar')}")
                    .enableHiveSupport()
                    .getOrCreate()
                    )
        
        return spark, mssql_adventureworks, mssql_properties
    
    except RuntimeError as e:
        print(f"Failed to create configuration: {e}")
        print_exc()
        
        return None
    
    except Exception as e:
        print(f"Unexpected error occur: {e}")
        print_exc()
        return None, None, None
    
    
def extract_salesperson_raw(spark, mssql_adventureworks, mssql_properties):
    
    if not all([spark, mssql_adventureworks, mssql_properties]):
        print("Missing connection configuration or Spark Session")
        exit(1)
        
    try:
        df_salesperson_raw = (spark
                                .read
                                .format('jdbc')
                                .option('url', mssql_adventureworks['url'])
                                .option('driver', mssql_properties['driver'])
                                .option('user', mssql_properties['user'])
                                .option('password', mssql_properties['password'])
                                .option('query', 

                                            """

                                            select

                                                pp.businessentityid,            hre.loginid,
                                                pp.firstname,                   pp.lastname,
                                                hre.gender,                     hre.birthdate,
                                                hre.hiredate,                   hre.jobtitle,
                                                hre.maritalstatus,              hre.organizationlevel,
                                                hreh.departmentid,              hreh.startdate,
                                                hreh.enddate,                   hrd.groupname,
                                                hrd.[name] as departmentname,   hreph.payfrequency,
                                                hreph.rate,                     hreph.ratechangedate,
                                                ssp.commissionpct,              ssp.salesquota,
                                                ssp.bonus,                      sst.[name] as regionname,
                                                sst.[group] as continentname,   sst.countryregioncode,
                                                sst.territoryid,                salariedflag,
                                                currentflag

                                            from person.person as pp
                                            left join
                                                humanresources.employee as hre
                                            on pp.businessentityid = hre.businessentityid
                                            left join
                                                humanresources.employeedepartmenthistory as hreh
                                            on hre.businessentityid = hreh.businessentityid
                                            left join
                                                humanresources.department as hrd
                                            on hreh.departmentid = hrd.departmentid
                                            left join
                                                humanresources.employeepayhistory as hreph
                                            on hreph.businessentityid = hre.businessentityid
                                            inner join
                                                sales.salesperson as ssp
                                            on hre.businessentityid = ssp.businessentityid
                                            left join
                                                sales.salesterritory as sst
                                            on sst.territoryid = ssp.territoryid

                                            """        

                            ).load())
    
        return df_salesperson_raw

    except Exception as e:
        print(f"""Error to extract and create  dataframe: 
              {df_salesperson_raw}.
              {e}""")
        print_exc()
        return None, None, None
    

#finalize spark session function

def spark_stop(spark):
    
    if spark:
        spark.stop()
        
    else:
        print("Spark Session not initialized")
        
def main():
    
    spark = None
    
    try:
        spark, mssql_adventureworks, mssql_properties = load_source_connections()
        
        if not spark:
            print("Missing Spark Session")
            print_exc()
            exit(1)
            
        salesperson_df = extract_salesperson_raw(spark, mssql_adventureworks, mssql_properties)
        
        if salesperson_df:
            salesperson_df.write.parquet('hdfs://ha-cluster/raw/salesperson', mode='overwrite')
            
            print(f""" Data frame created successfully.
                  
                  {salesperson_df}.""")
    
    except Exception as e:
        print(f"Main script encontered some error value: {e}")
        print_exc()
        
        return None, None, None
    
    #finalize spark session
    finally:
        print("Closing spark session... ")
        spark_stop(spark)
        
        
if __name__ == '__main__':
    main()