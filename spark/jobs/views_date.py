from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os
from dotenv import load_dotenv

env = load_dotenv(dotenv_path='/mnt/d/linux/project/vars/.env')

#env vars for postgresql connection
mssqlsource = os.getenv('MSSQLSOURCE')
mssqluser = os.getenv('MSSQL_SPARK_USER')
mssqlpwd = os.getenv('MSSQL_SPARK_PWD')
mssqldriver = os.getenv('MSSQL_DRIVER')
sqlserverjar = os.getenv('SQLSERVER_JAR')
mssql_dwdest = os.getenv('MSSQL_CONN')

#create session
def createsession():
    
    spark = (( SparkSession
            .builder
            .appName('payments')
            .config('spark.jars', f'{sqlserverjar}' )
            .config('spark.executor.extraClassPath', f'{sqlserverjar}')
            .config('spark.driver.extraClassPath', f'{sqlserverjar}')
            .getOrCreate()
        ))
    
    return spark

spark = createsession()

def create_duedate_view(spark = createsession(), source = mssql_dwdest, user = mssqluser, pwd = mssqlpwd, driver = mssqldriver):
    
    temp_view = (( 
                  
                    spark
                        .read
                        .format('jdbc')
                        .option('url', source)
                        .option('driver', driver)
                        .option('user', user)
                        .option('password', pwd)
                        .option('query', 
                                
                                """ 
                                
                                    select * from dimension.dimdate
                                
                                """
                                
                                )).load)
    
    return temp_view


def createview(data = create_duedate_view(), conn_dw = mssql_dwdest, user = mssqluser, pwd = mssqlpwd, driver = mssqldriver):
    (( 
        data
            .write
            .format('jdbc')
            .option('url', conn_dw)
            .option('driver', driver)
            .option('user', user)
            .option('password', pwd)
            .option('query', 
                    
                    """ 
                    create view dimension.duedate as 
                    
                        select * from dimension.dimdate
                    
                    """
                    
                    )
    ))
    
    if __name__ == '__main__':
        
        createsession()
        create_duedate_view()
        createview()
        spark.stop()