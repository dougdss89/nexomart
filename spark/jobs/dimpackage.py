#importing packages and env-var

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
#creating sparksession 

spark = ((
        SparkSession
            .builder
            .appName('dimpackage')
            .config('spark.jars', f'{sqlserverjar}')
            .config('spark.driver.extraClassPath', f'{sqlserverjar}')
            .config('spark.executor.extraClassPath', f'{sqlserverjar}')
        .getOrCreate()
))

#loading data from table into a dataframe
def createpackage(spark = spark, pgconn = mssqlsource, pguser = mssqluser, pgpwd = mssqlpwd, pgdriver = mssqldriver):
    
    """loading and create packagetype dimension """
    dimpackage_frame = ((spark
                        .read
                        .format('jdbc')
                        .option('url', pgconn)
                        .option('user', pguser)
                        .option('password', pgpwd)
                        .option('driver', pgdriver)
                        .option('query',
                                """
                                    select

                                        packagetypeid,
                                        packagetypename
                                        
                                    from warehouse.packagetypes
                                
                                """)
                    )).load()
    
    return dimpackage_frame

packagetype = createpackage()

#rename and reshape data type
def renamepackage(data = packagetype):
    packagetype_dim = ((
        
                    data
                        .withColumnRenamed('packagetypeid', 'packageid')
                        .withColumnRenamed('packagetypename', 'packagetype')


    ))
    
    return packagetype_dim

#load transformed data into a database table

def writepackage(data = renamepackage(), pgconn_dw = mssql_dwdest, pguser = mssqluser, pgpwd = mssqlpwd, pgdriver = mssqldriver):
    ((
        data
            .write
            .format('jdbc')
            .option('url', pgconn_dw)
            .option('user', pguser)
            .option('password', pgpwd)
            .option('driver', pgdriver)
            .option('dbtable', 'dimension.packagetype')
            .option('mode', 'overwrite')
        .save(mode='overwrite')
        
    ))
    
if __name__ == '__main__':
    createpackage()
    renamepackage()
    writepackage()
    spark.stop()