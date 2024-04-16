#importing packages and vars

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os
from dotenv import load_dotenv

#load environment variables
env = load_dotenv(dotenv_path='/mnt/d/linux/project/vars/.env')

#env vars for postgresql connection
mssqlsource = os.getenv('MSSQLSOURCE')
mssqluser = os.getenv('MSSQL_SPARK_USER')
mssqlpwd = os.getenv('MSSQL_SPARK_PWD')
mssqldriver = os.getenv('MSSQL_DRIVER')
sqlserverjar = os.getenv('SQLSERVER_JAR')
mssql_dwdest = os.getenv('MSSQL_CONN')


#creating session

spark = (( 
        SparkSession
            .builder
            .appName('dim_delivery')
            .config('spark.jars', f'{sqlserverjar}')
            .config('spark.driver.extraClassPath', f'{sqlserverjar}')
            .config('spark.executor.extraClassPath', f'{sqlserverjar}')
            .getOrCreate()
        ))

print('Session created')

def createdelivery(spark = spark, conn= mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """ creating delivery dataframe from deliverymethods table """
    tmp_deliveryframe = (( spark
                            .read
                            .format('jdbc')
                            .option('url', f'{conn}')
                            .option('driver', f'{driver}')
                            .option('user', f'{user}')
                            .option('password', f'{pwd}')
                            .option('query', 
                                    
                                    """ 
                                        select
                                        
                                            deliverymethodid,
                                            deliverymethodname
                                            
                                        from application.deliverymethods
                                    
                                    """)
                            
                        )).load()
    
    return tmp_deliveryframe

delivery = createdelivery()

#renaming delivery dimension columns

def renamecolumns(deliverydt = delivery):
    
    """renaming delivery dimension columns

    Returns:
        _type_: dataframe
    """
    
    deliveryframe = (( 
                        deliverydt
                            .withColumnRenamed('deliverymethodid', 'deliveryid')
                            .withColumnRenamed('deliverymethodname', 'deliverytype')

                    ))
    
    return deliveryframe

delivery = renamecolumns()

#writing data on dimension table
def writedataframe(data = delivery, conn = mssql_dwdest, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """writing data on data warehouse
        table: dimension.deliverymethod
    """
    (( data
        .write
            .format('jdbc')
            .option('url', conn)
            .option('driver', driver)
            .option('user', user)
            .option('password', pwd)
            .option('dbtable', 'dimension.deliverymethod')
        .save(mode= 'overwrite')
    ))
    
if __name__ == '__main__':

    createdelivery()
    renamecolumns()
    writedataframe()
    spark.stop()