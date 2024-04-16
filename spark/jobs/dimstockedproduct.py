#importing packages, sessions and env_var

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

#create spark session

spark = ((
        SparkSession
            .builder
            .appName('stock_holding')
            .config('spark.jars', f'{sqlserverjar}')
            .config('spark.executor.extraClassPath', f'{sqlserverjar}')
            .config('spark.driver.extraClassPath', f'{sqlserverjar}')
        .getOrCreate()
))

#loading data from table
def createstockholding(spark = spark, conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    """ creating stock holding dimension """
    
    tmpstockholding_frame = (( spark
                                    .read
                                    .format('jdbc')
                                    .option('url', conn)
                                    .option('driver', driver)
                                    .option('user', user)
                                    .option('password', pwd)
                                    .option('query',
                                            
                                            """ 
                                                select

                                                    stockitemid,
                                                    binlocation,
                                                    reorderlevel,
                                                    lasteditedby,
                                                    lastcostprice,
                                                    quantityonhand,
                                                    laststocktakequantity,
                                                    targetstocklevel
                                                
                                                from warehouse.stockitemholdings
                                            
                                            """
                                        )                         
                            )).load()
    
    return tmpstockholding_frame

#rename columns
#making some adjustments
def renamedimension(data = createstockholding()):
    
    itemholding  = (( 
                    data
                        .withColumnRenamed('stockitemid', 'productid')
                        .withColumnRenamed('binlocation', 'shelflocation')
                        .withColumnRenamed('quantityonhand', 'availabeqtd')
                        .withColumnRenamed('laststocktakequantity', 'laststockqtd')
    ))
    
    return itemholding

#write data into a new table on datawarehouse
def writingdata(df = renamedimension(), conn_dw = mssql_dwdest, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    ((
        df
            .write
            .format('jdbc')
            .option('url', conn_dw)
            .option('driver', driver)
            .option('user', user)
            .option('password', pwd)
            .option('dbtable', 'dimension.stockedproduct')
        .save(mode='overwrite')
    ))

#closing spark application
if __name__ == '__main__':
    
    createstockholding()
    renamedimension()
    writingdata()
    spark.stop()