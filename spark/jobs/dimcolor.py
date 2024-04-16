from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, FloatType, IntegerType, StringType, DataType

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

#create spark session 
#creating sparksession
spark = SparkSession.\
            builder.\
            appName('teste_dimcolors').\
            config('spark.jars', f'{sqlserverjar}').\
            config('spark.driver.extraClassPath', f'{sqlserverjar}').\
            config('spark.executor.extraClassPath', f'{sqlserverjar}').\
        getOrCreate()


def selectcolors(spark = spark, connurl = mssqlsource , dbuser = mssqluser, dbpwd = mssqlpwd, dbdriver = mssqldriver):
    
    """ 
    
    this function selects data from colors table on warehouse schema
    All parameters are set by default variables that we create at the beginning.
    
    """
       
            
    dimension_colors = ((spark.\
                    read.\
                    format('jdbc').\
                    option('url', mssqlsource).\
                    option('user', mssqluser).\
                    option('password', mssqlpwd).\
                    option('driver', mssqldriver).\
                    option('query',
                        
                        """ 
                            select 

                                wrh_c.colorid,
                                wrh_c.colorname
                            
                            from warehouse.colors as wrh_c
                                left join
                                warehouse.colors_archive as wrh_ac
                            on wrh_c.colorid = wrh_ac.colorid

                        """))).load()
    
    return dimension_colors

data = selectcolors()



data = selectcolors()

def insertcolor(spark = spark, connurl = mssql_dwdest , dbuser = mssqluser, dbpwd = mssqlpwd, dbdriver = mssqldriver, data = data):
    
    data.\
        write.\
        format('jdbc').\
        option('url', connurl).\
        option('user', dbuser).\
        option('dbtable', 'dimension.colors').\
        option('mode', 'overwrite').\
        option('password', dbpwd).\
        option('driver', dbdriver).\
    save(mode='overwrite')
    
insertcolor()

if __name__ == '__main__':
    
    selectcolors()
    insertcolor()
    spark.stop()