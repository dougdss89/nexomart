
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


#creating spark session

#CREATE SESSION
spark = ((
        SparkSession
            .builder
            .appName('dim_stock')
            .config('spark.jars', f'{sqlserverjar}')
            .config('spark.driver.extraClassPath', f'{sqlserverjar}')
            .config('spark.executor.extraClassPath', f'{sqlserverjar}')
        .getOrCreate()
))

spark.sparkContext.setLogLevel("WARN")

#load data from postgre database table
#start cleaning data

#first of all, lets create a dataframe from data table
def createproduct(spark = spark, conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
        
        stockitems_frame = (( spark.read
                        .format('jdbc')
                        .option('url', conn)
                        .option('user', user)
                        .option('password', pwd)
                        .option('driver', driver)
                        .option('query', 
                                
                                """  
                                select * from warehouse.sparkproduct
                                
                                """      
                        ))).load()
        return stockitems_frame

#once data is loaded into a dataframe
#cleaning customfield column wiht reg_exp
#remove characteres like " {} [] "
#renaming resulting columns
#drop columns that not satisfy our dataset

def cleaningproduct(stockitems_frame = createproduct()):
        
        productframe = (stockitems_frame
                                .select('*',\
                                        f.regexp_replace('customfields', "[^0-9a-zA-Z , :]", "")
                                                .alias('temp_customfield'),
                                        f.regexp_replace('tags', '[^0-9a-zA-Z]', '')
                                                .alias('tags'))
                                .withColumnRenamed('Size', 'size')
                                .drop('customfields')
                                .drop('tags')
        )
        
        return productframe
                        


#now, select substring from temp_customfield
#leaving information about country manufacture
#droping unused columns
def extractname(temp_product = cleaningproduct()):
        
        (
        temp_product
                .select('*',
                        f.substring_index('temp_customfield', ',', 1)
                        .alias('temp_country'),
                        f.substring_index('temp_country', ':', -1)
                        .alias('countrymanufacture'))
                .drop('temp_country')
        )
        
        return temp_product
       
       
#fill null values
#renaming airbyte columns
def fillproductnull(dataframe = extractname()):
        
        dimproduct = (
        
                dataframe.fillna({'size': 'Not Available',
                        'colorid': 0,
                        'brand': 'Not Available',
                        'marketingcomments': 'Not Available'
                        })
                
                .drop('temp_customfield')
        )
        
        return dimproduct

#load transformed data into database table
#finishing elt process

def writeproduct(data = fillproductnull(), conn_dw = mssql_dwdest, user = mssqluser, pwd = mssqlpwd, driver = mssqldriver):
        (
        data.write
                .format('jdbc')
                .option('url', conn_dw)
                .option('user', user)
                .option('password', pwd)
                .option('driver', driver)
                .option('mode', 'overwrite')
                .option('dbtable', 'dimension.product')
                .save(mode = 'overwrite')
        )
        
if __name__ == '__main__':
        createproduct()
        print('Product created')
        cleaningproduct()
        extractname()
        fillproductnull()
        writeproduct()
        spark.stop()