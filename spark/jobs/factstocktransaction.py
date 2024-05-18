#importing packages 

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

print('Environments variable loaded')

#create sparksession
def sparksession():
    
    spark = (( 
            SparkSession
                .builder
                .appName('stock_transaction')
                .config('spark.jars', f'{sqlserverjar}')
                .config('spark.driver.extraClassPath', f'{sqlserverjar}')
                .config('spark.executor.extraClassPath', f'{sqlserverjar}')
            .getOrCreate()
            ))
    
    return spark

spark = sparksession()

print('Session created')

#load data from table
#creating temporary dataframe
def createtransaction(spark=spark, conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """ creating stocktransaction fact for data analysis"""
    
    tmp_stocktransaction = (( 
                            spark
                                .read
                                .format('jdbc')
                                .option('driver', driver)
                                .option('url', conn)
                                .option('user', user)
                                .option('password', pwd)
                                .option('query', 
                                        
                                        """ 
                                            select
                                            
                                                stockitemtransactionid,
                                                stockitemid as productid,
                                                supplierid,
                                                purchaseorderid,
                                                transactiontypeid,
                                                invoiceid,
                                                customerid,
                                                (quantity * -1) as qtdprocessed,
                                                lasteditedby
                                                
                                            from warehouse.stockitemtransactions
            
                                        """                        
                                        )                            
                            )).load()
    
    return tmp_stocktransaction

#rename columns 
def renameremovenull(dataframe = createtransaction()):
    
    """ rename and remove nulls from transaction dataframe """
    
    renamed_frame = ((dataframe
                        .withColumnRenamed('stockitemtransactionid', 'stocktransactionid')


    ))

#remove nulls
    removenull = ((
                    renamed_frame
                        .select('*', 
                            f.coalesce('supplierid', f.lit(0))
                                .alias('supplierid_n'),
                                
                            f.coalesce('customerid', f.lit(0))
                                .alias('customerid_n'),
                                
                            f.coalesce('purchaseorderid', f.lit(0))
                                .alias('purchaseorderid_n'),
                                
                            f.coalesce('invoiceid', f.lit(0))
                                .alias('invoiceid_n'))        
    ))

    return removenull

#drop null columns
#rename normalized columns

def reorganizedropcolumns(data = renameremovenull()):
    
    """ drop columns and reorganize columns """
    dropcolumns = (( data
                        .drop('supplierid')
                        .drop('customerid')
                        .drop('purchaseorderid')
                        .drop('invoiceid')
                        .withColumnRenamed('supplierid_n', 'supplierid')
                        .withColumnRenamed('customerid_n', 'customerid')
                        .withColumnRenamed('purchaseorderid_n', 'purchaseorderid')
                        .withColumnRenamed('invoiceid_n', 'invoiceid')
    ))

#reorganizing columns hierarchy
    stocktransation = ((dropcolumns
                            .select('stocktransactionid',
                                    'productid',
                                    'transactiontypeid',
                                    'customerid',
                                    'invoiceid',
                                    'supplierid',
                                    'purchaseorderid',
                                    'qtdprocessed',
                                    'lasteditedby')
    ))

    return stocktransation

#load data to table
def writetransaction(sourcedata = reorganizedropcolumns(), conn_dw = mssql_dwdest, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """ writing data on fact table """
    ((
    sourcedata
        .write
            .format('jdbc')
            .option('url', conn_dw)
            .option('driver', driver)
            .option('user', user)
            .option('password', pwd)
            .option('dbtable', 'fact.stocktransaction')
            .option('mode', 'overwrite')
    .save(mode= 'overwrite')
    ))

if __name__== '__main__':
    
    createtransaction()
    renameremovenull()
    reorganizedropcolumns()
    writetransaction()
    spark.stop()