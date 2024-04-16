
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

#creating session
def sparksession():
    
    spark = (( SparkSession 
                .builder
                .appName('dim_transaction')
                .config('spark.jars', f'{sqlserverjar}')
                .config('spark.driver.extraClassPath', f'{sqlserverjar}')
                .config('spark.executor.extraClassPath', f'{sqlserverjar}')
                .getOrCreate()
                
            ))

    return spark

spark = sparksession()

#load data from table
def createtransaction(spark = sparksession(), conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """ creating transaction type dimension """
    tmp_transactionframe = (( spark
                                .read
                                    .format('jdbc')
                                    .option('url', conn)
                                    .option('driver', driver)
                                    .option('user', user)
                                    .option('password', pwd)
                                    .option('query', 
                                        
                                            """ 
                                            select
                                            
                                                transactiontypeid,
                                                transactiontypename
                                                
                                            from application.transactiontypes
                                            
                                            """)
                            )).load()
    return tmp_transactionframe

#normalize columns name
def renametransactioncolumns(dataframe = createtransaction()):
    
    transactionframe = (( dataframe
                            .withColumnRenamed('transactiontypename', 'transaction_name')

                        ))
    
    return transactionframe

#load data to a table
def writetransactiontype(datasource = renametransactioncolumns(), conn_dw = mssql_dwdest, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """writing transaction type dimension on data warehouse
    
    """
    (( datasource
        .write
            .format('jdbc')
            .option('url', conn_dw)
            .option('driver', driver)
            .option('user', user)
            .option('password', pwd)
            .option('dbtable', 'dimension.transactiontype')
        .save(mode= 'overwrite')
    ))
    
if __name__== '__main__':
    
    createtransaction()
    renametransactioncolumns()
    writetransactiontype()
    spark.stop()