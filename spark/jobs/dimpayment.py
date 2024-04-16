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

#create session
spark = (( SparkSession
            .builder
            .appName('payments')
            .config('spark.jars', f'{sqlserverjar}' )
            .config('spark.executor.extraClassPath', f'{sqlserverjar}')
            .config('spark.driver.extraClassPath', f'{sqlserverjar}')
            .getOrCreate()
        ))

#loading data

def createpayment(spark = spark, conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """ 
        loading paymentmethod table to payment dimension
        on data warehouse.
    """
    tmp_paymentframe = (( spark
                            .read
                            .format('jdbc')
                            .option('url', conn)
                            .option('driver', driver)
                            .option('user', user)
                            .option('password', pwd)
                            .option('query', 
                                    
                                    """ 
                                    
                                        select
                                        
                                            paymentmethodid,
                                            paymentmethodname
                                            
                                        from application.paymentmethods
                                    
                                    """
                                    )
                        )).load()
    
    return tmp_paymentframe

#renaming columns
def renamepayment(data = createpayment()):
    
    """ just renaming columns to insert on datawarehouse """
    
    paymentframe = (( data
                        .withColumnRenamed('paymentmethodid', 'paymentid')
                        .withColumnRenamed('paymentmethodname', 'methodname')

                    ))
    
    return paymentframe


#writing on datawarehouse

def writepayment(data = renamepayment(), conn_dw = mssql_dwdest, user = mssqluser, pwd = mssqlpwd, driver = mssqldriver):
    (( 
    data
        .write
        .format('jdbc')
        .option('url', conn_dw)
        .option('driver', driver)
        .option('user', user)
        .option('password', pwd)
        .option('dbtable', 'dimension.paymentmethod')
        .save(mode='overwrite')
    ))
    
    return "ELT finished successfully"

if __name__ == '__main__':
    
    createpayment()
    renamepayment()
    writepayment()
    spark.stop()