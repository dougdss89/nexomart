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

#create session
spark = ((SparkSession
            .builder
            .appName('suppliertransaction')
            .config('spark.jars', f'{sqlserverjar}')
            .config('spark.driver.extraClassPath', f'{sqlserverjar}')
            .config('spark.executor.extraClassPath', f'{sqlserverjar}')
            .getOrCreate()
        ))

#loding data from source table 
def createsupplier_transaction(spark = spark, conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """ creatign supplier transaction fact table """
    
    tmp_suppliertransaction = (( spark
                                    .read
                                    .format('jdbc')
                                    .option('url', conn)
                                    .option('driver', driver)
                                    .option('user', user)
                                    .option('password', pwd)
                                    .option('query', 
                                            """ 
                                                select
                                                    
                                                    supplierinvoicenumber,
                                                    transactiontypeid,
                                                    supplierid,
                                                    purchaseorderid,
                                                    suppliertransactionid,
                                                    paymentmethodid,
                                                    transactionamount,
                                                    taxamount,
                                                    amountexcludingtax,
                                                    outstandingbalance,
                                                    transactiondate,
                                                    isfinalized
                                                    
                                                from purchasing.suppliertransactions
                                            """)
                                    )).load()
    return tmp_suppliertransaction

def renametransaction(data = createsupplier_transaction()):
    
    """ raname transaction dataframe columns
    """
    renamed_frame = (( data
                        .select('*', 
                                f.coalesce('supplierinvoicenumber', f.lit('999999'))
                                    .alias('supplierinvoicenumber_n'),
                        
                                f.coalesce('purchaseorderid', f.lit('999999'))
                                    .alias('purchaseorderid_n'),
                                
                                #substituting true-false for 0-1
                                f.when(data.isfinalized == 'true', 1)
                                    .otherwise(0)
                                    .alias('isfinalized_n')
                            )
                                    
                    ))
    
    return renamed_frame

#droping columns from dataframe
def dropcolumns(sourcedata = renametransaction()):
    
    """ 
        droping nonconformed columns
    """
    dropcol_transaction = (( sourcedata
                                .drop('supplierinvoicenumber')
                                .drop('purchaseorderid')
                                .drop('isfinalized')
                                
                                #renaming
                                .withColumnRenamed('supplierinvoicenumber_n', 'suppliertransaction_number')
                                .withColumnRenamed('taxamount', 'tax')
                                .withColumnRenamed('amountexcludingtax', 'price')
                                .withColumnRenamed('transactionamount', 'total')
                                .withColumnRenamed('purchaseorderid_n', 'purchaseorderid')
                                .withColumnRenamed('isfinalized_n', 'isfinalized')
                                .withColumnRenamed('outstandingbalance', 'balance')
                        ))
    return dropcol_transaction

#organizing table
def organizedataframe(dataframe = dropcolumns()):
    
    """ organize hierarchy columns 
    """
    suppliertransaction_frame = ((  
                                dataframe
                                    .select('suppliertransactionid',
                                            'transactiontypeid',
                                            'suppliertransaction_number',
                                            'purchaseorderid',
                                            'supplierid',
                                            'paymentmethodid',
                                            'price',
                                            'tax',
                                            'total',
                                            'balance',
                                            'transactiondate',
                                            'isfinalized')
                                ))
    return suppliertransaction_frame

#write to datawarehouse
def writetransactionfact(transactionframe = organizedataframe(), conn_dw = mssql_dwdest, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    """write transaction dataframe on fact table

    Args:
        transactionframe (_type_, dataframe): _description_. Defaults to organizedataframe().
        conn_dw (_type_, str): _description_. Defaults to conn_dw.
        driver (_type_, str): _description_. Defaults to driver.
        user (_type_, str): _description_. Defaults to user.
        pwd (_type_, str): _description_. Defaults to pwd.
    """
    (( 
    transactionframe
        .write
            .format('jdbc')
            .option('url', conn_dw)
            .option('driver', driver)
            .option('user', user)
            .option('password', pwd)
            .option('dbtable', 'fact.suppliertransaction')
        .save(mode= 'overwrite')
    ))
    
if __name__=='__main__':
    
    createsupplier_transaction()
    renametransaction()
    dropcolumns()
    organizedataframe()
    writetransactionfact()
    spark.stop()