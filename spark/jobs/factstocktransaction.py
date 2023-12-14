#import packages, modules and functions

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f
import os
from dotenv import load_dotenv

#load vars
#load vars

env = load_dotenv('/mnt/d/linux/project/vars/.env')

pgconn = os.getenv('POSTGRE_CONN')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')
pgjar = os.getenv('POSTGRE_JAR')
pgdriver = os.getenv('PG_DRIVER')
pgconn_dw = os.getenv('POSTGRE_CONN_DW')

print('Environments variable loaded')

#create sparksession
spark = (( 
          SparkSession
            .builder
            .appName('stock_transaction')
            .config('spark.jars', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
        .getOrCreate()
        ))

print('Session created')

#load data from table
#creating temporary dataframe
def createtransaction(spark=spark, pgconn = pgconn, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    """ creating stocktransaction fact for data analysis"""
    
    tmp_stocktransaction = (( 
                            spark
                                .read
                                .format('jdbc')
                                .option('driver', pgdriver)
                                .option('url', pgconn)
                                .option('user', pguser)
                                .option('password', pgpwd)
                                .option('query', 
                                        
                                        """ 
                                            select
                                            
                                                stockitemtransactionid,
                                                stockitemid,
                                                supplierid,
                                                purchaseorderid,
                                                transactiontypeid,
                                                invoiceid,
                                                customerid,
                                                (quantity * -1) as qtdprocessed,
                                                lasteditedby,
                                                _airbyte_ab_id,
                                                _airbyte_stockitemtransactions_hashid,
                                                _airbyte_emitted_at,
                                                _airbyte_normalized_at
                                                
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
                        .withColumnRenamed('_airbyte_ab_id', 'elt_hashid')
                        .withColumnRenamed('_airbyte_stockitemtransactions_hashid', 'stocktransaction_hashid')
                        .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                        .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
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
                                    'stockitemid as productid',
                                    'transactiontypeid',
                                    'customerid',
                                    'invoiceid',
                                    'supplierid',
                                    'purchaseorderid',
                                    'qtdprocessed',
                                    'lasteditedby',
                                    'elt_hashid',
                                    'stocktransaction_hashid',
                                    'elt_start',
                                    'elt_end')
    ))

    return stocktransation

#load data to table
def writetransaction(sourcedata = reorganizedropcolumns(), pgconn_dw = pgconn_dw, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    """ writing data on fact table """
    ((
    sourcedata
        .write
        .format('jdbc')
        .option('url', pgconn_dw)
        .option('driver', pgdriver)
        .option('user', pguser)
        .option('password', pgpwd)
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