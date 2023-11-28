#import packages and vars

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os
from dotenv import load_dotenv

env = load_dotenv('/mnt/d/linux/project/vars/.env')

#creating env-vars
pgconn = os.getenv('POSTGRE_CONN')
pgdriver = os.getenv('PG_DRIVER')
pgjar = os.getenv('POSTGRE_JAR')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')
pgconn_dw = os.getenv('POSTGRE_CONN_DW')

#creating session

spark = (( SparkSession 
            .builder
            .appName('dim_transaction')
            .config('spark.jars', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
            
        ))

#load data from table
def createtransaction(spark = spark, pgconn = pgconn, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    """ creating transaction type dimension """
    tmp_transactionframe = (( spark
                                .read
                                    .format('jdbc')
                                    .option('url', pgconn)
                                    .option('driver', pgdriver)
                                    .option('user', pguser)
                                    .option('password', pgpwd)
                                    .option('query', 
                                        
                                            """ 
                                            select
                                            
                                                transactiontypeid,
                                                transactiontypename,
                                                _airbyte_ab_id,
                                                _airbyte_transactiontypes_hashid,
                                                _airbyte_emitted_at,
                                                _airbyte_normalized_at
                                                
                                            from application.transactiontypes
                                            """)
                            )).load()
    return tmp_transactionframe

#normalize columns name
def renametransactioncolumns(dataframe = createtransaction()):
    
    transactionframe = (( dataframe
                            .withColumnRenamed('transactiontypename', 'transaction_name')
                            .withColumnRenamed('_airbyte_ab_id', 'elt_hashid')
                            .withColumnRenamed('_airbyte_transactiontypes_hashid', 'transactiontype_hashid')
                            .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                            .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
                        ))
    
    return transactionframe

#load data to a table
def writetransactiontype(datasource = renametransactioncolumns(), pgconn_dw = pgconn_dw, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    """writing transaction type dimension on data warehouse
    
    """
    (( datasource
        .write
            .format('jdbc')
            .option('url', pgconn_dw)
            .option('driver', pgdriver)
            .option('user', pguser)
            .option('password', pgpwd)
            .option('dbtable', 'dimension.transactiontype')
        .save(mode= 'overwrite')
    ))
    
if __name__== '__main__':
    
    createtransaction()
    renametransactioncolumns()
    writetransactiontype()
    spark.stop()