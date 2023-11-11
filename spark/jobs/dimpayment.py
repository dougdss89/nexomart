#import packages and env-vars

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os
from dotenv import load_dotenv

env = load_dotenv('/mnt/d/linux/project/vars/.env')

#load variables
pgjar = os.getenv('POSTGRE_JAR')
pgconn = os.getenv('POSTGRE_CONN')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')
pgdriver = os.getenv('PG_DRIVER')
pgconn_dw = os.getenv('POSTGRE_CONN_DW')

#create session
spark = (( SparkSession
            .builder
            .appName('payments')
            .config('spark.jars', f'{pgjar}' )
            .config('spark.executor.extraClassPath', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .getOrCreate()
        ))

#loading data

def createpayment(spark = spark, pgconn = pgconn, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    """ 
        loading paymentmethod table to payment dimension
        on data warehouse.
    """
    tmp_paymentframe = (( spark
                            .read
                            .format('jdbc')
                            .option('url', pgconn)
                            .option('driver', pgdriver)
                            .option('user', pguser)
                            .option('password', pgpwd)
                            .option('query', 
                                    
                                    """ 
                                    
                                        select
                                        
                                            paymentmethodid,
                                            paymentmethodname,
                                            _airbyte_ab_id,
                                            _airbyte_paymentmethods_hashid,
                                            _airbyte_emitted_at,
                                            _airbyte_normalized_at
                                            
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
                        .withColumnRenamed('_airbyte_ab_id', 'elt_hashid')
                        .withColumnRenamed('_airbyte_paymentmethods_hashid', 'paymentmethod_hashid')
                        .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                        .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
                    ))
    
    return paymentframe


#writing on datawarehouse

def writepayment(data = renamepayment(), pgconn_dw = pgconn_dw, pguser = pguser, pgpwd = pgpwd, pgdriver = pgdriver):
    (( 
    data
        .write
        .format('jdbc')
        .option('url', pgconn)
        .option('driver', pgdriver)
        .option('user', pguser)
        .option('password', pgpwd)
        .option('dbtable', 'application.testeinsert')
        .save(mode='overwrite')
    ))
    
    return "ELT finished successfully"

if __name__ == '__main__':
    
    createpayment()
    renamepayment()
    writepayment()
    spark.stop()