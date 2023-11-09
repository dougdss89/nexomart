#load env-vars and sparksession

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os
from dotenv import load_dotenv

env = load_dotenv('/mnt/d/linux/project/vars/.env')

pgconn = os.getenv('POSTGRE_CONN')
pgdriver = os.getenv('PG_DRIVER')
pgjar = os.getenv('POSTGRE_JAR')
pgpwd = os.getenv('POSTGRE_PWD')
pguser = os.getenv('POSTGRE_USER')
pgconn_dw = os.getenv('POSTGRE_CONN_DW')


#creating session

spark = (( 
        SparkSession
            .builder
            .appName('dim_delivery')
            .config('spark.jars', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
        ))

print('Session created')

def createdelivery(spark = spark, conn= pgconn, driver = pgdriver, user = pguser, pwd = pgpwd):
    
    """ creating delivery dataframe from deliverymethods table """
    tmp_deliveryframe = (( spark
                            .read
                            .format('jdbc')
                            .option('url', f'{pgconn}')
                            .option('driver', f'{pgdriver}')
                            .option('user', f'{pguser}')
                            .option('password', f'{pgpwd}')
                            .option('query', 
                                    
                                    """ 
                                        select
                                        
                                            deliverymethodid,
                                            deliverymethodname,
                                            _airbyte_ab_id,
                                            _airbyte_deliverymethods_hashid,
                                            _airbyte_emitted_at,
                                            _airbyte_normalized_at
                                            
                                        from application.deliverymethods
                                    
                                    """)
                            
                        )).load()
    
    return tmp_deliveryframe

delivery = createdelivery()

#renaming delivery dimension columns

def renamecolumns(deliverydt = delivery):
    
    """renaming delivery dimension columns

    Returns:
        _type_: dataframe
    """
    
    deliveryframe = (( 
                        deliverydt
                            .withColumnRenamed('deliverymethodid', 'deliveryid')
                            .withColumnRenamed('deliverymethodname', 'deliverytype')
                            .withColumnRenamed('_airbyte_ab_id', 'elt_hashid')
                            .withColumnRenamed('_airbyte_deliverymethods_hashid', 'deliverymethods_hashid')
                            .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                            .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
                    ))
    
    return deliveryframe

delivery = renamecolumns()

#writing data on dimension table
def writedataframe(data = delivery, conn = pgconn_dw, driver = pgdriver, user = pguser, pwd = pgpwd):
    
    """writing data on data warehouse
        table: dimension.deliverymethod
    """
    (( data
        .write
            .format('jdbc')
            .option('url', conn)
            .option('driver', driver)
            .option('user', user)
            .option('password', pwd)
            .option('dbtable', 'dimension.deliverymethod')
        .save(mode= 'overwrite')
    ))
    
if __name__ == '__main__':

    createdelivery()
    renamecolumns()
    writedataframe()
    spark.stop()