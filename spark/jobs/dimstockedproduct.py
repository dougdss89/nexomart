#importing packages, sessions and env_var

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f
from dotenv import load_dotenv

import os

env = load_dotenv('/mnt/d/linux/project/vars/.env')

pgjar = os.getenv('POSTGRE_JAR')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')
pgdriver = os.getenv('PG_DRIVER')
pgconn = os.getenv('POSTGRE_CONN')
pgconn_dw = os.getenv('POSTGRE_CONN_DW')

#create spark session

spark = ((
        SparkSession
            .builder
            .appName('stock_holding')
            .config('spark.jars', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
        .getOrCreate()
))

#loading data from table
def createstockholding(spark = spark, pgconn = pgconn, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    """ creating stock holding dimension """
    
    tmpstockholding_frame = (( spark
                                    .read
                                    .format('jdbc')
                                    .option('url', pgconn)
                                    .option('driver', pgdriver)
                                    .option('user', pguser)
                                    .option('password', pgpwd)
                                    .option('query',
                                            
                                            """ 
                                                select

                                                    stockitemid,
                                                    binlocation,
                                                    reorderlevel,
                                                    lasteditedby,
                                                    lastcostprice,
                                                    quantityonhand,
                                                    laststocktakequantity,
                                                    targetstocklevel,
                                                    _airbyte_ab_id,
                                                    _airbyte_stockitemholdings_hashid,
                                                    _airbyte_emitted_at,
                                                    _airbyte_normalized_at
                                                
                                                from warehouse.stockitemholdings
                                            
                                            """
                                        )                         
                            )).load()
    
    return tmpstockholding_frame

#rename columns
#making some adjustments
def renamedimension(data = createstockholding()):
    
    itemholding  = (( 
                    data
                        .withColumnRenamed('stockitemid', 'productid')
                        .withColumnRenamed('binlocation', 'shelflocation')
                        .withColumnRenamed('quantityonhand', 'availabeqtd')
                        .withColumnRenamed('laststocktakequantity', 'laststockqtd')
                        .withColumnRenamed('_airbyte_ab_id', 'extract_id')
                        .withColumnRenamed('_airbyte_stockitemsholding_hashid', 'stockedprod_hashid')
                        .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                        .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
    ))
    
    return itemholding

#write data into a new table on datawarehouse
def writingdata(df = renamedimension(), pgconn_dw = pgconn_dw, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    ((
        df
            .write
            .format('jdbc')
            .option('url', pgconn_dw)
            .option('driver', pgdriver)
            .option('user', pguser)
            .option('password', pgpwd)
            .option('dbtable', 'dimension.stockedproduct')
        .save(mode='overwrite')
    ))

#closing spark application
if __name__ == '__main__':
    
    createstockholding()
    renamedimension()
    writingdata()
    spark.stop()