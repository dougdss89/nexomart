#importing packages and modules
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f
import os

from dotenv import load_dotenv

env = load_dotenv('/mnt/d/linux/project/vars/.env')

#load vars

pgconn = os.getenv('POSTGRE_CONN')
pgdriver = os.getenv('PG_DRIVER')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')
pgjar = os.getenv('POSTGRE_JAR')

#creating session 

spark = (( SparkSession
            .builder
                .appName('purchase')
                .config('spark.jars', f'{pgjar}')
                .config('spark.driver.extraClassPath', f'{pgjar}')
                .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
        ))

#load data from table
tmp_purchaseframe = (( 
                      spark
                        .read
                        .format('jdbc')
                        .option('url', pgconn)
                        .option('driver', pgdriver)
                        .option('user', pguser)
                        .option('password', pgpwd)
                        .option('query', 
                                
                                """ 
                                    
                                    select 

                                      purchaseorderid,
                                      supplierid,
                                      contactpersonid,
                                      deliverymethodid,
                                      orderdate,
                                      expecteddeliverydate,
                                      supplierreference,
                                      isorderfinalized,
                                      _airbyte_ab_id,
                                      _airbyte_purchaseorders_hashid,
                                      _airbyte_emitted_at,
                                      _airbyte_normalized_at

                                    from purchasing.purchaseorders
                                
                                """)
                        )).load()

#write data to table
(( 
  tmp_purchaseframe
    .write
        .format('jdbc')
        .option('url', pgconn)
        .option('driver', pgdriver)
        .option('user', pguser)
        .option('password', pgpwd)
        .option('dbtable', 'purchasing.testeinsert')
    .save(mode= 'overwrite')
))