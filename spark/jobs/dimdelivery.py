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

deliveryframe = (( 
                    tmp_deliveryframe
                        .withColumnRenamed('deliverymethodid', 'deliveryid')
                        .withColumnRenamed('deliverymethodname', 'deliverytype')
                        .withColumnRenamed('_airbyte_ab_id', 'elt_hashid')
                        .withColumnRenamed('_airbyte_deliverymethods_hashid', 'deliverymethods_hashid')
                        .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                        .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
                ))


(( deliveryframe
    .write
        .format('jdbc')
        .option('url', pgconn)
        .option('driver', pgdriver)
        .option('user', pguser)
        .option('password', pgpwd)
        .option('dbtable', 'application.testeinsert')
    .save(mode= 'overwrite')
))

spark.stop()