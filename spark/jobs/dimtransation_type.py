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

#normalize columns name

transactionframe = (( tmp_transactionframe
                        .withColumnRenamed('transactiontypename', 'transaction_name')
                        .withColumnRenamed('_airbyte_ab_id', 'elt_hashid')
                        .withColumnRenamed('_airbyte_transactiontypes_hashid', 'transactiontype_hashid')
                        .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                        .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
                    ))

#load data to a table
(( transactionframe
    .write
        .format('jdbc')
        .option('url', pgconn)
        .option('driver', pgdriver)
        .option('user', pguser)
        .option('password', pgpwd)
        .option('dbtable', 'application.testeinsert')
    .save(mode= 'overwrite')
))