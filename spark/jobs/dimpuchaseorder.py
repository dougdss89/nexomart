#importing packages

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os
from dotenv import load_dotenv

env = load_dotenv('/mnt/d/linux/project/vars/.env')

#importing vars

pgconn = os.getenv('POSTGRE_CONN')
pgdriver = os.getenv('PG_DRIVER')
pgjar = os.getenv('POSTGRE_JAR')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')

#create session

spark = (( SparkSession
            .builder
                .appName('purchase_orderline')
                .config('spark.jars', f'{pgjar}')
                .config('spark.driver.extraClassPath', f'{pgjar}')
                .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
        ))

tmp_purchaseordline = (( spark
                            .read
                            .format('jdbc')
                            .option('url', pgconn)
                            .option('driver', pgdriver)
                            .option('user', pguser)
                            .option('password', pgpwd)
                            .option('query', 
                                    
                                    """ 
                                        select 
                                            purchaseorderlineid,
                                            purchaseorderid,
                                            stockitemid,
                                            receivedouters,
                                            orderedouters,
                                            packagetypeid,
                                            description,
                                            expectedunitpriceperouter,
                                            cast(isorderlinefinalized as varchar(10)) as isorderlinefinalized,
                                            lastreceiptdate,
                                            _airbyte_ab_id,
                                            _airbyte_purchaseorderlines_hashid,
                                            _airbyte_emitted_at,
                                            _airbyte_normalized_at
                                        
                                        from purchasing.purchaseorderlines
                                    
                                    """                            
                                    )
                        )).load()

