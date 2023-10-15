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


#create sparksession

#creatins session

spark = (( 
          SparkSession
            .builder
            .appName('stock_transaction')
            .config('spark.jars', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
        .getOrCreate()
        ))

#load data from table
#creating temporary dataframe
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
                                            quantity,
                                            lasteditedby,
                                            _airbyte_ab_id,
                                            _airbyte_stockitemtransactions_hashid,
                                            _airbyte_emitted_at,
                                            _airbyte_normalized_at
                                            
                                        from warehouse.stockitemtransactions
         
                                    """                        
                                    )                            
                         )).load()