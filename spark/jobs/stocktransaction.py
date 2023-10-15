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

print('Data loaded to table')

#rename columns 
renamed_frame = ((tmp_stocktransaction
                    .withColumnRenamed('stockitemtransactionid', 'stocktransactionid')
                    .withColumnRenamed('_airbyte_ab_id', 'elt_hashid')
                    .withColumnRenamed('_airbyte_stockitemtransactions_hashid', 'stocktransaction_hashid')
                    .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                    .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
))
print('Column renamed')

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
print('Nulls removed')

#drop null columns
#rename normalized columns
dropcolumns = (( removenull
                    .drop('supplierid')
                    .drop('customerid')
                    .drop('purchaseorderid')
                    .drop('invoiceid')
                    .withColumnRenamed('supplierid_n', 'supplierid')
                    .withColumnRenamed('customerid_n', 'customerid')
                    .withColumnRenamed('purchaseorderid_n', 'purchaseorderid')
                    .withColumnRenamed('invoiceid_n', 'invoiceid')
))
print('Droped and renamed columns')

#reorganizing columns hierarchy
stocktransation = ((dropcolumns
                        .select('stocktransactionid',
                                'stockitemid',
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
print('Columns reorganized')

#load data to table
((
stocktransation
    .write
    .format('jdbc')
    .option('url', pgconn)
    .option('driver', pgdriver)
    .option('user', pguser)
    .option('password', pgpwd)
    .option('dbtable', 'warehouse.testeinsert')
    .option('mode', 'overwrite')
.save(mode= 'overwrite')
))
print('Data persisted into table')

spark.stop()