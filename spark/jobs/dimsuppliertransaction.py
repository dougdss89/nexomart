#import packages

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f
import os

from dotenv import load_dotenv

env = load_dotenv('/mnt/d/linux/project/vars/.env')

#importing variables
pgjar = os.getenv('POSTGRE_JAR')
pgconn = os.getenv('POSTGRE_CONN')
pgdriver = os.getenv('PG_DRIVER')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')

#create session
spark = ((SparkSession
            .builder
            .appName('suppliertransaction')
            .config('spark.jars', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
        ))

#loding data from source table 

tmp_suppliertransaction = (( spark
                                .read
                                .format('jdbc')
                                .option('url', pgconn)
                                .option('driver', pgdriver)
                                .option('user', pguser)
                                .option('password', pgpwd)
                                .option('query', 
                                        """ 
                                            select
                                                
                                                supplierinvoicenumber,
                                                transactiontypeid,
                                                supplierid,
                                                purchaseorderid,
                                                suppliertransactionid,
                                                paymentmethodid,
                                                transactionamount,
                                                taxamount,
                                                amountexcludingtax,
                                                outstandingbalance,
                                                transactiondate,
                                                isfinalized,
                                                _airbyte_ab_id,
                                                _airbyte_suppliertransactions_hashid,
                                                _airbyte_emitted_at,
                                                _airbyte_normalized_at
                                                
                                            from purchasing.suppliertransactions
                                        """)
                                )).load()

renamed_frame = (( tmp_suppliertransaction
                    .select('*', 
                            f.coalesce('supplierinvoicenumber', f.lit('999999'))
                                .alias('supplierinvoicenumber_n'),
                    
                            f.coalesce('purchaseorderid', f.lit('999999'))
                                .alias('purchaseorderid_n'),
                            
                            #substituting true-false for 0-1
                            f.when(tmp_suppliertransaction.isfinalized == 'true', 1)
                                .otherwise(0)
                                .alias('isfinalized_n')
                        )
                                
                ))
dropcol_transaction = (( renamed_frame
                            .drop('supplierinvoicenumber')
                            .drop('purchaseorderid')
                            .drop('isfinalized')
                            
                            #renaming
                            .withColumnRenamed('supplierinvoicenumber_n', 'suppliertransaction_number')
                            .withColumnRenamed('taxamount', 'tax')
                            .withColumnRenamed('amountexcludingtax', 'price')
                            .withColumnRenamed('transactionamount', 'total')
                            .withColumnRenamed('purchaseorderid_n', 'purchaseorderid')
                            .withColumnRenamed('isfinalized_n', 'isfinalized')
                            .withColumnRenamed('_airbyte_ab_id', 'elt_hashid')
                            .withColumnRenamed('_airbyte_suppliertransactions_hashid', 'suppliertransactions_hashid')
                            .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                            .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
                            .withColumnRenamed('outstandingbalance', 'balance')
                    ))

#organizing table
suppliertransaction_frame = ((  
                              dropcol_transaction
                                .select('suppliertransactionid',
                                        'transactiontypeid',
                                        'suppliertransaction_number',
                                        'purchaseorderid',
                                        'supplierid',
                                        'paymentmethodid',
                                        'price',
                                        'tax',
                                        'total',
                                        'balance',
                                        'transactiondate',
                                        'isfinalized',
                                        'elt_hashid',
                                        'suppliertransactions_hashid',
                                        'elt_start',
                                        'elt_end')
                                ))

#write to datawarehouse
(( 
  suppliertransaction_frame
    .write
        .format('jdbc')
        .option('url', pgconn)
        .option('driver', pgdriver)
        .option('user', pguser)
        .option('password', pgpwd)
        .option('dbtable', 'purchasing.testeinsert')
    .save(mode= 'overwrite')
  ))