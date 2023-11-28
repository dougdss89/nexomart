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
pgconn_dw = os.getenv('POSTGRE_CONN_DW')

#create session

spark = (( SparkSession
            .builder
                .appName('purchase_orderline')
                .config('spark.jars', f'{pgjar}')
                .config('spark.driver.extraClassPath', f'{pgjar}')
                .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
        ))

#getting purchaseorderline
def createpurchaseline (spark = spark, pgconn = pgconn, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    """ create purchase orderline to join with order """
    tmp_purchaseorderline = (( spark
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
                                                _airbyte_ab_id as elt_orderline_hashid,
                                                _airbyte_purchaseorderlines_hashid as orderline_hashid
                                            
                                            from purchasing.purchaseorderlines
                                        
                                        """                            
                                        )
                            )).load()
    
    return tmp_purchaseorderline

#getting purchasorder
def createpurchase_order(spark = spark, pgconn = pgconn, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    """ loading purchaseorder data to dataframe """
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
                                        expecteddeliverydate as duedate,
                                        supplierreference,
                                        isorderfinalized,
                                        _airbyte_ab_id,
                                        _airbyte_purchaseorders_hashid,
                                        _airbyte_emitted_at,
                                        _airbyte_normalized_at

                                        from purchasing.purchaseorders
                                    
                                    """)
                            )).load()
    
    return tmp_purchaseframe

#joining and renaming dataframe 
def join_order(orders = createpurchase_order(), orderline = createpurchaseline()):
    
    join_purchase = (( 
                    orders
                        .join(orderline, 'purchaseorderid', 'left')
                    ))

    #renaming columns
    #repalace true-false for 0-1
    #drop column
    purchase = (( join_purchase
                        .withColumnRenamed('purchaseorderlineid', 'orderlineid')
                        .withColumnRenamed('expectedunitpriceperouter', 'expectedunit')
                        .withColumnRenamed('receivedouters', 'qtdreceived')
                        .withColumnRenamed('orderedouters', 'qtdordered')
                        .withColumnRenamed('_airbyte_ab_id', 'elt_order_hashid')
                        .withColumnRenamed('_airbyte_purchaseorders_hashid', 'purchaseorder_hashid')
                        .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                        .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
                        
                        .withColumn('isorderfinalized', f.when(join_purchase.isorderlinefinalized == 'true', 1)
                                                            .otherwise('0'))                                                         
            ))
    
    return purchase
    
def reordercolumn(orderframe = join_order()):
    
    factpurchase = (( orderframe
                        .select('purchaseorderid',
                                'orderlineid',
                                'supplierid',
                                'stockitemid',
                                'expectedunit',
                                'qtdordered',
                                'qtdreceived',
                                'deliverymethodid',
                                'packagetypeid',
                                'supplierreference',
                                'contactpersonid',
                                'orderdate',
                                'expecteddeliverydate',
                                'lastreceiptdate',
                                'isorderfinalized',
                                'elt_order_hashid',
                                'elt_orderline_hashid',
                                'purchaseorder_hashid',
                                'orderline_hashid',
                                'elt_start',
                                'elt_end'
                            )
                ))
    
    return factpurchase

#write to table
def writeorders(data = reordercolumn(), pgconn_dw = pgconn_dw, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    """ write to fact table on data warehouse"""
    (( 
        data
            .write
            .format('jdbc')
            .option('url', pgconn_dw)
            .option('user', pguser)
            .option('password', pgpwd)
            .option('driver', pgdriver)
            .option('dbtable', 'fact.purchase')
            .save(mode= 'overwrite')
        ))
    
if __name__== '__main__':
    createpurchase_order()
    createpurchaseline()
    join_order()
    reordercolumn()
    writeorders()
    spark.stop()