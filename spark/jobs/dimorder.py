#importing packages 
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

from os import getenv
from dotenv import load_dotenv

env= load_dotenv('/mnt/d/linux/project/vars/.env')

#env variables
pgconn = getenv('POSTGRE_CONN')
pgdriver = getenv('PG_DRIVER')
pgjar = getenv('POSTGRE_JAR')
pguser = getenv('POSTGRE_USER')
pgpwd = getenv('POSTGRE_PWD')

#create session
spark = (( SparkSession
            .builder
            .appName('orders')
            .config('spark.jars', pgjar)
            .config('spark.driver.extraClassPath', pgjar)
            .config('spark.executor.extraClassPath', pgjar)
            .getOrCreate()
        ))

#importing order table from source
tmp_orders = (( spark
                .read
                .format('jdbc')
                .option('url', pgconn)
                .option('driver', pgdriver)
                .option('user', pguser)
                .option('password', pgpwd)
                .option('query', 
                        """
                            select
                            
                                orderid,
                                customerid,
                                salespersonpersonid,
                                customerpurchaseordernumber,
                                pickedbypersonid,
                                backorderorderid,
                                contactpersonid,
                                orderdate,
                                expecteddeliverydate,
                                pickingcompletedwhen as processedtime,
                                _airbyte_ab_id,
                                _airbyte_orders_hashid,
                                _airbyte_emitted_at,
                                _airbyte_normalized_at
                                
                            from sales.orders
                            
                        """)
            )).load().withColumnRenamed('_airbyte_ab_id', 'elt_orders_hashid')

#import orderline from table
tmp_orderline = (( spark
                    .read
                    .format('jdbc')
                    .option('url', pgconn)
                    .option('driver', pgdriver)
                    .option('user', pguser)
                    .option('password', pgpwd)
                    .option('query', 
                            
                            """
                                select

                                    orderid,
                                    orderlineid,
                                    stockitemid,
                                    packagetypeid,
                                    quantity,
                                    pickedquantity,
                                    unitprice,
                                    taxrate,
                                    pickingcompletedwhen,
                                    _airbyte_ab_id,
                                    _airbyte_orderlines_hashid
                                    
                                from sales.orderlines
                            
                            """)
                )).load().withColumnRenamed('_airbyte_ab_id', 'elt_orderline_hashid')

#joining tables 
join_order = (( tmp_orders
                .join(tmp_orderline, 'orderid', 'left')
            ))


#adjust hierarchy columns
#removing nulls
#changing datetime to date
orders = (( join_order
            .select('orderid',
                    'orderlineid',
                    'customerpurchaseordernumber',
                    'stockitemid',
                    'customerid',
                    'salespersonpersonid',
                    'packagetypeid',
                    'pickedbypersonid',
                    'backorderorderid',
                    'contactpersonid',
                    'orderdate',
                    'expecteddeliverydate',
                    'pickingcompletedwhen',
                    'processedtime',
                    'quantity',
                    'pickedquantity',
                    'unitprice',
                    'taxrate',
                    'elt_orderline_hashid',
                    'elt_orders_hashid',
                    '_airbyte_orders_hashid',
                    '_airbyte_orderlines_hashid',
                    '_airbyte_emitted_at',
                    '_airbyte_normalized_at'
                    )
            
            #cleaning and adjust datatype
            .withColumn('pickingcompletedwhen', f.expr('cast(pickingcompletedwhen as timestamp)'))
                                                .withColumnRenamed('pickingcompletedwhen', 'orderstart')
                                                
            .withColumn('processedtime', f.expr('cast(processedtime as timestamp)'))
                                        .withColumnRenamed('processedtime', 'orderfinalized')
            
            .withColumn('pickedbypersonid', f.expr('coalesce(pickedbypersonid, 0)'))
            
            .withColumn('orderdate', f.expr('cast(orderdate as date)'))
            
            .withColumn('expecteddeliverydate', f.expr('cast(expecteddeliverydate as date)'))
                                        .withColumnRenamed('expecteddeliverydate', 'deliverydate')
            
            .withColumn('backorderorderid', f.expr('coalesce(backorderorderid, 0)'))
            
            .withColumn('unitprice', f.expr('cast(unitprice as decimal(12, 2))'))
            
            .withColumn('taxrate', f.expr('cast(taxrate as decimal(5, 2))'))
            
            .withColumn('_airbyte_emitted_at', f.expr('cast(_airbyte_emitted_at as date)'))
                                                .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
            
            .withColumn('_airbyte_normalized_at', f.expr('cast(_airbyte_normalized_at as date)'))
                                                .withColumnRenamed('_airbyte_normalized_at', 'elt_end')

              
            #renaming columns..                                  
            .withColumnRenamed('_airbyte_orders_hashid', 'orders_hashid')
            
            .withColumnRenamed('_airbyte_orderlines_hashid', 'orderlines_hashid')
            .withColumnRenamed('customerpurchaseordernumber', 'customerpurchaseorder')

        ))

#write data to table
((
    orders
        .write
        .format('jdbc')
            .option('url', pgconn)
            .option('driver', pgdriver)
            .option('user', pguser)
            .option('password', pgpwd)
            .option('dbtable', 'sales.testeinsert')
        .save(mode= 'overwrite')
))