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
pg_conn_dw = getenv('POSTGRE_CONN_DW')

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
def createorders(spark = spark, conn = pgconn, driver = pgdriver, user = pguser, pwd = pgpwd):
    
    """ Load orders from orders table 
        to join with orderline
    """
    tmp_orders = (( spark
                    .read
                    .format('jdbc')
                    .option('url', conn)
                    .option('driver', driver)
                    .option('user', user)
                    .option('password', pwd)
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
    
    return tmp_orders


#importing orderline from table
def createorder_line(spark = spark, conn = pgconn, driver = pgdriver, user = pguser, pwd = pgpwd):
    
    """ import orderline data from orderline table
        to join with orders.
    """
    tmp_orderline = (( spark
                        .read
                        .format('jdbc')
                        .option('url', conn)
                        .option('driver', driver)
                        .option('user', user)
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
    
    return tmp_orderline

#joining tables 
orders = createorders()
orderlines = createorder_line()

def cleaning_join(order = orders, orderlines = orderlines):
    
    """
    joining and cleaning orders table 
    to generate salesorder dimension

    Returns:
        _type_: dataframe
    """
    join_order = (( order
                    .join(orderlines, 'orderid', 'left')
                ))


    #adjust hierarchy columns
    #removing nulls
    #changing datetime to date
    salesorder = (( join_order
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
                .withColumnRenamed('orderid', 'salesorderid')
                .withColumnRenamed('orderlineid', 'salesorderlineid')
                
                .withColumnRenamed('stockitemid', 'productid')
                .withColumn('pickingcompletedwhen', f.expr('cast(pickingcompletedwhen as timestamp)'))
                                                    .withColumnRenamed('pickingcompletedwhen', 'receivedorder')
                                                    
                .withColumn('processedtime', f.expr('cast(processedtime as timestamp)'))
                                            .withColumnRenamed('processedtime', 'orderfinalized')
                
                .withColumn('pickedbypersonid', f.expr('coalesce(pickedbypersonid, 0)'))
                
                .withColumn('orderdate', f.expr('cast(orderdate as date)'))
                
                .withColumn('expecteddeliverydate', f.expr('cast(expecteddeliverydate as date)'))
                                            .withColumnRenamed('expecteddeliverydate', 'duedate')
                
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
    
    return salesorder

salesorders = cleaning_join()

#write data to table
def writesales_order(dw_conn = pg_conn_dw, driver = pgdriver, user= pguser, pwd = pgpwd):
    
    """ writing data on salesorder dimension"""
    ((
        salesorders
            .write
            .format('jdbc')
                .option('url', dw_conn)
                .option('driver', driver)
                .option('user', user)
                .option('password', pwd)
                .option('dbtable', 'fact.sales')
            .save(mode= 'overwrite')
    ))
    
    return "Data was loaded successfully"

if __name__=='__main__':
    createorders()
    createorder_line()
    cleaning_join()
    writesales_order()
    spark.stop()