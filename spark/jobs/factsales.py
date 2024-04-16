#importing packages 

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os
from dotenv import load_dotenv

env = load_dotenv(dotenv_path='/mnt/d/linux/project/vars/.env')

#env vars for postgresql connection
mssqlsource = os.getenv('MSSQLSOURCE')
mssqluser = os.getenv('MSSQL_SPARK_USER')
mssqlpwd = os.getenv('MSSQL_SPARK_PWD')
mssqldriver = os.getenv('MSSQL_DRIVER')
sqlserverjar = os.getenv('SQLSERVER_JAR')
mssql_dwdest = os.getenv('MSSQL_CONN')


#create session

def sparksession():
    
    spark = (( SparkSession
                .builder
                .appName('orders')
                .config('spark.jars', sqlserverjar)
                .config('spark.driver.extraClassPath', sqlserverjar)
                .config('spark.executor.extraClassPath', sqlserverjar)
                .getOrCreate()
            ))
    
    return spark

spark = sparksession()

#importing order table from source
def createorders(spark = sparksession(), conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
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
                                    pickingcompletedwhen as processedtime
                                    
                                from sales.orders
                                
                            """)
                )).load()
    
    return tmp_orders


#importing orderline from table
def createorder_line(spark = spark, conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """ import orderline data from orderline table
        to join with orders.
    """
    tmp_orderline = (( spark
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
                                        orderlineid,
                                        stockitemid,
                                        packagetypeid,
                                        quantity,
                                        pickedquantity,
                                        unitprice,
                                        taxrate,
                                        pickingcompletedwhen
                                        
                                    from sales.orderlines
                                
                                """)
                    )).load()
    
    return tmp_orderline

#joining tables 

def cleaning_join(order = createorders(), orderlines = createorder_line()):
    
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
                        'taxrate'
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
                
                .withColumnRenamed('customerpurchaseordernumber', 'customerpurchaseorder')

            ))
    
    return salesorder

salesorders = cleaning_join()

#write data to table
def writesales_order(data = cleaning_join(), dw_conn = mssql_dwdest, driver = mssqldriver, user= mssqluser, pwd = mssqlpwd):
    
    """ writing data on salesorder dimension"""
    ((
        data
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