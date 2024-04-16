
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
                    .appName('purchase_orderline')
                    .config('spark.jars', f'{sqlserverjar}')
                    .config('spark.driver.extraClassPath', f'{sqlserverjar}')
                    .config('spark.executor.extraClassPath', f'{sqlserverjar}')
                .getOrCreate()
            ))
    
    return spark

spark = sparksession()

#getting purchaseorderline
def createpurchaseline (spark = sparksession(), conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """ create purchase orderline to join with order """
    tmp_purchaseorderline = (( spark
                                .read
                                .format('jdbc')
                                .option('url', conn)
                                .option('driver', driver)
                                .option('user', user)
                                .option('password', pwd)
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
                                                lastreceiptdate
                                            
                                            from purchasing.purchaseorderlines
                                        
                                        """                            
                                        )
                            )).load()
    
    return tmp_purchaseorderline

#getting purchasorder
def createpurchase_order(spark = sparksession(), conn = mssqlsource , driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """ loading purchaseorder data to dataframe """
    tmp_purchaseframe = (( 
                        spark
                            .read
                            .format('jdbc')
                            .option('url', conn)
                            .option('driver', driver)
                            .option('user', user)
                            .option('password', pwd)
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
                                        isorderfinalized

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
                                'duedate',
                                'lastreceiptdate',
                                'isorderfinalized'
                            )
                ))
    
    return factpurchase

#write to table
def writeorders(data = reordercolumn(), conn_dw = mssql_dwdest, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """ write to fact table on data warehouse"""
    (( 
        data
            .write
            .format('jdbc')
            .option('url', conn_dw)
            .option('user', user)
            .option('password', pwd)
            .option('driver', driver)
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