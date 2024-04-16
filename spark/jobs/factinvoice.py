
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

def create_session():
    spark = (( SparkSession
                    .builder
                    .appName('factinvoice')
                    .config('spark.jars', f'{sqlserverjar}')    
                    .config('spark.driver.extraClassPath', f'{sqlserverjar}')
                    .config('spark.executor.extraClassPath', f'{sqlserverjar}')
                    .getOrCreate()
                ))
    
    return spark

def create_invoiceorder(spark = create_session(), conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """
    create invoice order to join with invoiceoderline
    """
    
    temp_invoice = (( spark
                        .read
                        .format('jdbc')
                        .option('url', f'{conn}')
                        .option('driver', f'{driver}')
                        .option('user', f'{user}')
                        .option('password', f'{pwd}')
                        .option('query', 
                                
                                """ 
                                    select
                                    
                                        sil.invoiceid,
                                        sil.invoicelineid,
                                        si.orderid,
                                        si.invoicedate,
                                        si.confirmeddeliverytime,
                                        sil.packagetypeid,
                                        si.customerid,
                                        si.accountspersonid,
                                        si.contactpersonid,
                                        si.billtocustomerid,
                                        si.packedbypersonid,
                                        si.salespersonpersonid,
                                        si.deliverymethodid,
                                        si.customerpurchaseordernumber as ordernumberserial,
                                        sil.stockitemid as productid,
                                        sil.unitprice,
                                        sil.quantity,
                                        si.totalchilleritems,
                                        sil.taxrate,
                                        sil.taxamount,
                                        sil.extendedprice,
                                        sil.lineprofit
                                        
                                    from sales.invoicelines as sil
                                    
                                    left join
                                        sales.invoices as si
                                        
                                    on sil.invoiceid = si.invoiceid
                                
                                """
                            ).load()
    ))
    
    return temp_invoice

def write_invoice(invoiceframe = create_invoiceorder(), conn_dw = mssql_dwdest, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    (( 
        invoiceframe
            .write
            .format('jdbc')
            .option('url', conn_dw)
            .option('driver', driver)
            .option('user', user)
            .option('password', pwd)
            .option('dbtable', 'fact.invoice')
            .mode('overwrite')
        .save(mode= 'overwrite')
    ))
    
if __name__== '__main__':
    
    create_session()
    create_invoiceorder()
    write_invoice()
    create_session().stop()