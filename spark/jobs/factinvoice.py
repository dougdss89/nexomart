
import pyspark.sql.functions as f
from pyspark.sql.session import SparkSession

import os
from dotenv import load_dotenv

#load env-var
env= load_dotenv('/mnt/d/linux/project/vars/.env')

pgconn = os.getenv('POSTGRE_CONN')
pgdriver = os.getenv('PG_DRIVER')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')
pgconn_dw = os.getenv('POSTGRE_CONN_DW')
pgjar = os.getenv('POSTGRE_JAR')

#create session

def create_session():
    spark = (( SparkSession
                    .builder
                    .appName('factinvoice')
                    .config('spark.jars', f'{pgjar}')    
                    .config('spark.driver.extraClassPath', f'{pgjar}')
                    .config('spark.executor.extraClassPath', f'{pgjar}')
                    .getOrCreate()
                ))
    
    return spark

def create_invoiceorder(spark = create_session(), pgconn = pgconn, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    """
    create invoice order to join with invoiceoderline
    """
    
    temp_invoice = (( spark
                        .read
                        .format('jdbc')
                        .option('url', f'{pgconn}')
                        .option('driver', f'{pgdriver}')
                        .option('user', f'{pguser}')
                        .option('password', f'{pgpwd}')
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
                                        sil.lineprofit,
                                        si._airbyte_ab_id as invoice_elt_hashid,
                                        sil._airbyte_ab_id as invoiceline_elt_hashid,
                                        si._airbyte_invoices_hashid,
                                        sil._airbyte_invoicelines_hashid,
                                        si._airbyte_normalized_at,
                                        si._airbyte_emitted_at
                                        
                                    from sales.invoicelines as sil
                                    left join
                                        sales.invoices as si
                                    on sil.invoiceid = si.invoiceid
                                
                                """
                            ).load()
    ))
    
    return temp_invoice

def write_invoice(invoiceframe = create_invoiceorder(), pgconn_dw = pgconn_dw, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    (( 
        invoiceframe
            .write
            .format('jdbc')
            .option('url', pgconn_dw)
            .option('driver', pgdriver)
            .option('user', pguser)
            .option('password', pgpwd)
            .option('dbtable', 'fact.invoice')
            .mode('overwrite')
        .save(mode= 'overwrite')
    ))
    
if __name__== '__main__':
    
    create_session()
    create_invoiceorder()
    write_invoice()
    create_session().stop()