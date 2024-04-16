
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


def sparksession():
    
    spark = (( SparkSession
                .builder
                .appName('supplier')
                .config('spark.jar', f'{sqlserverjar}')
                .config('spark.driver.extraClassPath', f'{sqlserverjar}')
                .config('spark.executor.extraClassPath', f'{sqlserverjar}')
                .getOrCreate()
            ))
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


spark = sparksession()



#loding data from source table
def createsupplier(spark = spark, conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """
        create supplier dimension table
    """
    tmp_supplier = (( spark
                        .read
                        .format('jdbc')
                        .option('url', conn)
                        .option('driver', driver)
                        .option('user', user)
                        .option('password', pwd)
                        .option('query', 
                                """ 
                                    select
                                    
                                        supplierid,
                                        suppliercategoryid,
                                        deliverymethodid,
                                        supplierreference,
                                        bankinternationalcode,
                                        suppliername,
                                        bankaccountname,
                                        bankaccountbranch,
                                        paymentdays,
                                        primarycontactpersonid,
                                        alternatecontactpersonid,
                                        deliverycityid,
                                        postalcityid,
                                        deliverypostalcode,
                                        deliveryaddressline1,
                                        deliveryaddressline2
                                        
                                    from purchasing.suppliers
                                
                                """))
                    ).load()

    return tmp_supplier

#loading supplier category data from source table to join with supplier
def createsupplier_category(spark = spark, conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """ create supplier category dimension  to join with supplier """
    tmp_categorysupplier = (( spark
                                .read
                                .format('jdbc')
                                .option('url', conn)
                                .option('driver', driver)
                                .option('user', user)
                                .option('password', pwd)
                                .option('query', 
                                        
                                        """ 
                                            select 
                                            
                                                suppliercategoryid,
                                                suppliercategoryname
                                            
                                            from purchasing.suppliercategories
                                        
                                        """
                                        )
                            )).load()
    
    return tmp_categorysupplier

#joining dataframes
def joinsupplier(supplier = createsupplier(), category=createsupplier_category()):
    
    """
        joining by suppliercategoryid
        since columns with datetime information about elt have the same value,
        i'll keep them out of this dataframe
    """
    join_supplier = (( supplier
                        .join(category,
                            'suppliercategoryid',
                            'left')
                    ))

    return join_supplier

#removing nulls
#fill empty values
def fillrenameframe(dataframe = joinsupplier()):
    
    """
        adjust dataframe and normalize data
        renaming and drop temp columns
    """
    renamed_column = (( dataframe
                        .select('*',
                                    f.coalesce('deliverymethodid', f.lit(999))
                                        .alias('deliverymethodid_n'),
                                    
                                    #adjust some fields that's empty
                                    f.when(dataframe.deliveryaddressline1 == '', 'Unknown')
                                    .otherwise(dataframe.deliveryaddressline1)
                                        .alias('deliveryaddress_n'),
                                        
                                    #removing comma character from column
                                    f.regexp_replace('suppliername', '[^A-Za-z0-9]', ' ')
                                        .alias('suppliername_n')
                                )
                    ))

    #rename and drop columns
    supplier_frame = (( renamed_column
                        .drop('deliverymethodid')
                        .drop('deliveryaddressline1')
                        .drop('suppliername')
                        
                        #renaming column
                        .withColumnRenamed('deliverymethodid_n', 'deliverymethodid')
                        .withColumnRenamed('deliveryaddress_n', 'deliveryaddress')
                        .withColumnRenamed('suppliername_n', 'suppliername')
                        .withColumnRenamed('supplierreference', 'suppliercode')
                        .withColumnRenamed('primarycontactpersonid', 'contact_id')
                        .withColumnRenamed('alternatecontactpersonid', 'submanager_contactid')
                        .withColumnRenamed('bankaccountname', 'bankname')
                        .withColumnRenamed('bankaccountbranch', 'bank_branch')
                        .withColumnRenamed('deliverypostalcode', 'postalcode')
                        .withColumnRenamed('deliveryaddressline2', 'optional_address')
                        .withColumnRenamed('deliverycityid', 'city_id')
                    ))
    
    return supplier_frame

#reorganizing columns

def organizecolumn(sourcedata = fillrenameframe()):
    
    """ organize columns
    """
    
    insert_supplier = (( sourcedata
                        .select(
                                'supplierid',
                                'suppliercategoryid',
                                'suppliercode',
                                'deliverymethodid',
                                'suppliername',
                                'bankinternationalcode',
                                'bankname',
                                'bank_branch',
                                'contact_id',
                                'submanager_contactid',
                                'paymentdays',
                                'city_id',
                                'postalcode',
                                'deliveryaddress',
                                'optional_address'
                                )
                        ))
    
    return insert_supplier

#write to table
def writesupplierdimension(supplier_dimension= organizecolumn(), conn_dw = mssql_dwdest, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    (( supplier_dimension
        .write
            .format('jdbc')
            .option('url', conn_dw)
            .option('driver', driver)
            .option('user', user)
            .option('password', pwd)
            .option('dbtable', 'dimension.supplier')
        .save(mode= 'overwrite')
    ))
    
if __name__ == '__main__':
    
    createsupplier()
    createsupplier_category()
    fillrenameframe()
    joinsupplier()
    organizecolumn()
    writesupplierdimension()
    spark.stop()