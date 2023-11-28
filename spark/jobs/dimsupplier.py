#packages

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os
from dotenv import load_dotenv

env = load_dotenv('/mnt/d/linux/project/vars/.env')

pgconn = os.getenv('POSTGRE_CONN')
pgdriver = os.getenv('PG_DRIVER')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')
pgjar = os.getenv('POSTGRE_JAR')
pgconn_dw = os.getenv('POSTGRE_CONN_DW')

spark = (( SparkSession
            .builder
            .appName('supplier')
            .config('spark.jar', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
        ))

#loding data from source table
def createsupplier(spark = spark, pgconn = pgconn, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    """
        create supplier dimension table
    """
    tmp_supplier = (( spark
                        .read
                        .format('jdbc')
                        .option('url', pgconn)
                        .option('driver', pgdriver)
                        .option('user', pguser)
                        .option('password', pgpwd)
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
                                        deliveryaddressline2,
                                        _airbyte_ab_id as supplier_elt_hashid,
                                        _airbyte_suppliers_hashid,
                                        _airbyte_emitted_at,
                                        _airbyte_normalized_at
                                        
                                    from purchasing.suppliers
                                
                                """))
                    ).load()

    return tmp_supplier

#loading supplier category data from source table to join with supplier
def createsupplier_category(spark = spark, pgconn = pgconn, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    """ create supplier category dimension  to join with supplier """
    tmp_categorysupplier = (( spark
                                .read
                                .format('jdbc')
                                .option('url', pgconn)
                                .option('driver', pgdriver)
                                .option('user', pguser)
                                .option('password', pgpwd)
                                .option('query', 
                                        
                                        """ 
                                            select 
                                            
                                                suppliercategoryid,
                                                suppliercategoryname,
                                                _airbyte_ab_id as supcategory_elt_hashid,
                                                _airbyte_suppliercategories_hashid
                                            
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
                        .withColumnRenamed('_airbyte_suppliers_hashid', 'suppliers_hashid')
                        .withColumnRenamed('_airbyte_suppliercategories_hashid', 'suppliercategory_hashid')
                        .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                        .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
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
                                'optional_address',
                                'supplier_elt_hashid',
                                'supcategory_elt_hashid',
                                'suppliers_hashid',
                                'suppliercategory_hashid',
                                'elt_start',
                                'elt_end'
                                )
                        ))
    
    return insert_supplier

#write to table
def writesupplierdimension(supplier_dimension= organizecolumn(), pgconn_dw = pgconn_dw, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    (( supplier_dimension
        .write
            .format('jdbc')
            .option('url', pgconn_dw)
            .option('driver', pgdriver)
            .option('user', pguser)
            .option('password', pgpwd)
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