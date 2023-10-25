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

spark = (( SparkSession
            .builder
            .appName('supplier')
            .config('spark.jar', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
        ))

#loding data from source table
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

#loading supplier category data from source table to join with supplier
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

#joining dataframes
join_supplier = (( tmp_supplier
                  
                  #joining by suppliercategoryid
                  #since columns with datetime information about elt have the same value,
                  #i'll keep them out of this dataframe
                  
                    .join(tmp_categorysupplier,
                          'suppliercategoryid',
                          'left')
                  ))

#removing nulls
#fill empty values
renamed_column = (( join_supplier
                    .select('*',
                                f.coalesce('deliverymethodid', f.lit(999))
                                    .alias('deliverymethodid_n'),
                                
                                #adjust some fields that's empty
                                f.when(join_supplier.deliveryaddressline1 == '', 'Unknown')
                                 .otherwise(join_supplier.deliveryaddressline1)
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

#reorganizing columns
insert_supplier = (( supplier_frame
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

#write to table
(( insert_supplier
    .write
        .format('jdbc')
        .option('url', pgconn)
        .option('driver', pgdriver)
        .option('user', pguser)
        .option('password', pgpwd)
        .option('dbtable', 'purchasing.testeinsert')
    .save(mode= 'overwrite')
))