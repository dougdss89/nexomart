#packages and modules
#load env-var

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os

from dotenv import load_dotenv

env = load_dotenv('/mnt/d/linux/project/vars/.env')

#environment variables
pgconn = os.getenv('POSTGRE_CONN')
pgdriver = os.getenv('PG_DRIVER')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')
pgjar = os.getenv('POSTGRE_JAR')


#creating spark session
spark = (( SparkSession
            .builder
                .appName('customers')
                .config('spark.jar', f'{pgjar}')
                .config('spark.driver.extraClassPath', f'{pgjar}')
                .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
        ))

#load customer data from source table

tmp_renamedrop_df = (( spark
                        .read
                        .format('jdbc')
                        .option('url', pgconn)
                        .option('driver', pgdriver)
                        .option('user', pguser)
                        .option('password', pgpwd)
                        .option('query', 
                                """ 
                                
                                    select

                                        customerid,
                                        customercategoryid,
                                        buyinggroupid,
                                        billtocustomerid,
                                        split_part(customername, '(', 1)as firstname,
		                                replace(split_part(customername, '(', 2), ')', '') as lastnameOrBranch,
                                        replace(split_part(customername, ',', 2),')', '') as place,
                                        accountopeneddate,
                                        creditlimit,
                                        standarddiscountpercentage,
                                        paymentdays,
                                        primarycontactpersonid,
                                        alternatecontactpersonid,
                                        deliverymethodid,
                                        deliverycityid,
                                        postalcityid,
                                        deliverypostalcode,
                                        deliveryaddressline1,
                                        deliveryaddressline2,
                                        postaladdressline1,
                                        postaladdressline2,
                                        isstatementsent,
                                        isoncredithold,
                                        _airbyte_ab_id,
                                        _airbyte_customers_hashid,
                                        _airbyte_emitted_at,
                                        _airbyte_normalized_at
                                        
                                    from sales.customers
                                
                                """)
                        
                        #this column has been renamed to avoid conflict ambiguity
                    )).load().withColumnRenamed('_airbyte_ab_id', 'customer_elt_hashid')


#load customercategory data from source table
tmp_custmercategory = (( spark
                            .read
                            .format('jdbc')
                            .option('url', pgconn)
                            .option('driver', pgdriver)
                            .option('user', pguser)
                            .option('password', pgpwd)
                            .option('query', 
                                    
                                    """ 
                                        select

                                            customercategoryid,
                                            customercategoryname,
                                            _airbyte_ab_id,
                                            _airbyte_customercategories_hashid
                                        
                                        from sales.customercategories
                                        
                                    """)
                        )).load().withColumnRenamed('_airbyte_ab_id', 'custcategory_elt_hashid')

#load buying groups from table
#this table contains information about buyer groups that customers has been categorized
tmp_buyinggroups = (( spark
                        .read
                        .format('jdbc')
                        .option('url', pgconn)
                        .option('driver', pgdriver)
                        .option('user', pguser)
                        .option('password', pgpwd)
                        .option('query', 
                                
                                """
                                    select
                                    
                                        buyinggroupid,
                                        buyinggroupname,
                                        _airbyte_ab_id,
                                        _airbyte_buyinggroups_hashid
                                        
                                    from sales.buyinggroups
                                        
                                """)
                        )).load().withColumnRenamed('_airbyte_ab_id', 'custgroup_elt_hashid')

#joining previous dataframes

joined_frames = (( tmp_renamedrop_df
                    .join(tmp_custmercategory, 'customercategoryid', 'left')
                    .join(tmp_buyinggroups, 'buyinggroupid', 'left')
                ))

#renaming and trimming some string columns
#remove nulls 
rename_df = ((
                joined_frames
                    .select('*', 
                            f.trim('firstname').alias('firstname_n'),
                            f.trim('lastnameOrbranch').alias('lastname'),
                            f.coalesce('creditlimit', f.lit(0.00)).alias('creditlimit_n'),
                            f.coalesce('buyinggroupid', f.lit(0)).alias('buyergroup_id'),
                            f.coalesce('alternatecontactpersonid', f.lit(0)).alias('alternatecontact_id')
                        )
))

#drop columns that has been renamed
#renaming columns to data warehouse standard
renamedrop_df = ((rename_df
                     .drop('creditlimit')
                     .drop('buyinggroupid')
                     .drop('alternatecontactpersonid')
                     .drop('firstname')
                     .drop('lastnameOrBranch')
                     
                     #rename columns
                     .withColumnRenamed('creditlimit_n', 'creditlimit')
                     .withColumnRenamed('buyergroup_id', 'buyergroupid')
                     .withColumnRenamed('alternatecontact_id', 'alternatecontactid')
                     .withColumnRenamed('standarddiscountpercentage', 'discountpercentage')
                     .withColumnRenamed('primarycontactpersonid', 'primarycontactid')
                     .withColumnRenamed('deliverypostalcode', 'postalcode')
                     .withColumnRenamed('deliveryaddressline1', 'primaryaddress')
                     .withColumnRenamed('deliveryaddressline2', 'alternateaddress')
                     .withColumnRenamed('firstname_n', 'firstname')
                     .withColumnRenamed('postaladdressline1', 'postaladdress')
                     .withColumnRenamed('postaladdressline2', 'alternatepostaladdress')
                     .withColumnRenamed('_airbyte_customers_hashid', 'customers_hashid')
                     .withColumnRenamed('customer_elt_hashid', 'elt_customer_hashid')
                     .withColumnRenamed('_airbyte_customercategories_hashid', 'customercategory_hashid')
                     .withColumnRenamed('custcategory_elt_hashid', 'elt_custcategory_hashid')
                     .withColumnRenamed('_airbyte_buyinggroups_hashid', 'elt_buyergroup_hashid')
                     .withColumnRenamed('custgroup_elt_hashid', 'buyergroup_hashid')
                     .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                     .withColumnRenamed('_airbyte_normalized_at', 'elt_finalized')
))

#get location for store customers
df = renamedrop_df.withColumn('lastname', f.split('lastname', '[^A-Za-z0-9 '']'))

renamedrop_df = df.withColumn('lastname', df['lastname'].getItem(0))

#organizing hierarchy columns
customerframe= ((
                renamedrop_df
                    .select('customerid',
                            'customercategoryid',
                            'billtocustomerid',
                            'buyergroupid',
                            'firstname',
                            'lastname',
                            'customercategoryname',
                            'buyinggroupname',
                            'accountopeneddate',
                            'paymentdays',
                            'creditlimit',
                            'discountpercentage',
                            'primarycontactid',
                            'alternatecontactid',
                            'deliverymethodid',
                            'deliverycityid',
                            'postalcityid',
                            'postalcode',
                            'primaryaddress',
                            'alternateaddress',
                            'postaladdress',
                            'alternatepostaladdress',
                            'place',
                            'isoncredithold',
                            'isstatementsent',
                            'elt_customer_hashid',
                            'customers_hashid',
                            'elt_custcategory_hashid',
                            'customercategory_hashid',
                            'elt_buyergroup_hashid',
                            'buyergroup_hashid'
                            )
                    
                    #change boolean for 'binary'
                    .withColumn('isoncredithold', f.when(renamedrop_df['isoncredithold'] == 'true', 1).otherwise(0))
                    .withColumn('isstatementsent', f.when(renamedrop_df['isstatementsent'] == 'true', 1).otherwise(0))
        
))

#write data on datawarehouse
((
    customerframe
        .write
        .format('jdbc')
            .option('url', pgconn)
            .option('driver', pgdriver)
            .option('user', pguser)
            .option('password', pgpwd)
            .option('dbtable', 'sales.testeinsert')
        .save(mode= 'overwrite')
))