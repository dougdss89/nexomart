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
pgconn_dw = os.getenv('POSTGRE_CONN_DW')

#creating spark session
def createsession():
    """
        this function generates sparksession for elt task
        some improvements will be made in future
        
    Returns:
        _type_: sparksession
    """
    
    spark = (( SparkSession
                .builder
                    .appName('customers')
                    .config('spark.jar', f'{pgjar}')
                    .config('spark.driver.extraClassPath', f'{pgjar}')
                    .config('spark.executor.extraClassPath', f'{pgjar}')
                .getOrCreate()
            ))
    
    return spark

spark = createsession()

#loading customer data from source table
def loadcustomer(spark = spark, connection = pgconn, user = pguser, pwd = pgpwd, driver = pgdriver):
    
    """ this function loads data from customer table on postgre database
        and split some columns to ease data process
        
        Returns:
        A temporary dataframe with data in brute state
    """    
    tmp_customer = (( spark
                            .read
                            .format('jdbc')
                            .option('url', connection)
                            .option('driver', driver)
                            .option('user', user)
                            .option('password', pwd)
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
    
    return tmp_customer

#loading customercategory data from source table
def load_customercategory(connection = pgconn, user = pguser, pwd = pgpwd, driver = pgdriver):

    """ 
        In this function, we'll load customer category data 
        to join with customer and buying groups.
    
        This also creates store category for store dimension.
    """
    tmp_custmercategory = (( spark
                                .read
                                .format('jdbc')
                                .option('url', connection)
                                .option('driver', driver)
                                .option('user', user)
                                .option('password', pwd)
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
    return tmp_custmercategory

#loading buying groups from table
def load_buyinggroups(connection = pgconn, user = pguser, pwd = pgpwd, driver = pgdriver):

    """ 
        extract buying groups data from buying groups table on postgre database           
    """
    tmp_buyinggroups = (( spark
                            .read
                            .format('jdbc')
                            .option('url', connection)
                            .option('driver', driver)
                            .option('user', user)
                            .option('password', pwd)
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
    return tmp_buyinggroups

customer = loadcustomer()
customercategory = load_customercategory()
buyinggroups = load_buyinggroups()


#joining previous dataframes
def joining_frames(customer = customer, custcategory = customercategory, buygroups = buyinggroups):

    """
        joining customers information from customer, customercategory and buying groups dataframe 
        that has been loaded from source table
    
    Returns:
        _type_: dataframe
    """
    joined_frames = (( customer
                        .join(custcategory, 'customercategoryid', 'left')
                        .join(buyinggroups, 'buyinggroupid', 'left')
                    ))
    
    return joined_frames

data = joining_frames()

#renaming and trimming some string columns
#remove nulls 
def droprename_dataframe(dataframe = data):
    
    """
    performing cleaning and conformizing customer data

    Returns:
        _type_: _description_
    """

    renamed_df = ((
                    dataframe
                        .select('*', 
                                f.trim('firstname').alias('firstname_n'),
                                f.trim('lastnameOrbranch').alias('lastname'),
                                f.coalesce('creditlimit', f.lit(0.00)).alias('creditlimit_n'),
                                f.coalesce('buyinggroupid', f.lit(0)).alias('buyergroup_id'),
                                f.coalesce('alternatecontactpersonid', f.lit(0)).alias('alternatecontact_id')
                            )
                        
                    #drop columns that has been renamed
                    #renaming columns to data warehouse standard
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
    
    return renamed_df

customerdataframe = droprename_dataframe()

#get location for store customers
#this step we split lastname columns to get store location
def get_storelocation(dataframe = customerdataframe):
    
    """ 
    this function creates location column to store dataframe

    Returns:
        _type_: dataframe
    """

    df = dataframe.withColumn('lastname', f.split('lastname', '[^A-Za-z0-9 '']'))

    storelocation = df.withColumn('lastname', df['lastname'].getItem(0))
    
    return storelocation

customer_store = get_storelocation()

#organizing hierarchy columns
#separate store from customers
def createcustomer(dataframe = customer_store):

    """separate customer from store and create customer dataframe

    Returns:
        _type_: dataframe
    """
    customer= ((
                dataframe
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
                        .where('firstname not like "Tailspin%" and firstname not like "Wingtip%" ')
                                                
                    #change boolean for 'binary' and empty for nulls
                    #adjuste customer columns and remove nulls
                    .withColumn('isoncredithold', f.when(dataframe['isoncredithold'] == 'true', 1).otherwise(0))
                    .withColumn('isstatementsent', f.when(dataframe['isstatementsent'] == 'true', 1).otherwise(0))
                    .withColumn('buyinggroupname', f.expr('coalesce(buyinggroupname, "Personal Customer")'))
                    .withColumn('place', f.when(dataframe['place'] == "", None).otherwise(dataframe['place']))
                    
                    #normalize null values without rename column
                    .withColumn('place', f.expr('coalesce(place, "Not Available")'))
                    .withColumn('creditlimit', f.expr('cast(creditlimit as numeric(12, 2)) as creditlimit'))
                    .withColumn('discountpercentage', f.expr('cast(discountpercentage as numeric(12, 2)) as discountpercentage'))
                    .withColumn('elt_buyergroup_hashid', f.expr('coalesce(elt_buyergroup_hashid, "Not Available")'))
                    .withColumn('buyergroup_hashid', f.expr('coalesce(buyergroup_hashid, "Not Available")'))
                    .withColumnRenamed('firstname', 'customername')
                    .drop('lastname')
                ))
    
    return customer

#creating store table
def createstore(dataframe = customer_store):
    
    """ 
    creating store table from source dataframe
    this dataframe is apart from customer and generates store dimension

    Returns:
        _type_: dataframe
    """
    store = (( 
                dataframe
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
                            'buyergroup_hashid')
                        .where('firstname like "Tailspin%" or firstname like "Wingtip%"')
                    .withColumnRenamed('lastname', 'businessunity')
                ))
    
    return store

customer = createcustomer()
store = createstore()

#writing data to customer dimension
def writecustomer(data = customer, conn = pgconn, user = pguser, pwd = pgpwd, driver = pgdriver):

    ((
        data
            .write
            .format('jdbc')
                .option('url', pgconn_dw)
                .option('driver', driver)
                .option('user', user)
                .option('password', pwd)
                .option('dbtable', 'dimension.customer')
            .save(mode= 'overwrite')
    ))

#writing customer_category to dimension table
def writecustomercategory(data =customercategory,  conn = pgconn, user = pguser, pwd = pgpwd, driver = pgdriver):

    ((
        customercategory
            .write
            .format('jdbc')
                .option('url', pgconn_dw)
                .option('driver', driver)
                .option('user', user)
                .option('password', pwd)
                .option('dbtable', 'dimension.customercategory')
            .save(mode= 'overwrite')
    ))
#writing to storedimension
def writestore(data = store, conn = pgconn, user = pguser, pwd = pgpwd, driver = pgdriver):

    ((
        store
            .write
            .format('jdbc')
                .option('url', pgconn_dw)
                .option('driver', driver)
                .option('user', user)
                .option('password', pwd)
                .option('dbtable', 'dimension.store')
            .save(mode= 'overwrite')
    ))
    
if __name__ == '__main__':
    
    createsession()
    loadcustomer()
    load_customercategory()
    load_buyinggroups()
    joining_frames()
    droprename_dataframe()
    get_storelocation()
    createcustomer()
    createstore()
    writecustomer()
    writecustomercategory()
    writestore()
    spark.stop()