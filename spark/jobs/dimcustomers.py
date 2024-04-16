#importing packages and vars

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os
from dotenv import load_dotenv

#load environment variables
env = load_dotenv(dotenv_path='/mnt/d/linux/project/vars/.env')

#env vars for postgresql connection
mssqlsource = os.getenv('MSSQLSOURCE')
mssqluser = os.getenv('MSSQL_SPARK_USER')
mssqlpwd = os.getenv('MSSQL_SPARK_PWD')
mssqldriver = os.getenv('MSSQL_DRIVER')
sqlserverjar = os.getenv('SQLSERVER_JAR')
mssql_dwdest = os.getenv('MSSQL_CONN')

#creating spark session
def createsession():
    """
        this function generates sparksession for elt task
        some improvements will be made in future
        
    Returns:
        _type_: sparksession
        """
    spark = ((SparkSession
                .builder
                .appName('dimcountry')
                .config('spark.jar', f'{sqlserverjar}')
                .config('spark.driver.extraClassPath', f'{sqlserverjar}')
                .config('spark.executor.extraClassPath', f'{sqlserverjar}')
                .getOrCreate()
                
            ))
    
    return spark

spark = createsession()

#loading customer data from source table
def loadcustomer(spark = spark, connection = mssqlsource, user = mssqluser, pwd = mssqlpwd, driver = mssqldriver):
    
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
                                            isoncredithold
                                            
                                        from sales.customers
                                    
                                    """)
                            
                            #this column has been renamed to avoid conflict ambiguity
                        )).load()
    
    return tmp_customer

#loading customercategory data from source table
def load_customercategory(connection = mssqlsource, user = mssqluser, pwd = mssqlpwd, driver = mssqldriver):

    """ 
        In this function, we'll load customer category data 
        to join with customer and buying groups.
    
        This also creates store category for store dimension.
    """
#load customercategory data from source table
    tmp_custmercategory = (( spark
                            .read
                            .format('jdbc')
                            .option('url', mssqlsource)
                            .option('driver', mssqldriver)
                            .option('user', mssqluser)
                            .option('password', mssqlpwd)
                            .option('query', 
                                    
                                    """ 
                                        select

                                            customercategoryid,
                                            customercategoryname
                                        
                                        from sales.customercategories
                                        
                                    """)
                        )).load()
    
    return tmp_custmercategory

#loading buying groups from table
def load_buyinggroups(connection = mssqlsource, user = mssqluser, pwd = mssqlpwd, driver = mssqldriver):

    """ 
        extract buying groups data from buying groups table on postgre database           
    """
#load customercategory data from source table
    tmp_buyinggroups = (( spark
                        .read
                        .format('jdbc')
                        .option('url', mssqluser)
                        .option('driver', mssqldriver)
                        .option('user', mssqluser)
                        .option('password', mssqlpwd)
                        .option('query', 
                                
                                """
                                    select
                                    
                                        buyinggroupid,
                                        buyinggroupname
                                        
                                    from sales.buyinggroups
                                        
                                """)
                        )).load()
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
                        .join(buygroups, 'buyinggroupid', 'left')
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
                            'isstatementsent'
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
                            'isstatementsent')
                        .where('firstname like "Tailspin%" or firstname like "Wingtip%"')
                    .withColumnRenamed('lastname', 'businessunity')
                ))
    
    return store

customer = createcustomer()
store = createstore()

#writing data to customer dimension
def writecustomer(data = customer, conn = mssql_dwdest, user = mssqluser, pwd = mssqlpwd, driver = mssqldriver):

    ((
        data
            .write
            .format('jdbc')
                .option('url', conn)
                .option('driver', driver)
                .option('user', user)
                .option('password', pwd)
                .option('dbtable', 'dimension.customer')
            .save(mode= 'overwrite')
    ))

#writing customer_category to dimension table
def writecustomercategory(data =customercategory, conn = mssql_dwdest, user = mssqluser, pwd = mssqlpwd, driver = mssqldriver):

    ((
        data
            .write
            .format('jdbc')
                .option('url', conn)
                .option('driver', driver)
                .option('user', user)
                .option('password', pwd)
                .option('dbtable', 'dimension.customercategory')
            .save(mode= 'overwrite')
    ))
#writing to storedimension
def writestore(data = store, conn = mssql_dwdest, user = mssqluser, pwd = mssqlpwd, driver = mssqldriver):

    ((
        data
            .write
            .format('jdbc')
                .option('url', conn)
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