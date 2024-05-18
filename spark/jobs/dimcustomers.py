
#loading customer data from source table
#importing packages and vars

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
    tmp_store = (( spark
                            .read
                            .format('jdbc')
                            .option('url', connection)
                            .option('driver', driver)
                            .option('user', user)
                            .option('password', pwd)
                            .option('query', 
                                    """ 

                                        SELECT
                                            ss.customerid					,
                                            ss.firstname					,
                                            ss.lastnameOrBranch				,
                                            ss.place						,
                                            ss.customercategoryid			,
                                            ss.buyinggroupid				,
                                            ss.billtocustomerid				,
                                            ss.accountopeneddate			,
                                            ss.creditlimit					,
                                            ss.standarddiscountpercentage	,
                                            ss.paymentdays					,
                                            ss.primarycontactpersonid		,
                                            ss.alternatecontactpersonid		,
                                            ss.deliverymethodid				,
                                            ss.deliverycityid				,
                                            ss.postalcityid					,
                                            ss.deliverypostalcode			,
                                            ss.deliveryaddressline1			,
                                            ss.deliveryaddressline2			,
                                            ss.postaladdressline1			,
                                            ss.postaladdressline2			,
                                            ss.isstatementsent				,
                                            ss.isoncredithold					
                                        FROM Sales.sparkstore as ss

                                            left join
                                                sales.sparkcustomer as sc
                                        on ss.buyinggroupid = sc.buyinggroupid
                                    """)
                            
                        )).load()
    
    return tmp_store

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
                            .option('url', connection)
                            .option('driver', driver)
                            .option('user', user)
                            .option('password', pwd)
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
                        .option('url', connection)
                        .option('driver', driver)
                        .option('user', user)
                        .option('password', pwd)
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



#renaming and trimming some string columns
#remove nulls 
def renameadjust_column(data = joining_frames()):

    rename_df = ((
                    data
                        .select('*', 
                                f.trim('firstname').alias('firstname_n'),
                                f.trim('lastnameOrbranch').alias('lastname'),
                                f.coalesce('creditlimit', f.lit(0.00)).alias('creditlimit_n'),
                                f.coalesce('buyinggroupid', f.lit(0)).alias('buyergroup_id'),
                                f.coalesce('alternatecontactpersonid', f.lit(0)).alias('alternatecontact_id')
                            )
    ))

    tmp_customerframe = ((rename_df
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
    
    return tmp_customerframe

def store_splitname(data = renameadjust_column()):
    
    df = data.withColumn('lastname', f.split('lastname', '[^A-Za-z0-9 '']'))

    tmpstore = df.withColumn('lastname', df['lastname'].getItem(0))
    
    storeframe= ((
                tmpstore
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
                            ).where('firstname like "Tailspin%" or firstname like "Wingtip%" ')
                    .withColumn('creditlimit', f.expr('cast(creditlimit as decimal(12,2))'))
                ))
    return storeframe

def cleanfirstlastname_customer(data = renameadjust_column()):
    
    data = (( 
            data
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
                    ).where('firstname not like "Tailspin%" and firstname not like "Wingtip%" ')
    ))
    
    df_customer = data.withColumn('firstname', f.split('firstname', '[^A-Za-z0-9]', -1))


    tmp_customerframe = (( df_customer
                        .withColumn('lastname', df_customer['firstname'].getItem(1))
    ))
    
    #change boolean for 'binary' and empty for nulls
    customer = ((
        tmp_customerframe
            .withColumn('isoncredithold', f.when(tmp_customerframe['isoncredithold'] == 'true', 1).otherwise(0))
            .withColumn('isstatementsent', f.when(tmp_customerframe['isstatementsent'] == 'true', 1).otherwise(0))
            .withColumn('buyinggroupname', f.expr('coalesce(buyinggroupname, "Personal Customer")'))
            .withColumn('place', f.when(tmp_customerframe['place'] == "", None).otherwise(tmp_customerframe['place']))
            .withColumn('firstname', tmp_customerframe['firstname'].getItem(0)).alias('temp_name')
            
            #normalize null values without rename column
            .withColumn('place', f.expr('coalesce(place, "Not Available")'))
            .withColumn('creditlimit', f.expr('cast(creditlimit as decimal(12,2))'))

            # .withColumnRenamed('firstname', 'customername')
))
    return customer

def create_storetable(data = store_splitname()):
    
    #create store table
    storeframe = (( 
               data
                .select(    'customerid',
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
                .withColumnRenamed('lastname', 'businessunity')
               ))
    return storeframe

#organizing columns

def create_customertable(data = cleanfirstlastname_customer()):
    
    customerframe= ((
                    data
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
                                )))
    return customerframe

#write to table
def writetodimension(store = create_storetable(), customer = create_customertable(), customercategory = customercategory):
    
    ((
        customer
            .write
            .format('jdbc')
                .option('url', mssql_dwdest)
                .option('driver', mssqldriver)
                .option('user', mssqluser)
                .option('password', mssqlpwd)
                .option('dbtable', 'dimension.customer')
            .save(mode= 'overwrite')
    ))
    
    ((
        store
            .write
            .format('jdbc')
                .option('url', mssql_dwdest)
                .option('driver', mssqldriver)
                .option('user', mssqluser)
                .option('password', mssqlpwd)
                .option('dbtable', 'dimension.store')
            .save(mode= 'overwrite')
    ))
    
    ((
    customercategory
        .write
        .format('jdbc')
            .option('url', mssql_dwdest)
            .option('driver', mssqldriver)
            .option('user', mssqluser)
            .option('password', mssqlpwd)
            .option('dbtable', 'dimension.customercategory')
        .save(mode= 'overwrite')
    ))
    

if __name__ == '__main__':
    
    createsession()
    loadcustomer()
    load_customercategory()
    load_buyinggroups()
    joining_frames()
    renameadjust_column()
    store_splitname()
    cleanfirstlastname_customer()
    create_storetable()
    create_customertable()
    writetodimension()
    spark.stop()