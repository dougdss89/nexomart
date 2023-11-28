#importing packages and vars

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os
from dotenv import load_dotenv

env = load_dotenv('/mnt/d/linux/project/vars/.env')

pgjar = os.getenv('POSTGRE_JAR')
pgconn = os.getenv('POSTGRE_CONN')
pgdriver = os.getenv('PG_DRIVER')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')
pgconn_dw = os.getenv('POSTGRE_CONN_DW')

#create session 
spark = ((SparkSession
            .builder
                .appName('dimcountry')
                .config('spark.jar', f'{pgjar}')
                .config('spark.driver.extraClassPath', f'{pgjar}')
                .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
            
))

def loadcountry(spark = spark, connection = pgconn, dbuser = pguser, dbpwd = pgpwd, driver = pgdriver):
    
    """ 
    
        This function loads data from country
        table into a dataframe
    
    """
    tmp_countryframe = (( spark
                            .read
                            .format('jdbc')
                            .option('url', f'{connection}')
                            .option('driver', f'{driver}')
                            .option('user', f'{dbuser}')
                            .option('password', f'{dbpwd}')
                            .option('query', 
                                    
                                    """ 
                                    select

                                        countryid,
                                        continent,
                                        region,
                                        subregion,
                                        formalname,
                                        countryname,
                                        isonumericcode,
                                        isoalpha3code,
                                        latestrecordedpopulation,
                                        _airbyte_ab_id,
                                        _airbyte_countries_hashid,
                                        _airbyte_emitted_at,
                                        _airbyte_normalized_at
                                                                    
                                    from application.countries
                                    
                                    """
                                )
                        
                        )).load()
    
    return tmp_countryframe
    
countrydata = loadcountry()

def renamecolumns(countrydataframe = countrydata):
    
    """
    
        this function only rename columns
        the source data was normalized
        
    """
    countryframe = (( countrydataframe
                        .withColumnRenamed('_airbyte_ab_id', 'elt_hashid')
                        .withColumnRenamed('_airbyte_countries_hashid', 'country_hashid')
                        .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                        .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
                        .withColumnRenamed('isonumericcode', 'isostandardid')
                        .withColumnRenamed('isoalpha3code', 'countrycode')
                        .withColumnRenamed('region', 'countryregion')
                    ))
    
    return countryframe

dimcountry = renamecolumns()

def insertcountrydata(spark = spark, dwconnection = pgconn_dw, dbuser = pguser, dbpwd = pgpwd, driver = pgdriver, country = dimcountry):
#insert normalized data
    (( 
    country
        .write
            .format('jdbc')
            .option('url', f'{dwconnection}')
            .option('driver', f'{driver}')
            .option('user', f'{dbuser}')
            .option('password', f'{dbpwd}')
            .option('dbtable', 'dimension.country')
        .save(mode= 'overwrite')
    ))
    
if __name__== '__main__':
    
    loadcountry()
    renamecolumns()
    insertcountrydata()
    spark.stop