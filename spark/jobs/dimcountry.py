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

#create session 
spark = ((SparkSession
            .builder
            .appName('dimcountry')
            .config('spark.jar', f'{sqlserverjar}')
            .config('spark.driver.extraClassPath', f'{sqlserverjar}')
            .config('spark.executor.extraClassPath', f'{sqlserverjar}')
            .getOrCreate()
            
        ))

def loadcountry(spark = spark):
    
    """ 
    
        This function loads data from country
        table into a dataframe
    
    """
    tmp_countryframe = (( spark
                        .read
                        .format('jdbc')
                        .option('url', f'{mssqlsource}')
                        .option('driver', f'{mssqldriver}')
                        .option('user', f'{mssqluser}')
                        .option('password', f'{mssqlpwd}')
                        .option('query', 
                                
                                """ 
                                select

                                    ac.countryid,
                                    ac.continent,
                                    ac.region,
                                    ac.subregion,
                                    ac.formalname,
                                    ac.countryname,
                                    ac.isonumericcode,
                                    ac.isoalpha3code,
                                    ac.latestrecordedpopulation
                                                                  
                                from application.countries as ac
                                
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

                        .withColumnRenamed('isonumericcode', 'isostandardid')
                        .withColumnRenamed('isoalpha3code', 'countrycode')
                        .withColumnRenamed('region', 'countryregion')
                    ))
    
    return countryframe

dimcountry = renamecolumns()

def insertcountrydata(spark = spark, dwconnection = mssql_dwdest, dbuser = mssqluser, dbpwd = mssqlpwd, driver = mssqldriver, country = dimcountry):
#insert normalized data
    (( 
    dimcountry
        .write
        .format('jdbc')
        .option('url', f'{mssql_dwdest}')
        .option('driver', f'{mssqldriver}')
        .option('user', f'{mssqluser}')
        .option('password', f'{mssqlpwd}')
        .option('dbtable', 'dimension.country')
        .save(mode= 'overwrite')
    ))
    
if __name__== '__main__':
    
    loadcountry()
    renamecolumns()
    insertcountrydata()
    spark.stop