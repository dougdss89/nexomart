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

#create session 
spark = ((SparkSession
            .builder
            .appName('dimcountry')
            .config('spark.jar', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
            
))

#loading data from table
tmp_countryframe = (( spark
                        .read
                        .format('jdbc')
                        .option('url', f'{pgconn}')
                        .option('driver', f'{pgdriver}')
                        .option('user', f'{pguser}')
                        .option('password', f'{pgpwd}')
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
                                    ac.latestrecordedpopulation,
                                    ac._airbyte_ab_id,
                                    ac._airbyte_countries_hashid,
                                    ac._airbyte_emitted_at,
                                    ac._airbyte_normalized_at
                                                                  
                                from application.countries as ac
                                
                                """
                            )
                     
)).load()

#renaming columns

countryframe = (( tmp_countryframe
                    .withColumnRenamed('_airbyte_ab_id', 'elt_hashid')
                    .withColumnRenamed('_airbyte_countries_hashid', 'country_hashid')
                    .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                    .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
                    .withColumnRenamed('isonumericcode', 'isostandardid')
                    .withColumnRenamed('isoalpha3code', 'countrycode')
                    .withColumnRenamed('region', 'countryregion')
))

#insert normalized datav
(( 
  countryframe
    .write
        .format('jdbc')
        .option('url', f'{pgconn}')
        .option('driver', f'{pgdriver}')
        .option('user', f'{pguser}')
        .option('password', f'{pgpwd}')
        .option('dbtable', 'application.testeinsert')
    .save(mode= 'overwrite')
))