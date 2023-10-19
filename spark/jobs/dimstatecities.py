#this dimension belengs to application schema
#creating statecities dimension

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f
import os as os

from dotenv import load_dotenv

#loading environments variable

env = load_dotenv('/mnt/d/linux/project/vars/.env')

pgjar = os.getenv('POSTGRE_JAR')    
pgconn = os.getenv('POSTGRE_CONN')
pgdriver = os.getenv('PG_DRIVER')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')

#session 

spark = (( SparkSession
            .builder
            .appName('statecities')
            .config('spark.jars', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
        ))

#load cities data to dataframe
tmp_citiesframe =  ((spark
                        .read
                        .format('jdbc')
                        .option('url', pgconn)
                        .option('driver', pgdriver)
                        .option('user', pguser)
                        .option('password', pgpwd)
                        .option('query', 
                                """ 
                                    select 

                                        apc.cityid,
                                        apc.cityname,
                                        apc.stateprovinceid,
                                        apc.latestrecordedpopulation,
                                        apc._airbyte_ab_id,
                                        apc._airbyte_cities_hashid,
                                        cast(apc._airbyte_emitted_at as date) as _airbyte_emitted_at,
                                        cast(apc._airbyte_normalized_at as date) as _airbyte_normalized_at

                                    from application.cities as apc

                                    left join

                                        application.cities_archive as apca
                                    on apc.cityid = apca.cityid
                                    
                                    """) 
                          )).load()

tmp_citiesframe.show(5, False)

#load states and provinces to dataframe
tmp_stateframe = ((spark
                    .read
                    .format('jdbc')
                    .option('url', pgconn)
                    .option('driver', pgdriver)
                    .option('user', pguser)
                    .option('password', pgpwd)
                    .option('query',
                            """ 
                                select

                                    asp.countryid,
                                    asp.stateprovinceid,
                                    asp.stateprovincename,
                                    asp.salesterritory,
                                    asp.stateprovincecode,
                                    asp.latestrecordedpopulation,
                                    asp._airbyte_ab_id,
                                    asp._airbyte_stateprovinces_hashid,
                                    cast(asp._airbyte_emitted_at as date) as _airbyte_emitted_at,
                                    cast(asp._airbyte_normalized_at as date) as _airbyte_normalized_at
                                from application.stateprovinces as asp
                                
                                left join
                                    application.stateprovinces_archive as aspa
                                on asp.stateprovinceid = aspa.stateprovinceid
                            
                            """) 
                   )).load()

tmp_stateframe.show()

#joining dataframes based on stateprovinceid column
state_cities = (( 
                 tmp_stateframe
                    .join(tmp_citiesframe,
                          on= 'stateprovinceid',
                          how= 'left')
                    
                    #select and rename columns
                    .select(tmp_stateframe.countryid,
                            tmp_stateframe.stateprovinceid,
                            tmp_stateframe.stateprovincename,
                            tmp_stateframe.salesterritory,
                            tmp_stateframe.stateprovincecode,
                            tmp_citiesframe.cityid,
                            tmp_citiesframe.cityname,
                            tmp_stateframe.latestrecordedpopulation.alias('statepopulation'),
                            
                            #removing null values
                            f.coalesce(tmp_citiesframe.latestrecordedpopulation, f.lit(0)).alias('citypopulation'),
                            tmp_stateframe._airbyte_ab_id,
                            
                            #rename to avoid duplicated results
                            tmp_citiesframe._airbyte_ab_id.alias('city_elt_hashid'),
                            tmp_stateframe._airbyte_stateprovinces_hashid,
                            tmp_citiesframe._airbyte_cities_hashid,
                            tmp_stateframe._airbyte_emitted_at,
                            tmp_stateframe._airbyte_normalized_at)

                 ))
#rename column
statecities_final = (( 
                      state_cities
                        .withColumnRenamed('stateprovinceid', 'stateid')
                        .withColumnRenamed('stateprovincename', 'statename')
                        .withColumnRenamed('_airbyte_ab_id', 'state_elt_hashid')
                        .withColumnRenamed('tmp_citiesframe._airbyte_ab_id', 'city_elt_hashid')
                        .withColumnRenamed('_airbyte_stateprovinces_hashid', 'stateprovince_hashid')
                        .withColumnRenamed('_airbyte_cities_hashid', 'cities_hashid')
                        .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                        .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
                      ))


#load transformed data to table

((
    statecities_final
        .write
            .format('jdbc')
            .option('url', f'{pgconn}')
            .option('driver', f'{pgdriver}')
            .option('user', f'{pguser}')
            .option('password', f'{pgpwd}')
            .option('dbtable', 'application.testeinsert')
        .save(mode= 'Overwrite')
    
))