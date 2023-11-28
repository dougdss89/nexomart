#this dimension belongs to application schema
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
pgconn_dw = os.getenv('POSTGRE_CONN_DW')

#session 
spark = (( SparkSession
            .builder
            .appName('statecities')
            .config('spark.jars', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
            .getOrCreate()
        ))

#loading cities data to dataframe
def createcity(spark = spark, pgconn = pgconn, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    
    """ creating city and state dimension """
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
    
    return tmp_citiesframe


#loading states and provinces to dataframe
def createstate(spark = spark, pgconn = pgconn, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    """ create country dimension to join with state """
    
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
    
    return tmp_stateframe


#joining dataframes based on stateprovinceid column
def join_select_countrystate(country = createstate(), state= createcity()):
    
    state_cities = (( 
                    country
                        .join(state,
                            on= 'stateprovinceid',
                            how= 'left')
                        
                        #select and rename columns
                        .select(country.stateprovinceid,
                                country.countryid,
                                country.stateprovincename,
                                country.salesterritory,
                                country.stateprovincecode,
                                state.cityid,
                                state.cityname,
                                country.latestrecordedpopulation.alias('statepopulation'),
                                
                                #removing null values
                                f.coalesce(state.latestrecordedpopulation, f.lit(0)).alias('citypopulation'),
                                country._airbyte_ab_id,
                                
                                #rename to avoid duplicated results
                                state._airbyte_ab_id.alias('city_elt_hashid'),
                                country._airbyte_stateprovinces_hashid,
                                state._airbyte_cities_hashid,
                                country._airbyte_emitted_at,
                                country._airbyte_normalized_at)

                    ))
    #rename column
    dimregion = (( 
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

    return dimregion

#write transformed data to table
def writedata(data= join_select_countrystate(), pgconn_dw= pgconn_dw, pgdriver = pgdriver, pguser = pguser, pgpwd = pgpwd):
    ((
        data
            .write
                .format('jdbc')
                .option('url', f'{pgconn_dw}')
                .option('driver', f'{pgdriver}')
                .option('user', f'{pguser}')
                .option('password', f'{pgpwd}')
                .option('dbtable', 'dimension.state')
            .save(mode= 'Overwrite')   
    ))
    
if __name__ == '__main__':
    createcity()
    createstate()
    join_select_countrystate()
    writedata()
    spark.stop()