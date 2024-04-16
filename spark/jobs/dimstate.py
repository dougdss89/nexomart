
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os
from dotenv import load_dotenv

env = load_dotenv(dotenv_path='/mnt/d/linux/project/vars/.env')

#env vars for postgresql connection
mssqlsource = os.getenv('MSSQLSOURCE')
mssqluser = os.getenv('MSSQL_SPARK_USER')
mssqlpwd = os.getenv('MSSQL_SPARK_PWD')
mssqldriver = os.getenv('MSSQL_DRIVER')
sqlserverjar = os.getenv('SQLSERVER_JAR')
mssql_dwdest = os.getenv('MSSQL_CONN')

#session 
def sparksession():
    
    spark = (( SparkSession
            .builder
                .appName('statecities')
                .config('spark.jars', f'{sqlserverjar}')
                .config('spark.driver.extraClassPath', f'{sqlserverjar}')
                .config('spark.executor.extraClassPath', f'{sqlserverjar}')
            .getOrCreate()
        ))
    
    return spark

spark = sparksession()

#loading cities data to dataframe
def createcity(spark = sparksession(), conn = mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    """ creating city and state dimension """
    tmp_citiesframe =  ((spark
                            .read
                            .format('jdbc')
                            .option('url', conn)
                            .option('driver', driver)
                            .option('user', user)
                            .option('password', pwd)
                            .option('query', 
                                    """ 
                                        select 

                                            apc.cityid,
                                            apc.cityname,
                                            apc.stateprovinceid,
                                            apc.latestrecordedpopulation

                                        from application.cities as apc

                                        left join

                                            application.cities_archive as apca
                                        on apc.cityid = apca.cityid
                                        
                                        """) 
                            )).load()
    
    return tmp_citiesframe


#loading states and provinces to dataframe
def createstate(spark = sparksession(), conn = mssqlsource, driver = mssqldriver , user = mssqluser, pwd = mssqlpwd):
    """ create country dimension to join with state """
    
    tmp_stateframe = ((spark
                        .read
                        .format('jdbc')
                        .option('url', conn)
                        .option('driver', driver)
                        .option('user', user)
                        .option('password', pwd)
                        .option('query',
                                """ 
                                    select

                                        asp.countryid,
                                        asp.stateprovinceid,
                                        asp.stateprovincename,
                                        asp.salesterritory,
                                        asp.stateprovincecode,
                                        asp.latestrecordedpopulation
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
                                f.coalesce(state.latestrecordedpopulation, f.lit(0)).alias('citypopulation'))

                    ))
    #rename column
    dimregion = (( 
                    state_cities
                    .withColumnRenamed('stateprovinceid', 'stateid')
                    .withColumnRenamed('stateprovincename', 'statename')

                ))

    return dimregion

#write transformed data to table
def writedata(data= join_select_countrystate(), conn_dw= mssql_dwdest, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    ((
        data
            .write
                .format('jdbc')
                .option('url', f'{conn_dw}')
                .option('driver', f'{driver}')
                .option('user', f'{user}')
                .option('password', f'{pwd}')
                .option('dbtable', 'dimension.state')
            .save(mode= 'Overwrite')   
    ))
    
if __name__ == '__main__':
    createcity()
    print('City created')
    
    createstate()
    print('State created')
    
    join_select_countrystate()
    print('Join process completed successfully')
    
    writedata()
    print('Data was saved successfully')
    
    print('Finishing SparkSession')
    spark.stop()