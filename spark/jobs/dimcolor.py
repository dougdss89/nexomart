from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType, DataType
import os
from dotenv import load_dotenv

#load environment variables
env = load_dotenv(dotenv_path='/mnt/d/linux/project/vars/.env')

#env vars for postgresql connection
pgconn = os.getenv('POSTGRE_CONN')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')
driver = os.getenv('PG_DRIVER')
pgjar = os.getenv('POSTGRE_JAR')
pgconn_dw = os.getenv('POSTGRE_CONN_DW')


#create spark session 
spark = (SparkSession.\
            builder.\
            appName('dim_colors').\
            config('spark.jars', f'{pgjar}').\
            config('spark. driver.extraClassPath', f'{pgjar}').\
            config('spark.executor.extraClassPath', f'{pgjar}').\
        getOrCreate()
    )


def selectcolors(spark = spark, connurl = pgconn, dbuser = pguser, dbpwd = pgpwd, dbdriver = driver):
    
    """ 
    
    this function selects data from colors table on warehouse schema
    All parameters are set by default variables that we create at the beginning.
    
    """
       
    dimension_colors = spark.\
                            read.\
                            format('jdbc').\
                            option('url', connurl).\
                            option('user', dbuser).\
                            option('password', dbpwd).\
                            option('driver', dbdriver).\
                            option('query',
                                   
                            """
                                select 

                                    wrh_c.colorid,
                                    wrh_c.colorname,
                                    wrh_c._airbyte_ab_id as elt_hashid,
                                    wrh_c._airbyte_ab_id as color_hashid,
                                    wrh_c._airbyte_emitted_at as elt_start,
                                    wrh_c._airbyte_normalized_at as elt_end
                        
                                from warehouse.colors as wrh_c
                                    left join
                                    warehouse.colors_archive as wrh_ac
                                on wrh_c.colorid = wrh_ac.colorid
                                   
                            """).\
                        load()
                            
    return dimension_colors

data = selectcolors()

def insertcolor(spark = spark, connurl = pgconn, dbuser = pguser, dbpwd = pgpwd, dbdriver = driver, data = data):
    
    """ This function inserts data into color dimension table """
    
    data.\
        write.\
        format('jdbc').\
        option('url', pgconn_dw).\
        option('user', dbuser).\
        option('dbtable', 'dimension.color').\
        option('mode', 'overwrite').\
        option('password', dbpwd).\
        option('driver', dbdriver).\
    save(mode='overwrite')
    
insertcolor()


if __name__ == '__main__':
    
    selectcolors()
    insertcolor()
    spark.stop()