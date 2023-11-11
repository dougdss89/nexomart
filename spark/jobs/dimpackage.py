#importing packages and env-vars

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

#load env-var
from dotenv import load_dotenv
import os

env = load_dotenv("/mnt/d/linux/project/vars/.env")

pgjar = os.getenv('POSTGRE_JAR')
pgdriver = os.getenv('PG_DRIVER')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')
pghost = os.getenv('POSTGRE_HOST')
pgconn = os.getenv('POSTGRE_CONN')
pgconn_dw = os.getenv('POSTGRE_CONN_DW')

#creating sparksession 

spark = ((
        SparkSession
            .builder
            .appName('dimpackage')
            .config('spark.jars', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
        .getOrCreate()
))

#loading data from table into a dataframe
def createpackage(spark = spark, pgconn = pgconn, pguser = pguser, pgpwd = pgpwd, pgdriver = pgdriver):
    
    """loading and create packagetype dimension """
    dimpackage_frame = ((spark
                        .read
                        .format('jdbc')
                        .option('url', pgconn)
                        .option('user', pguser)
                        .option('password', pgpwd)
                        .option('driver', pgdriver)
                        .option('query',
                                """
                                    select

                                        packagetypeid,
                                        packagetypename,
                                        _airbyte_ab_id,
                                        _airbyte_packagetypes_hashid,
                                        _airbyte_emitted_at,
                                        _airbyte_normalized_at
                                    
                                    from warehouse.packagetypes
                                
                                """)
                    )).load()
    
    return dimpackage_frame

packagetype = createpackage()

#rename and reshape data type
def renamepackage(data = packagetype):
    packagetype_dim = ((
        
                    data
                        .withColumnRenamed('packagetypeid', 'packageid')
                        .withColumnRenamed('packagetypename', 'packagetype')
                        .withColumnRenamed('_airbyte_ab_id', 'extract_id')
                        .withColumnRenamed('_airbyte_packagetypes_hashid', 'package_hashid')
                        .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                        .withColumnRenamed('_airbyte_normalized_at', 'elt_end')

    ))
    
    return packagetype_dim

#load transformed data into a database table

def writepackage(data = renamepackage(), pgconn_dw = pgconn_dw, pguser = pguser, pgpwd = pgpwd, pgdriver = pgdriver):
    ((
        data
            .write
            .format('jdbc')
            .option('url', pgconn_dw)
            .option('user', pguser)
            .option('password', pgpwd)
            .option('driver', pgdriver)
            .option('dbtable', 'dimension.packagetype')
            .option('mode', 'overwrite')
        .save(mode='overwrite')
        
    ))
    
if __name__ == '__main__':
    createpackage()
    renamepackage()
    writepackage()
    spark.stop()