#importing packages and env-vars

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DataType

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

#load data from table into a dataframe
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

#rename and reshape data type
renameframe = ((
    
                dimpackage_frame
                    .withColumnRenamed('packagetypeid', 'packageid')
                    .withColumnRenamed('packagetypename', 'packagetype')
                    .withColumnRenamed('_airbyte_ab_id', 'extract_id')
                    .withColumnRenamed('_airbyte_packagetypes_hashid', 'package_hashid')
                    .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
                    .withColumnRenamed('_airbyte_normalized_at', 'elt_end')

))

#load transformed data into a database table

((
    renameframe
        .write
        .format('jdbc')
        .option('url', pgconn)
        .option('user', pguser)
        .option('password', pgpwd)
        .option('driver', pgdriver)
        .option('dbtable', 'warehouse.testeinsert')
        .option('mode', 'overwrite')
    .save(mode='overwrite')
    
))

spark.stop()