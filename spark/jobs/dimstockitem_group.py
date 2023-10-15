#importing packages and env-var

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DataType
import os
from dotenv import load_dotenv

load_dotenv('/mnt/d/linux/project/vars/.env')

pgconn = os.getenv('POSTGRE_CONN')
pguser = os.getenv('POSTGRE_USER')
pgpwd = os.getenv('POSTGRE_PWD')
pgjar = os.getenv('POSTGRE_JAR')
pgdriver = os.getenv('PG_DRIVER')


#creating spark session

spark = (SparkSession
            .builder
            .appName('dim_stock')
            .config('spark.jars', f'{pgjar}')
            .config('spark.driver.extraClassPath', f'{pgjar}')
            .config('spark.executor.extraClassPath', f'{pgjar}')
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

#load data from postgre database table
#start cleaning data

#first of all, lets create a dataframe from data table
stockitems_frame = (( spark.read
                        .format('jdbc')
                        .option('url', pgconn)
                        .option('user', pguser)
                        .option('password', pgpwd)
                        .option('driver', pgdriver)
                        .option('query', 
                                
                                """  
                                select 

                                        wsi.StockItemID,
                                        wsi.StockItemName,
                                        wsi.SupplierID,
                                        sisg.stockgroupid,
                                        wsi.ColorID,
                                        wsi.UnitPackageID,
                                        wsi.OuterPackageID,
                                        wsi.QuantityPerOuter,
                                        wsi.Brand,
                                        wsi."Size",
                                        wsi.LeadTimeDays,
                                        wsi.IsChillerStock,
                                        wsi.TaxRate,
                                        wsi.UnitPrice,
                                        wsi.RecommendedRetailPrice,
                                        wsi.TypicalWeightPerUnit,
                                        wsi.MarketingComments,
                                        wsi.CustomFields,
                                        wsi.tags,
                                        wsi.ValidFrom,
                                        wsi.ValidTo,
                                        wsi._airbyte_ab_id,
                                        wsi._airbyte_stockitems_hashid,
                                        wsi._airbyte_emitted_at,
                                        wsi._airbyte_normalized_at

                                from Warehouse.StockItems as wsi

                                left join

                                        Warehouse.StockItems_Archive as wsia

                                on wsi.StockItemID = wsia.StockItemID

                                left join

                                        warehouse.stockitemstockgroups sisg

                                on wsi.stockitemid = sisg.stockitemid
                                
                                """      
                        ))).load()

#once data is loaded into a dataframe
#cleaning customfield column wiht reg_exp
#remove characteres like " {} [] "
#renaming resulting columns
#drop columns that not satisfy our dataset

tempframe = (stockitems_frame
                        .select('*',\
                                f.regexp_replace('customfields', "[^0-9a-zA-Z , :]", "")
                                        .alias('temp_customfield'),
                                f.regexp_replace('tags', '[^0-9a-zA-Z]', '')
                                        .alias('tags'))
                        .withColumnRenamed('Size', 'size')
                        .drop('customfields')
                        .drop('tags')
)
                


#now, select substring from temp_customfield
#leaving information about country manufacture
#droping unused columns

(
    tempframe
        .select('*',
                f.substring_index('temp_customfield', ',', 1)
                    .alias('temp_country'),
                f.substring_index('temp_country', ':', -1)
                    .alias('countrymanufacture'))
        .drop('temp_country')
)
       
       
#fill null values
#renaming airbyte columns
dimstockitem_frame = (
    
            tempframe.fillna({'size': 'unknown',
                  'colorid': 999,
                  'brand': 'unknown',
                  'marketingcomments': 'unknown'
                  })
            
            .withColumnRenamed('_airbyte_ab_id', 'extract_id')
            .withColumnRenamed('_airbyte_stockitems_hashid', 'stockitems_hashid')
            .withColumnRenamed('_airbyte_emitted_at', 'elt_start')
            .withColumnRenamed('_airbyte_normalized_at', 'elt_end')
            .drop('temp_customfield')
)

#load transformed data into database table
#finishing elt process

(
    dimstockitem_frame.write
            .format('jdbc')
            .option('url', pgconn)
            .option('user', pguser)
            .option('password', pgpwd)
            .option('driver', pgdriver)
            .option('mode', 'overwrite')
            .option('dbtable', 'warehouse.testeinsert')
        .save(mode = 'append')
)

spark.stop()