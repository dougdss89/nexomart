from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

spark = (
            SparkSession
                .builder
                .master("local[*]")

                .appName("notebook_teste")
                .config(
                    "spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,"
                    "org.scala-lang:scala-library:2.13.12")
                # Configuração do Catálogo Iceberg
                .config("spark.sql.catalog.icebergtb", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=log4j.properties") 
                .config("spark.log.level", "ERROR") 
                .config("spark.eventLog.enabled", "false") 
                # .config("spark.sql.catalog.icebergtb.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
                .config("spark.sql.catalog.icebergtb.type", "hadoop") 
                .config("spark.sql.catalog.icebergtb.warehouse", "s3a://iceberg/")
                .config("spark.sql.catalog.icebergtb.hadoop.fs.s3a.endpoint", "http://localhost:9000")
                .config("spark.sql.catalog.icebergtb.hadoop.fs.s3a.access.key", "minioadmin")
                .config("spark.sql.catalog.icebergtb.hadoop.fs.s3a.secret.key", "minioadmin")
                .config("spark.sql.catalog.icebergtb.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.sql.catalog.icebergtb.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") 
                .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") 
            .getOrCreate()
)

spark


#data transform

spark.sql(""" select 
          
                row_number() over (order by countryname) as currency_key,
                currencycode, 
                currecyname,
                countryregioncode as country_code,
                countryname,
                extracted_date,
                modified_date,
                eltversion

          
          from icebergtb.advworks_rdb.raw_dimcurrencycode""").show()

spark.sql(""" show catalogs """).show()