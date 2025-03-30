from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame

spark = (SparkSession.builder
                    .appName("unityteste")
                    .config("spark.sql.defaultCatalog", "spark_catalog")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .enableHiveSupport()
                    .getOrCreate()
    
)

spark.sql("""show catalogs""").show()
spark.sql(""" show databases in spark_catalog """).show()
spark.sql(""" show tables in spark_catalog.default""").show()
# spark.sql(""" select * from spark_catalog.default.dim_product""").show()

# data = [(1, "nomea"),
#         (2, "nomeb"),
#         (3, "nomec"),
#         (4, "nomed"),
#         (5, "nomee"),
#         (6, "nomef")]

# schema = StructType(
#                     [StructField("id", IntegerType(), False),
#                     StructField("name", StringType(), False)]
    
# )

# teste = spark.createDataFrame(data, schema)

# teste.write.format("delta").mode("overwrite").save("spark_catalog.default.nucteste")


