from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("nexo-iceberg-dev-sample").getOrCreate()

spark.sql("CREATE SCHEMA IF NOT EXISTS nexo.dev")

spark.sql("""
  CREATE TABLE IF NOT EXISTS nexo.dev.people (
    name STRING,
    age  INT
  ) USING iceberg
  TBLPROPERTIES ('format-version' = '3')
""")

schema = StructType(
    [
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
)
data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
df = spark.createDataFrame(data, schema)

df.writeTo("nexo.dev.people").append()

spark.sql("SELECT * FROM nexo.dev.people").show()

spark.stop()
