from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("nexo-iceberg-sample").getOrCreate()

spark.sql("CREATE SCHEMA IF NOT EXISTS nexo.landing")

spark.sql("""
  CREATE TABLE IF NOT EXISTS nexo.landing.sample (
    id     BIGINT,
    name   STRING,
    value  DOUBLE
  ) USING iceberg
  TBLPROPERTIES ('format-version' = '3')
""")

data = [(1, "Alice", 10.5), (2, "Bob", 20.0), (3, "Charlie", 15.75)]
df = spark.createDataFrame(data, ["id", "name", "value"])
df.writeTo("nexo.landing.sample").append()

spark.sql("SELECT * FROM nexo.landing.sample").show()

spark.stop()
