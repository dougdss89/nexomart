from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("IcebergNessieTest") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie-server:19120/api/v2") \
    .config("spark.sql.catalog.nessie.ref", "teste") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://iceberg/warehouse") \
    .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-minio-1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Creates the namespace (database) if it doesn't exist
# spark.sql("CREATE BRANCH IF NOT EXISTS dev IN nessie")
# spark.sql("USE REFERENCE dev IN nessie")

spark.sql("CREATE DATABASE IF NOT EXISTS nessie.db")

# Sample data
data = [("Data Engineer", 1), ("Data Scientist", 2), ("Analyst", 3)]
schema = StructType([
    StructField("role", StringType(), True),
    StructField("id", IntegerType(), True)
])

dataframe = spark.createDataFrame(data, schema)

# Writes the data
dataframe.write.format("iceberg").mode("overwrite").saveAsTable("nessie.db.testefim")

# Creates and uses a branch


# Query
spark.sql("SELECT * FROM nessie.db.testefim").show()

spark.stop()