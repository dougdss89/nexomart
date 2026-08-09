from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Spark configuration with MinIO
spark = SparkSession.builder \
    .appName("IcebergMinIOTest") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://iceberg/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-minio-1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Sample data creation
data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

dataframe = spark.createDataFrame(data, schema)

# Writes to Iceberg (MinIO)
dataframe.writeTo("local.db.people").create()

# Queries the data
spark.sql("SELECT * FROM local.db.people").show()

spark.stop()