from pyspark.sql import SparkSession

# Inicializa a sessão do Spark
spark = SparkSession.builder \
    .appName("DeltaLakeMinIODatabaseExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.defaultCatalog", "spark_catalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .enableHiveSupport()\
    .getOrCreate()

# Cria um banco de dados no MinIO
spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.default LOCATION 's3a://sparkdw/'")

# Usa o banco de dados criado
spark.sql("USE spark_catalog.default")

# Exemplo de DataFrame (dimensão)
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
columns = ["id", "name"]
dimension_df = spark.createDataFrame(data, columns)

# Grava o DataFrame como uma tabela gerenciada no banco de dados atual
dimension_df.write.format("delta").mode("overwrite").saveAsTable("spark_catalog.default.dimension_table")

# Lista as tabelas no banco de dados atual
spark.sql("SHOW TABLES IN spark_catalog.default").show()

# Consulta a tabela criada
result = spark.sql("SELECT * FROM spark_catalog.default.dimension_table")
result.show()