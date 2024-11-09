from pyspark.sql import SparkSession

# Inicia uma SparkSession com configurações para Iceberg
spark = (SparkSession.builder
         .appName("IcebergDataFrame")
         .config("spark.driver.memory", "12g")  # Aumenta a memória do driver
         .config("spark.executor.memory", "12g")  # Aumenta a memória dos executores
         .config("spark.sql.shuffle.partitions", "150")  # Ajusta o número de partições
         # Configurações específicas do Iceberg
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
         .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://secondnode.hdp.com:9000/warehouse/iceberg")
         .getOrCreate())

# Cria um DataFrame com 4 colunas e 7 milhões de linhas
data = [(i, f"col2_{i}", f"col3_{i}", f"col4_{i}") for i in range(100000)]
df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])

# Escreve o DataFrame em um diretório HDFS como parquet (ou outro formato)
hdfs_path = "hdfs://secondnode.hdp.com:9000/warehouse/raw/testecargasete"
df.write.mode("overwrite").parquet(hdfs_path)
print("DataFrame escrito em formato parquet no HDFS com sucesso!")

# Lê o mesmo DataFrame a partir do diretório HDFS
df_hdfs = spark.read.parquet(hdfs_path)

# Escreve o DataFrame lido no formato Iceberg
(df_hdfs.write
    .format("iceberg")
    .mode("overwrite")
    .saveAsTable("hadoop_prod.db.testecargasete"))
print("DataFrame escrito em formato Iceberg com sucesso!")

# Encerra a sessão Spark após a execução
spark.stop()
