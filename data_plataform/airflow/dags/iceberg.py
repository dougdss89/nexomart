from pyspark.sql import SparkSession

# Inicia uma SparkSession com configurações para Iceberg
spark = (SparkSession.builder
         .appName("IcebergDataFrame")
         .getOrCreate())

# Cria um DataFrame com 4 colunas e 7 milhões de linhas
data = [(i, f"col2_{i}", f"col3_{i}", f"col4_{i}") for i in range(1000)]
df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])

# Escreve o DataFrame em um diretório HDFS como parquet (ou outro formato)
hdfs_path = "hdfs://ha-cluster/landing/airflowteste"
df.write.mode("append").parquet(hdfs_path)
print("DataFrame escrito em formato parquet no HDFS com sucesso!")

# Lê o mesmo DataFrame a partir do diretório HDFS
df_hdfs = spark.read.parquet(hdfs_path)

# Escreve o DataFrame lido no formato Iceberg
(df_hdfs.write
    .format("iceberg")
    .mode("overwrite")
    .saveAsTable("hadoop_prod.db.airflowteste"))
print("DataFrame escrito em formato Iceberg com sucesso!")

# Encerra a sessão Spark após a execução
spark.stop()
