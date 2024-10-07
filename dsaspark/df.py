from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# Inicia uma SparkSession
spark = SparkSession.builder.appName("SimpleDataFrame").getOrCreate()

# Cria um DataFrame com 4 colunas e 1000 linhas
data = [(i, f"col2_{i}", f"col3_{i}", f"col4_{i}") for i in range(1000)]
df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])


df.write.mode("overwrite").parquet("hdfs://nodemaster.com:9000/hadoop/data/teste_parquet")
# Mostra as primeiras 5 linhas
df.show(5)
