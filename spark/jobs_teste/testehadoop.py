from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# Inicia uma SparkSession com mais memória e configuração de paralelismo
spark = (SparkSession.builder
         .appName("SimpleDataFrame")
         .config("spark.driver.memory", "12g")  # Aumenta a memória do driver
         .config("spark.executor.memory", "12g")  # Aumenta a memória dos executores
         .config("spark.sql.shuffle.partitions", "150")  # Ajusta o número de partições
         .getOrCreate())

# Cria um DataFrame com 4 colunas e 5 milhões de linhas
data = [(i, f"col2_{i}", f"col3_{i}", f"col4_{i}") for i in range(7000000)]
df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])

# Escreve o DataFrame em formato Parquet no HDFS
df.write.mode("overwrite").parquet("hdfs://secondnode.hdp.com:9000/testecargasete/sevenmilionrows")

# Verifica se o arquivo foi escrito corretamente
print("DataFrame escrito em formato Parquet com sucesso!")

# Mostra as primeiras 5 linhas
df.show(5)

# Encerra a sessão Spark após a execução
spark.stop()
