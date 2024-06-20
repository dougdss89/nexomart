# Projeto 4 - Configurando Um Cluster Spark Local Para Executar Pipelines de Machine Learning em Python
# Executando o Pipeline 3 de Machine Learning no Cluster Spark

# Imports
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType

# Inicializa o Spark Session
spark = SparkSession.builder.appName("DSAPipeline3").getOrCreate()

# Carrega os dados
df = spark.read.format("csv").option("header", "true").load("/opt/spark/data/dataset3.csv")

# Converte as colunas 'idade', 'renda_anual' e 'pontuação_gastos' para FloatType
df = df.withColumn("idade", col("idade").cast(FloatType()))
df = df.withColumn("renda_anual", col("renda_anual").cast(FloatType()))
df = df.withColumn("pontuação_gastos", col("pontuação_gastos").cast(FloatType()))

# Assembla as features 'idade', 'renda_anual' e 'pontuação_gastos' em um vetor
assembler = VectorAssembler(inputCols = ["idade", "renda_anual", "pontuação_gastos"], outputCol = "features")

# Define o modelo K-means
kmeans = KMeans(featuresCol = "features", k = 3)

# Define o pipeline
pipeline = Pipeline(stages = [assembler, kmeans])

# Treina o modelo
model = pipeline.fit(df)

# Faz as previsões (atribui os dados a clusters)
predictions = model.transform(df)

# Salva as previsões em um arquivo CSV
predictions.select("prediction").coalesce(1).write.format('com.databricks.spark.csv').mode("overwrite").option('header', 'true').save('/opt/spark/data/resultadop3')

# Para fechar a Spark Session quando a aplicação terminar
spark.stop()
