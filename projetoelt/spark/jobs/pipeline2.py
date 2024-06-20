# Projeto 4 - Configurando Um Cluster Spark Local Para Executar Pipelines de Machine Learning em Python
# Executando o Pipeline 2 de Machine Learning no Cluster Spark

# Imports
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType

# Inicializa o Spark Session
spark = SparkSession.builder.appName("DSAPipeline2").getOrCreate()

# Carrega os dados
df = spark.read.format("csv").option("header", "true").load("/opt/spark/data/dataset2.csv")

# Converte as colunas 'atributo2' e 'label' para FloatType
df = df.withColumn("atributo2", col("atributo2").cast(FloatType()))
df = df.withColumn("label", col("label").cast(FloatType()))

# Indexa a coluna 'atributo1'
indexer = StringIndexer(inputCol = "atributo1", outputCol = "atributo1Index")

# Assembla as features 'atributo1Index' e 'atributo2' em um vetor
assembler = VectorAssembler(inputCols = ["atributo1Index", "atributo2"], outputCol = "features")

# Define o modelo de regressão linear
lr = LinearRegression(featuresCol = "features", labelCol = "label", maxIter = 10, regParam = 0.3, elasticNetParam = 0.8)

# Define o pipeline
pipeline = Pipeline(stages = [indexer, assembler, lr])

# Treina o modelo
model = pipeline.fit(df)

# Faz as previsões
predictions = model.transform(df)

# Salva as previsões em um arquivo CSV
predictions.select("prediction").coalesce(1).write.format('com.databricks.spark.csv').mode("overwrite").option('header', 'true').save('/opt/spark/data/resultadop2')


# Para fechar a Spark Session quando a aplicação terminar
spark.stop()
