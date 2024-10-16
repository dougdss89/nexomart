# Projeto 4 - Configurando Um Cluster Spark Local Para Executar Pipelines de Machine Learning em Python
# Executando o Pipeline 1 de Machine Learning no Cluster Spark

# Imports
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Row

# Inicializa o Spark Session
spark = SparkSession.builder.appName("DSAPipeline1").getOrCreate()

# Carrega os dados
training = spark.read.format("libsvm").load("/opt/spark/data/dataset1.txt")

# Inicializa o modelo de Regressão Linear
lr = LinearRegression(featuresCol = 'features', labelCol = 'label', maxIter = 10, regParam = 0.3, elasticNetParam = 0.8)

# Treina o modelo
lrModel = lr.fit(training)

# Imprime os coeficientes e intercepta para a regressão linear
print("Coeficientes: " + str(lrModel.coefficients))
print("Intercepto: " + str(lrModel.intercept))

# Converte os coeficientes para um DataFrame e salva em um arquivo CSV
coef = [Row(Coeficientes = float(i)) for i in lrModel.coefficients.toArray()]
df = spark.createDataFrame(coef)
df.coalesce(1).write.format('com.databricks.spark.csv').mode("overwrite").option('header', 'true').save('/opt/spark/data/resultadop1')

# Para fechar a Spark Session quando a aplicação terminar
spark.stop()
