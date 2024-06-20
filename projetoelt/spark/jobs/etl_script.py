from pyspark.sql import SparkSession

spark = ((
            SparkSession
                .builder
                .appName('etl_script')
            .getOrCreate() 
          
          ))


data = spark.read.csv('/mnt/d/linux/project/projetoelt/airflow/scriptspark/dados/steam-games.csv',
                      header= True,
                      inferSchema= True)

transformed_data = data.withColumn('recent_review_double', data['recent_review_count'] * 2)


write_final_data = transformed_data.write.csv('/mnt/d/linux/project/projetoelt/airflow/scriptspark/dados/steam-games_final.csv',
                                               header= True
                                               )


spark.stop()

