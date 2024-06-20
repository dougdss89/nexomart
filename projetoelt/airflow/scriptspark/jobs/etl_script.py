from pyspark.sql import SparkSession

spark = ((
            SparkSession
                .builder
                .appName('etl_script')
            .getOrCreate() 
          
          ))


data = spark.read.csv('/opt/spark/data/steam-games.csv',
                      header= True,
                      inferSchema= True)

transformed_data = data.withColumn('recent_review_double', data['recent_review_count'] * 2)


write_final_data = transformed_data.write.csv('/opt/spark/data/steam-games_final.csv',
                                               header= True
                                               )


spark.stop()

