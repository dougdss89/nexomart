from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, rand
from configconnections_loader import load_source_connections
import time

def run_stress_test():
    print("Starting the Network Stress Test (MinIO/S3A)...")

    # 1. Loads the connections using your tidied-up module (4 variables)
    spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()

    if not spark or not minio_landing:
        print("Failed to load connections. Aborting test.")
        return

    try:
        start_time = time.time()

        # 2. Generates 50 million rows (approximately 10GB of data in memory)
        print("Generating 50 million rows of heavy synthetic data...")
        df_heavy = spark.range(0, 50000000) \
            .withColumn("uuid_aleatorio", expr("uuid()")) \
            .withColumn("metrica_teste", rand() * 1000) \
            .withColumn("texto_pesado", expr("repeat('EngenhariaDeDados_ApacheSpark_', 20)"))

        # 3. Forces 100 partitions to create 100 simultaneous connections to MinIO
        df_repartitioned = df_heavy.repartition(100)

        # 4. Writes to MinIO
        destination = f"{minio_landing}/stress_test/heavy_payload"
        print(f"Starting parallel write to: {destination}")

        df_repartitioned.write \
            .mode("overwrite") \
            .parquet(destination)

        end_time = time.time()
        print("ABSOLUTE SUCCESS! Wrote 50 million rows without IncompleteBody.")
        print(f"Total time: {round((end_time - start_time) / 60, 2)} minutes.")

    except Exception as e:
        print(f"The test failed! The network couldn't handle it: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        if spark:
            spark.stop()

if __name__ == '__main__':
    run_stress_test()