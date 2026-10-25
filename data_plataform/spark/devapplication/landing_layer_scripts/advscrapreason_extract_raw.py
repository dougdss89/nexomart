from sys import exit
from traceback import print_exc

from configconnections_loader import load_config, validate_required_keys
from pyspark.sql.session import SparkSession

# loading .env and key-values configurations


def load_source_connections():
    print("Import .env file and creating Spark Session")

    # loading configurations and validated it
    try:
        config = load_config()
        print("File .env loaded")

    except FileExistsError:
        print("File .env not founded: {str(e)}")

    except KeyError:
        print("Missing some key-values pair: {str(e)}")

    # importing key-values from .env
    # validating data imported
    # validating imported keys
    try:
        spark_jars = config.get("spark_jars", {})
        mssql_adventureworks = config.get("mssql_sources", {}).get("adventureworks", {})
        mssql_properties = mssql_adventureworks.get("properties", {})
        minio_landing = config.get("hadoop_paths", {})

        validate_required_keys(
            config=spark_jars, required_keys=["mssql_spark_jars"], config_name="spark_jars"
        )

        # validade properties key
        validate_required_keys(
            config=mssql_adventureworks,
            required_keys=["properties"],
            config_name="mssql_adventureworks",
        )

        validate_required_keys(
            config=minio_landing,  # validating hadoop_path key
            required_keys=["minio_landing"],  # check if key exists
            config_name="hadoop_paths",
        )

        spark = (
            SparkSession.builder.appName("extract_scrapreason_raw")
            .enableHiveSupport()
            .getOrCreate()
        )

        return spark, mssql_properties, mssql_adventureworks, minio_landing.get("minio_landing", "")

    except RuntimeError as e:
        print(f"Failed to create configuration: {str(e)}")
        print_exc()

        return None

    except Exception as e:
        print(f"Unexpected error occur: {str(e)}")
        print_exc()
        return None, None, None, None


def extract_scrapreason_raw(spark, mssql_properties, mssql_adventureworks):
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print("Missing configurations")

    try:
        df_scrapreason_raw = (
            spark.read.format("jdbc")
            .option("url", mssql_adventureworks["url"])
            .option("driver", mssql_properties["driver"])
            .option("user", mssql_properties["user"])
            .option("password", mssql_properties["password"])
            .option(
                "query",
                """ 
                                            select

                                                coalesce(scrapreasonid,0) as scrapreasonid,
                                                [name] as scrapname,
									            getdate() as inserted_date,
	                                            '9999-31-12 00:00:00.000' as modified_date,
                                                '1' as eltversion
                                                
                                            from production.scrapreason
                                        """,
            )
            .load()
        )

        return df_scrapreason_raw

    except RuntimeError as e:
        print(f"Fail to extract data from source: {str(e)} ")
        print_exc()
        return None

    except Exception as e:
        print(f"unexpected error encountered: {str(e)}")
        print_exc()
        return None, None, None, None


# stop spark session
def spark_stop(spark):
    if spark:
        spark.stop()


# creating main function
def main():
    spark = None

    try:
        spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()

        if not spark:
            print("Spark Session failed. Exiting application")
            print_exc()
            exit(1)

        df_scrapreason = extract_scrapreason_raw(spark, mssql_properties, mssql_adventureworks)

        if df_scrapreason:
            df_scrapreason.write.parquet(f"{minio_landing}/scrapreason", mode="overwrite")

            print(f"""Dataframe loaded successfully on destination:    
            {df_scrapreason}""")

    except Exception as e:
        print(f"Main script encontered error: {str(e)}")
        print_exc()

    finally:
        print("Stop spark session...")
        spark_stop(spark)

        print("Spark Session closed")


if __name__ == "__main__":
    main()
