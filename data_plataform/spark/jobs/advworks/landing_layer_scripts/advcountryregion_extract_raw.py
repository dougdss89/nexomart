from sys import exit
from traceback import print_exc

from configconnections_loader import load_config, validate_required_keys
from pyspark.sql import SparkSession


def load_source_connections():
    """Load source connections from .env file"""
    try:
        config = load_config()

        mssql_adventureworks = config.get("mssql_sources", {}).get("adventureworks", {})
        mssql_properties = mssql_adventureworks.get("properties", {})
        hadoop_config = config.get("hadoop_paths", {})

        validate_required_keys(mssql_adventureworks, ["url", "database"], "mssql_adventureworks")
        validate_required_keys(hadoop_config, ["minio_landing"], "hadoop_paths")

        spark = (
            SparkSession.builder.appName("extract_country_region_raw")
            .enableHiveSupport()
            .getOrCreate()
        )

        return spark, mssql_properties, mssql_adventureworks, hadoop_config.get("minio_landing")

    except Exception as e:
        print(f"Failed to create Spark Session: {str(e)}")
        print_exc()
        return None, None, None, None


def extract_raw_countryregion(spark, mssql_adventureworks, mssql_properties):
    """Extract country region data from SQL Server"""
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", mssql_adventureworks["url"])
            .option("driver", mssql_properties["driver"])
            .option("user", mssql_properties["user"])
            .option("password", mssql_properties["password"])
            .option(
                "query",
                """
                select distinct
                    sst.territoryid,
                    sst.[name],
                    sst.[group],
                    sst.countryregioncode,
                    getdate() as inserted_date,
                    '9999-31-12 00:00:00.000' as modified_date,
                    '1' as eltversion
                from sales.salesterritory as sst
                inner join person.countryregion as pcr
                    on pcr.countryregioncode = sst.countryregioncode
                left join person.stateprovince as psp
                    on psp.countryregioncode = pcr.countryregioncode
            """,
            )
            .load()
        )
        return df
    except Exception as e:
        print(f"Error extracting country region data: {str(e)}")
        print_exc()
        return None


def spark_stop(spark):
    try:
        if spark:
            spark.stop()
    except Exception as e:
        print(f"Error stopping Spark session: {str(e)}")


if __name__ == "__main__":
    spark = None
    try:
        spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()

        if not all([spark, mssql_properties, mssql_adventureworks, minio_landing]):
            raise RuntimeError("Failed to load required connections")

        df = extract_raw_countryregion(spark, mssql_adventureworks, mssql_properties)

        if df is None:
            raise RuntimeError("Failed to extract country region data")

        df.repartition(1).write.mode("overwrite").parquet(f"{minio_landing}/countryregion")

        print("Job completed successfully")

    except Exception as e:
        print(f"Job failed: {str(e)}")
        print_exc()
        exit(1)

    finally:
        spark_stop(spark)
