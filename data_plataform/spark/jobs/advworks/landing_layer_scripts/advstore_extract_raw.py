from sys import exit
from traceback import print_exc

from configconnections_loader import load_config, validate_required_keys
from pyspark.sql.session import SparkSession


# starting load script
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

        spark = SparkSession.builder.appName("extract_store_raw").enableHiveSupport().getOrCreate()

        return spark, mssql_properties, mssql_adventureworks, minio_landing.get("minio_landing", "")

    except RuntimeError as e:
        print(f"Failed to create configuration: {str(e)}")
        print_exc()

        return None

    except Exception as e:
        print(f"Unexpected error occur: {str(e)}")
        print_exc()
        return None, None, None, None


def extract_store_raw(spark, mssql_properties, mssql_adventureworks):
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print("Error to load variables from environment")

    try:
        df_store_raw = (
            spark.read.format("jdbc")
            .option("url", mssql_adventureworks["url"])
            .option("driver", mssql_properties["driver"])
            .option("user", mssql_properties["user"])
            .option("password", mssql_properties["password"])
            .option(
                "query",
                """
                                select

                                    st.businessentityid as storeid,
                                    sc.PersonID,
                                    sc.CustomerID,
                                    sc.AccountNumber,
                                    st.[Name] as storename,
                                    pp.FirstName,
                                    pp.LastName,
                                    st.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:AnnualSales)[1]','numeric(12,2)') AS AnnualSales,
                                    st.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:AnnualRevenue)[1]','numeric(12,2)') AS AnnualRevenue,
                                    st.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:BankName)[1]','varchar(50)') AS BankName,
                                    st.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:BusinessType)[1]','varchar(20)') AS BusinessType,
                                    st.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:YearOpened)[1]','int') AS YearOpened,
                                    st.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:Specialty)[1]','varchar(10)') AS Specialty,
                                    st.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:SquareFeet)[1]','numeric(9,2)') AS SquareFeet,
                                    st.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:Brands)[1]','varchar(5)') AS Brands,
                                    st.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:Internet)[1]','varchar(5)') AS Internet,
                                    st.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:NumberEmployees)[1]','int') AS NumberEmployees,
                                    SalesPersonID,
                                    sst.territoryid,
                                    [group] continentname,
                                    sst.[name] as regionname,
                                    sst.CountryRegionCode,
									getdate() as inserted_date,
	                                '9999-31-12 00:00:00.000' as modified_date,
                                    '1' as eltversion

                                from sales.store as st
                                inner join
                                    sales.customer as sc
                                on st.BusinessEntityID = sc.StoreID
                                left join
                                    person.person as pp
                                on pp.BusinessEntityID = sc.PersonID
                                inner join
                                    sales.salesterritory as sst
                                on
                                sc.TerritoryID = sst.TerritoryID
                            """,
            )
            .load()
        )

        return df_store_raw

    except ConnectionRefusedError as e:
        print(f"Error to connect on database source: {str(e)}")
        print_exc()

    except TimeoutError as e:
        print(f"Timeout error. Verify connection network: {str(e)}")
        print_exc()

    except Exception as e:
        print(f"Some unexpected error ocurred: {str(e)}")
        print_exc()
        exit(1)


# stop spark session
def spark_stop(spark):
    if spark:
        spark.stop()


# main script function
def main():
    spark = None

    try:
        spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()

        if not spark:
            print(f"Fail to create Spark Session: {spark}")
            print_exc()
            exit(1)

        # create and write dataframe
        df_store = extract_store_raw(spark, mssql_properties, mssql_adventureworks)

        if df_store:
            df_store.write.parquet(f"{minio_landing}/store", mode="overwrite")
            print(f"""Dataframe loaded successfully on destination:
            {df_store}""")

    except RuntimeError as e:
        print(f"Error to execute script: {str(e)}")

    except ConnectionError as e:
        print(f"Error to connect on Data Lake Hadoop: {str(e)}")

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        print_exc
        exit(1)

    finally:
        spark_stop(spark)
        print("Spark Session closed")


if __name__ == "__main__":
    main()
    print("Script successfully executed")
