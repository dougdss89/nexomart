from sys import exit
from traceback import print_exc

from configconnections_loader import load_config, validate_required_keys
from pyspark.sql.session import SparkSession

# load connections from env variables


def load_source_connections():
    try:
        config = load_config()
        print("Configurations loaded successfully")

    except FileNotFoundError as e:
        print(f"File .env not found. Impossible to load environment variables: {str(e)}")
        print_exc()
        return None

    except KeyError as e:
        print(f"Missing key-values pair: {str(e)}")
        print_exc()
        return None

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
            SparkSession.builder.appName("extract_fact_sales_raw").enableHiveSupport().getOrCreate()
        )

        return spark, mssql_properties, mssql_adventureworks, minio_landing.get("minio_landing", "")

    except RuntimeError as e:
        print(f"Fail to create Spark Session. Run time error: {str(e)}")
        print_exc()
        return None

    except Exception as e:
        print(f"Unexpected error ocurred. Please verify: {str(e)}")
        print_exc()
        exit(1)
        return None, None, None, None


# loading data from sources
def extract_fact_sales_raw(spark, mssql_properties, mssql_adventureworks):
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print("Missing key configurations or Spark Session.")
        exit(1)

    try:
        df_fact_sales_raw = (
            spark.read.format("jdbc")
            .option("url", mssql_adventureworks["url"])
            .option("driver", mssql_properties["driver"])
            .option("user", mssql_properties["user"])
            .option("password", mssql_properties["password"])
            .option(
                "query",
                """ 
                                        select

                                            sso.SalesOrderID,
                                            sso.SalesOrderDetailID,
                                            accountnumber,
                                            carriertrackingnumber,
                                            purchaseordernumber,
                                            salesordernumber,
                                            soh.OrderDate,
                                            soh.ShipDate,
                                            soh.DueDate,
                                            sohs.salesreasonid,
                                            SalesPersonID,
                                            ProductID,
                                            CustomerID,
                                            ShipMethodID,
                                            TerritoryID,
                                            SpecialOfferID,
                                            currencyrateid,
                                            cast(orderqty as int) as orderqty,
                                            unitprice,
                                            unitpricediscount,
                                            taxamt,
                                            freight,
                                            cast(OnlineOrderFlag as int) as onlineorderflag,
                                            LineTotal,
                                            getdate() as inserted_date,
											'9999-12-31 00:00:00.000' as modified_date,
											'1' as eltversion
                                                                            from sales.salesorderdetail as sso

                                        left join

                                            sales.salesorderheader as soh
                                        on sso.SalesOrderID = soh.SalesOrderID

                                        left join
                                            sales.SalesOrderHeaderSalesReason as sohs

                                        on soh.salesorderid = sohs.salesorderid
                                    """,
            )
            .load()
        )

        return df_fact_sales_raw

    except ConnectionError as e:
        print(f"Error to connect on database. Fail to extract data: {str(e)}")
        print_exc()
        return None

    except TimeoutError as e:
        print(f"Timeout error to connect on database: {str(e)}")
        print_exc()
        return None
    except Exception as e:
        print(f"Unexpected error ocurred: {str(e)}")
        print_exc()
        exit(1)
        return None, None, None, None


# stop spark_session
def spark_stop(spark):
    try:
        spark.stop()
        print("Spark Session closed")

    except TimeoutError as e:
        print(f"Spark Session not founded: {str(e)}")
        print_exc()
        return None

    except Exception as e:
        print(f"Unexpected error ocurred: {str(e)}")
        print_exc()
        return None, None, None, None


# main script
def main():
    spark = None

    try:
        spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()

        if not spark:
            print("Spark Session not founded.")
            print_exc()
            exit(1)

        df_sales = extract_fact_sales_raw(spark, mssql_properties, mssql_adventureworks)

        if df_sales:
            df_sales.write.parquet(f"{minio_landing}/fact_sales", mode="overwrite")

            print(f"""Dataframe loaded successfully on destination:    
            {df_sales}""")

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
