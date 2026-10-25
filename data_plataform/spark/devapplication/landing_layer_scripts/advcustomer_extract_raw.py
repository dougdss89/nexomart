from configconnections_loader import load_config
from pyspark.sql import SparkSession

# load .env config


def load_source_connections():
    """
    load source connections from .env file
    """

    try:
        config = load_config()
    except (FileNotFoundError, KeyError) as e:
        print(f"Could not load source connections from file: {str(e)}")
        return None, None, None, None

    # Validate required keys
    try:
        spark_jars = config["spark_jars"]
        mssql_adventureworks = config["mssql_sources"]["adventureworks"]
        mssql_properties = mssql_adventureworks["properties"]
        mssql_url = mssql_adventureworks["url"]
        minio_landing = config.get("hadoop_paths", {})

    except KeyError as e:
        print(f"Missing required configuration key: {str(e)}")
        return None, None, None, None

    # creating spark session
    spark = SparkSession.builder.appName("extract_customer_raw").enableHiveSupport().getOrCreate()
    return spark, mssql_properties, mssql_adventureworks, minio_landing.get("minio_landing", "")


spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()


# select data from table customer
def extract_raw_customer(**kwargs):
    df_customer_raw = (
        spark.read.format("jdbc")
        .option("url", mssql_adventureworks["url"])
        .option("driver", mssql_properties["driver"])
        .option("user", mssql_properties["user"])
        .option("password", mssql_properties["password"])
        .option(
            "query",
            """

                                select
                                    
                                    pp.businessentityid,
                                    sc.customerid,
                                    pp.firstname,
                                    pp.lastname,
                                    pp.persontype,
                                    pp.emailpromotion,
                                    ppt.[name] as typephone,
                                    pph.phonenumber,
                                    pad.postalcode,
                                    pat.name as addresstype,
                                    emailaddress,
                                    spcr.territoryid,
                                    spcr.CountryRegionName,
                                    pad.stateprovinceid,
                                    spcr.countryregioncode,
                                    spcr.stateprovincecode,
                                    spcr.stateprovincename as statename,
                                    pad.city,
                                    pad.addressline1,
                                    pad.addressline2,
                                    pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:TotalPurchaseYTD)[1]','numeric(12,2)') AS TotalPurchaseYTD,
                                    pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:DateFirstPurchase)[1]','date') AS DateFirstPurchase,
                                    pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:BirthDate)[1]','date') AS BirthDate,
                                    pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:MaritalStatus)[1]','varchar(10)') AS MaritalStatus,
                                    pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:YearlyIncome)[1]','varchar(50)') AS YearlyIncome,
                                    pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:Gender)[1]','varchar(10)') AS Gender,
                                    pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:TotalChildren)[1]','int') AS TotalChildren,
                                    pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:NumberChildrenAtHome)[1]','int') AS NumberChildrenAtHome,
                                    pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:Education)[1]','varchar(50)') AS Education,
                                    pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:Occupation)[1]','varchar(50)') AS Occupation,
                                    pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:HomeOwnerFlag)[1]','varchar(5)') AS HomeOwnerFlag,
                                    pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:NumberCarsOwned)[1]','int') AS NumberCarsOwned,
                                    getdate() as inserted_date,
                                    '9999-31-12 00:00:00.000' as modified_date,
                                    '1' as eltversion

                                from sales.customer as sc
                                    inner join
                                    person.Person as pp
                                on sc.personid = pp.BusinessEntityID
                                    inner join
                                    person.BusinessEntityAddress as pbea
                                on pp.businessentityid = pbea.businessentityid
                                    inner join
                                    person.[Address] as pad
                                on pbea.AddressID = pad.AddressID
                                    left join
                                    person.personphone as pph
                                on pp.BusinessEntityID = pph.BusinessEntityID
                                    inner join
                                    person.PhoneNumberType as ppt
                                on pph.PhoneNumberTypeID = ppt.PhoneNumberTypeID
                                    inner join
                                    person.AddressType as pat
                                on pbea.AddressTypeID = pat.AddressTypeID
                                    inner join
                                    person.EmailAddress as pea
                                on pp.BusinessEntityID = pea.BusinessEntityID
                                    inner join
                                    person.vStateProvinceCountryRegion as spcr
                                on pad.StateProvinceID = spcr.StateProvinceID
                             
                            """,
        )
    ).load()
    return df_customer_raw


write_customer = extract_raw_customer()

write_customer.write.mode("overwrite").parquet(f"{minio_landing}/customer")


def stop_spark(spark):
    spark.stop()


if __name__ == "__main__":
    load_source_connections()
    extract_raw_customer()
    stop_spark(spark)
