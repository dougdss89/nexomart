from pyspark.sql.session import SparkSession
from configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

#starting load script
def load_source_connections():
    
    print("Import .env file and creating Spark Session")
    
    #loading configurations and validated it
    try:
        config = load_config()
        print("File .env loaded")
        
        
    except FileExistsError as e:
        print("File .env not founded: {e}")
        
    except KeyError as e:
        print("Missing some key-values pair: {e}")
        
    #importing key-values from .env
    #validating data imported
    #validating imported keys
    try:
        spark_jars = config.get("spark_jars", {})
        mssql_adventureworks = config.get("mssql_sources", {}).get("adventureworks", {})
        mssql_properties = mssql_adventureworks.get("properties", {})
        hadoop_landing = config.get("hadoop_paths", {})

        validate_required_keys(config=spark_jars, 
                               required_keys=["mssql_spark_jars"],
                               config_name="spark_jars")
        
        #validade properties key
        validate_required_keys(config= mssql_adventureworks,
                               required_keys=["properties"],
                               config_name="mssql_adventureworks")
        
        validate_required_keys(config=hadoop_landing,
                               required_keys=["landing"],
                               config_name="hadoop_landing")
        
        spark = (SparkSession
                    .builder
                    .appName("extract_store_raw")
                        .config("spark.jars", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.executor.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.driver.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .enableHiveSupport()
                    .getOrCreate())
        
        return spark, mssql_properties, mssql_adventureworks, hadoop_landing.get("landing")
    
    except RuntimeError as e:
        print(f"Failed to create configuration: {e}")
        print_exc()
        
        return None
    
    except Exception as e:
        print(f"Unexpected error occur: {e}")
        print_exc()
        return None, None, None
        
def extract_store_raw(spark, mssql_properties, mssql_adventureworks):
    
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print(f"Error to load variables from environment")
        
    try:
        df_store_raw = (spark
                            .read
                            .format("jdbc")
                            .option("url", mssql_adventureworks["url"])
                            .option("driver", mssql_properties["driver"])
                            .option("user", mssql_properties["user"])
                            .option("password", mssql_properties["password"])
                            .option("query",
                                
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
                            """
                            ).load())
        
        return df_store_raw
    
    except ConnectionRefusedError as e:
        print(f"Error to connect on database source: {e}")
        print_exc()
        
    except TimeoutError as e:
        print(f"Timeout error. Verify connection network: {e}")
        print_exc()
        
    except Exception as e:
        print(f"Some unexpected error ocurred: {e}")
        print_exc()
        exit(1)

#stop spark session
def spark_stop(spark):
    if spark:
        spark.stop()
        

#main script function
def main():
    
    spark = None
    
    try:
        spark, mssql_properties, mssql_adventureworks, hadoop_landing = load_source_connections()
        
        if not spark:
            print(f"Fail to create Spark Session: {spark}")
            print_exc()
            exit(1)
            
        #create and write dataframe 
        df_store = extract_store_raw(spark, mssql_properties, mssql_adventureworks)
        
        if df_store:
            df_store.write.parquet(f"{hadoop_landing}/store", mode="overwrite")
            print(f"""Dataframe loaded successfully on destination:    
            {df_store}""")
            
    except RuntimeError as e:
        print(f"Error to execute script: {e}")
        
    except ConnectionError as e:
        print(f"Error to connect on Data Lake Hadoop: {e}")
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        print_exc
        exit(1)
        
    finally:
        spark_stop(spark)
        print("Spark Session closed")
        
if __name__ == "__main__":
    main()
    print(f"Script successfully executed")
