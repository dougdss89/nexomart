from pyspark.sql import SparkSession
from modules.configconnections_loader import load_config


# load .env config

def load_source_connections():
    """
        load source connections from .env file
    """
    
    try:
        config = load_config()
    except (FileNotFoundError, KeyError) as e:
        print(f"Could not load source connections from file: {e}")
        return None, None, None

    # Validar chaves necessárias
    try:
        spark_jars = config["spark_jars"]
        mssql_adventureworks = config["mssql_sources"]["adventureworks"]
        mssql_properties = mssql_adventureworks["properties"]
        mssql_url = mssql_adventureworks["url"]
        hadoop_landing = config.get("hadoop_paths", {}).get("landing", "")
        
    except KeyError as e:
        print(f"Missing required configuration key: {e}")
        return None, None, None
    
    #creating spark session    
    spark = (SparkSession
                .builder
                .appName("extract_customer_raw")
                    .config('spark.jars', config["spark_jars"]["mssql_spark_jars"])
                    .config("spark.driver.extraClassPath", config["spark_jars"]["mssql_spark_jars"])
                    .config("spark.executor.extraClassPath", config["spark_jars"]["mssql_spark_jars"])
                    
                    #parquet configuration
                    .config("spark.sql.parquet.compression.codec", "snappy")
                    .config("spark.sql.parquet.write.compression.codec", "snappy")
                    .config("parquet.block.size", "134217728")  # Tamanho do bloco (128 MB recomendado para HDFS)
                    .config("parquet.page.size", "1048576")  # Tamanho da página (1 MB recomendado)
                    .config("spark.sql.files.maxPartitionBytes", "134217728")  # Máximo de bytes por partição
                    .config("spark.sql.files.openCostInBytes", "4194304")  # Custo estimado de abrir arquivos (4 MB)
                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # Overwrite apenas em partições específicas
                    .config("spark.hadoop.dfs.replication", "1")  # Número de réplicas padrão
                    #write configuration
                    .config('spark.sql.files.maxRecordsPerFile', '1000000')
                    .config('spark.sql.shuffle.partitions', '200')
                    
                    #minimize overhead reads
                    .config('spark.sql.hive.metastorePartitionPruning', "true")
                    .config('spark.sql.parquet.filterPushdown', 'true')
                    
                .getOrCreate())

    
    return spark, mssql_properties, mssql_adventureworks, hadoop_landing

spark, mssql_properties, mssql_adventureworks, hadoop_landing = load_source_connections()

#select data from table customer
def extract_raw_customer(**kwargs):
    
    df_customer_raw = (spark
                       .read
                        .format("jdbc")
                        .option("url", mssql_adventureworks['url'])
                        .option("driver", mssql_properties['driver'])
                        .option('user', mssql_properties['user'])
                        .option('password', mssql_properties['password'])
                        .option('query',
                               
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
                                	psp.territoryid,
                                	pcr.[name] as countryname,
                                	pad.stateprovinceid,
                                	psp.countryregioncode,
                                	psp.stateprovincecode,
                                	psp.[name] as statename,
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

                                from  sales.customer as sc
                                left join 
                                	person.person as pp
                                on sc.personid = pp.businessentityid
                                left join
                                	person.personphone as pph
                                on pp.businessentityid = pph.businessentityid
                                left join
                                	person.phonenumbertype as ppt
                                on pph.phonenumbertypeid = ppT.phonenumbertypeid
                                left join
                                	person.businessentityaddress as pba
                                on pp.businessentityid = pba.businessentityid
                                left join
                                	person.[address] as pad
                                on pba.addressid = pad.addressid
                                left join
                                	person.addresstype as pat
                                on pba.addresstypeid = pat.addresstypeid
                                left join
                                	person.emailaddress as pea
                                on pp.businessentityid = pea.businessentityid
                                left join
                                	person.stateprovince as psp
                                on pad.stateprovinceid = psp.stateprovinceid
                                left join
                                	person.countryregion as pcr
                                on psp.countryregioncode = pcr.countryregioncode
                                where pp.businessentityid not in (select businessentityid
                                									from [HumanResources].[Employee])
                             
                            """
                       )).load()
    return df_customer_raw

write_customer = extract_raw_customer()

write_customer.repartition(2).write.mode("overwrite").parquet(f"{hadoop_landing}/customer")

def stop_spark(spark):
    spark.stop()
    
if __name__=='__main__':
    
    load_source_connections()
    extract_raw_customer()
    stop_spark(spark)