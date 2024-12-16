from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from spark.jobs.configconnections_loader import load_config
from sys import exit
from traceback import print_exc

#start to load env file

def load_source_connections():
    
    """ load .env file and create spark session """

    try:
        print("Loading .env file")
        config = load_config()
        
        print("Env file configuration was loaded")
        
    except FileNotFoundError as e:
        print(f"File .env not founded. Please verify file path: {e}")
        return None
    
    except KeyError as e:
        print(f"Failed to load key-values pair: {e}")
        return None
    
    #load properties, jars an create session
    
    try:
        spark_jars = config["spark_jars"]
        mssql_adventureworks = config["mssql_sources"]["adventureworks"]
        mssql_properties = mssql_adventureworks["properties"]
        
        required_keys = ['mssql_spark_jars', 'postgresql_jar']
       
        if not spark_jars or not all(key in spark_jars for key in required_keys):
            raise RuntimeError("Missing required Spark Jars configuration: mssql_spark_jar or postgresql_jar")
        
        if not mssql_adventureworks:
            print(f"Adventureworks sources and properties wasn't loaded. Missing properties {mssql_adventureworks}")
            return None, None, None
        
        if not mssql_properties:
            print(f"We detected certain dependencies on {mssql_properties} that was not loaded. Reload {mssql_adventureworks} and retry it")
            return None, None, None
        
        spark = (SparkSession
                    .builder
                    .appName('date_extract_raw')
                    .config('spark.jars', f"{spark_jars.get('mssql_spark_jars', '')},{spark_jars.get('postgresql_jar', '')}")
                    .config('spark.driver.extraClassPath', f"{spark_jars.get('mssql_spark_jars', '')},{spark_jars.get('postgresql_jar', '')}")
                    .config('spark.executor.extraClassPath', f"{spark_jars.get('mssql_spark_jars', '')},{spark_jars.get('postgresql_jar', '')}")
                    .enableHiveSupport()
                    .getOrCreate()
                )
        return spark, mssql_properties, mssql_adventureworks
    
    except Exception as e:
        print(f'Fail to create SparkSession: {e}')
        print_exc()
        return None
    
def extract_date_raw(spark, mssql_properties, mssql_adventureworks):
    
    if not all ([spark, mssql_properties, mssql_adventureworks]):
        print("Missing some connection and session variables. Invalid Parameters")
        
        return None
    
    try:
    
        df_date_raw = (spark
                        .read
                        .format('jdbc')
                        .option('url', mssql_adventureworks['url'])
                        .option('driver', mssql_properties['driver'])
                        .option('user', mssql_properties['user'])
                        .option('password', mssql_properties['password'])
                        .option('query', 
                                
                                """ 
                                    select

                                        distinct(cast(orderdate as date)) as orderdate

                                    from sales.salesorderheader
                                """

                                    
                                    )).load()
        return df_date_raw
    
    except Exception as e:
        print("Failed to load datasource to dataframe")
        print_exc()    
        return None

def update_dimdate(spark, dataframe):
    
    dataframe.createOrReplaceTempView('dimdate')
    
    # Registra o DataFrame como uma view temporária
    # dataframe_date.createOrReplaceTempView("dimdate")
    
    # Executa a consulta SQL
    tempdataframe = spark.sql("""
        SELECT
            CAST(orderdate AS DATE) AS datekey,
            CAST(orderdate AS DATE) AS orderdatekey,
            CAST(orderdate AS DATE) AS orderdate,
            YEAR(orderdate) AS orderyear,
            CONCAT('FY', YEAR(orderdate)) AS fiscalyear,
            date_format(orderdate, 'MMMM') AS monthname,
            date_format(orderdate, 'MMM') AS monthabrv,
            MONTH(orderdate) AS monthnum,
            
            CASE
                WHEN MONTH(orderdate) BETWEEN 1 AND 3 THEN 'Q1'
                WHEN MONTH(orderdate) BETWEEN 4 AND 6 THEN 'Q2'
                WHEN MONTH(orderdate) BETWEEN 7 AND 9 THEN 'Q3'
                WHEN MONTH(orderdate) BETWEEN 10 AND 12 THEN 'Q4'
            END AS quartermonth,
            
            date_format(orderdate, 'EEEE') AS weekname,
            date_format(orderdate, 'EEE') AS weeknameabrv,
            
            CASE 
                WHEN date_format(orderdate, 'EEEE') IN ('Saturday', 'Sunday') THEN 'Weekend'
                ELSE 'Weekday'
            END AS isweekday,
            
            WEEKOFYEAR(orderdate) AS weeknuminyear,
            DAY(orderdate) AS daynum,
            
            CASE 
                WHEN DAY(orderdate) = 1 THEN 'Yes'
                ELSE 'No'
            END AS ismonthbegin,
            
            CASE
                WHEN date_format(orderdate, 'EEEE') = 'Monday' THEN 'Yes'
                ELSE 'No'
            END AS isweekbegin,
            
            CASE
                WHEN date_format(orderdate, 'EEEE') = 'Monday' THEN CAST(orderdate AS DATE)
                ELSE NULL
            END AS weekbegindate,
            
            UPPER(CONCAT(YEAR(orderdate), '_', date_format(orderdate, 'MMM'))) AS yearmonth,
            CONCAT(YEAR(orderdate), LPAD(MONTH(orderdate), 2, '0')) AS yearmonthnum,
            LAST_DAY(orderdate) AS lastdayofmonth,
            
            CASE
                WHEN DAY(orderdate) = DAY(LAST_DAY(orderdate)) THEN 'Yes'
                ELSE 'No'
            END AS islastdayofmonth,
            
            CASE	
                WHEN YEAR(orderdate) % 4 = 0 AND (YEAR(orderdate) % 100 != 0 OR YEAR(orderdate) % 400 = 0) THEN 'Yes'
                ELSE 'No'
            END AS leapyear
        FROM dimdate
    """)

    # Remove a view temporária
    spark.sql("DROP VIEW dimdate")

    # Retorna o novo DataFrame
    dataframe = tempdataframe
    return dataframe
    
    
def stop_spark(spark):
    if spark:
        spark.stop()
        
def main():
    
        #load source connections to main
    try:
        spark, mssql_properties, mssql_adventureworks = load_source_connections()
        
    #verifying spark
        if not spark:
            print("Fail to stablishes Spark Session")
            exit(1)
            
    #extract and check if datasources was loaded to dataframe
        dataframe_date = extract_date_raw(spark, mssql_properties, mssql_adventureworks)
        
        df_date_raw = update_dimdate(spark, dataframe_date)
    
        if df_date_raw:
            df_date_raw.repartition(2).write.mode('overwrite').parquet('hdfs://ha-cluster/raw/date')
        
            print('Dataframe was loaded sucessfully')
           
    except Exception as e:
        print("Main script was not properly configurated. Verify")
        print_exc()
        
        return None
    
    finally:
        #closing spark session
        stop_spark(spark)
        print("Spark Session closed")
        
if __name__ == '__main__':
    main() #executing script 