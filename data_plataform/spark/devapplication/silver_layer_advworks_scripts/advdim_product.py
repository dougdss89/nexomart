from startSparkSession import SparkSessionManager
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from configconnections_loader import load_config, validate_required_keys
import logging
from traceback import print_exc


from py4j.protocol import Py4JJavaError #java exception error


#logging 
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

#import env configuration

def load_source_configurations(spark_application: str):
    
    try:
        config = load_config()
        
        iceberg = config.get("hadoop_paths", {})
        iceberg_db = config.get("hadoop_paths", {})
        iceberg_catalog = config.get("hadoop_paths", {})
        
        validate_required_keys(
            config = iceberg,
            required_keys= ["raw_icebergdb", "silver_icebergdb", "iceberg_catalog"],
            config_name = "hadoop_paths"
        )
        
        spark_manager = SparkSessionManager(spark_application)
        
        spark = spark_manager.create_session()
        
        return spark, iceberg.get("raw_icebergdb", ""), iceberg_db.get("silver_icebergdb", ""), iceberg_catalog.get("iceberg_catalog", "")

    except FileNotFoundError as e:
        logger.error(f"Could not load connections from source: {str(e)}")
        print_exc()
        exit(1)
        
    except KeyError as e:
        logger.error(f"key value pair error. Value not found. Verify: {str(e)}")
        print_exc()
        exit(1)
        
    except RuntimeError as e:
        logger.error(f"Failed to create Spark Session. Runtime Error: {str(e)}")
        print_exc()
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occurred. {str(e)}")
        print_exc()
        exit(1)
        
    logger.info(f"Spark Session created: {spark}")
        
    logger.info(f"Spark Session created: {spark}")
    
def getCreateSurrogateKey(spark, catalog:str, iceberg:str, table_name:str) -> DataFrame:

    
    try:
        
        raw_lakehouse = f"{catalog}.{iceberg}.{table_name}"
        
        dataframe = spark.read.format("iceberg").load(raw_lakehouse)
        
        dataframe.createTempView("temp_product")
                
        dataframe = spark.sql(""" 
                                select 
                                    row_number() over(order by productid) as productkey,                                    
                                    *
                                from temp_product
                            """)
        
        logger.info(f"The data source were successfully extracted: {dataframe}")
        
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except RuntimeError as e:
        logger.error(f"Invalid runtime error: {str(e)}")
        print_exc(e)
        exit(1)   
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)
    
def transformProductName(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        color = dataframe.select(collect_set("color").alias("colors")).first()["colors"]

        regex_pattern = r"\b("+ "|".join(color) +r")\b"

        dataframe = (dataframe
                        .withColumn("productname", split(col("productname"), ",")[0])
                        .withColumn("productname", regexp_replace(col("productname"), regex_pattern, ""))
                        .withColumn("productname", regexp_replace(col("productname"), " -", ""))
                    )
        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)    

def transformProductModel(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                        .withColumn("productnumber", substring_index(col("productnumber"), "-", 2))
        )
        
        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)
        
def transformProductClass(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                        .withColumn("class", 
                            expr(""" 
                                case
                                    when productcategoryid is null and productsubcategoryid is null then "Discontinued"
                                    when trim(rtrim(class)) = "L" then "Light"
                                    when trim(rtrim(class)) = "M" then "Medium"
                                    when trim(rtrim(class)) = "H" then "Heavy"
                                else "Small"
                                end as class
                            """))
                )
        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)
        
def transformProductSize(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe 
                    .withColumn("size",
                        expr(""" 
                                case	
                                    when ProductSubcategoryID is null and ProductCategoryID is null then 'Discontinued'
                                    when Size = 'L' then 'Large'
                                    when Size = 'M' then 'Medium'
                                    when Size = 'Xl' then 'Extra Large'
                                    when Size = 'S' then 'Small'
                                    when Size <= '42' then 'Small'
                                    when Size > '42' and Size <= '50' then 'Medium'
                                    when Size > '50' and Size <= '60' then 'Large'
                                    when Size > '60' then 'Extra Large'
                                else 'Small Piece'
                                end as size
                            """))
                    )
        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)

def transformProductLine(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                        .withColumn("productline",
                            expr(""" 
                                case	
                                    when ProductSubcategoryID is null and ProductCategoryID is null then 'Discontinued'
                                    when trim(ProductLine) = 'R' then 'Road'
                                    when trim(ProductLine) = 'S' then 'Sport'
                                    when trim(ProductLine) = 'M' then 'Mountain'
                                    when trim(ProductLine) = 'T' then 'Touring'
                                else 'Accessories'
                                end as productline
                            """)                           
                        ))
        
        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)
        
def transformProductColor(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe 
                    .withColumn("color", 
                        expr(""" 
                            case
                                when ProductSubcategoryID is null and ProductCategoryID is null then 'Discontinued'
                                when Color is null then 'Multicolor'
                                when color = 'Multi' then 'Multicolor'
                                when Color like '%/%' then 'Multicolor'
                            else color
                            end as color
                        """))
                    )
        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)

def transformProductStyle(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                    .withColumn("style", 
                        expr(""" 
                            case
                                when trim(rtrim(style)) = "M" then "Masculine"
                                when trim(rtrim(style)) = "W" then "Feminine"
                            else "Unissex"
                            end as style
                        """))
                )
        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)
        
def transformProductModelName(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe 
                    .withColumn("modelname", 
                                expr(""" 
                                    case
                                        when ProductSubcategoryID is null and ProductCategoryID is null then 'Discontinued'
                                        when modelname like 'LL%' then REPLACE(modelname, 'LL', 'Light')
                                        when modelname like 'ML%' then REPLACE(modelname, 'ML', 'Medium')
                                        when modelname like 'HL%' then REPLACE(modelname, 'HL', 'Heavy')
                                    else modelname
                                    end as modelname
                                """)))

        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)
        
def transformProductMeasureCode(dataframe: DataFrame) -> DataFrame: 
    
    try:
        
        dataframe = (dataframe 
                    .withColumn("weightunitmeasurecode", 
                              expr("""	
                                   case 
                                        when trim(rtrim(weightunitmeasurecode)) is null then 'Not Available'
                                        when trim(rtrim(weightunitmeasurecode)) = 'G' then 'Grams'
                                        when trim(rtrim(weightunitmeasurecode)) = 'LB' then 'Pounds'
                                   else weightunitmeasurecode
                                   end as weightunitmeasurecode
                              """))
                    )
        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)
        
def transformProductManufClass(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe = (dataframe
                        .withColumn("manufacturyclass",
                            expr(""" 
                                case 
                                    when DaysToManufacture <= '1' then 'Fast'
                                    when DaysToManufacture > '1' and DaysToManufacture <= '3' then 'Normal'
                                    else 'Slowly'
                                end as manufactureclass
                            """))
                        )
        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)
        
def transformProductSize(dataframe: DataFrame) -> DataFrame:
    
    try:
        dataframe = (dataframe
                        .withColumn("sizemeasure", 
                            expr(""" 
                                case
                                    when trim(rtrim(sizeunitmeasurecode)) = "CM" then "Centimeters"
                                    else "Not Available"
                                end as sizemeasure
                            """))
                        )
        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)
        
def transformProductNull(dataframe: DataFrame) -> DataFrame:
    
    try:
        
        dataframe = (dataframe
                        .fillna({
                                "productsubcategoryid": 0,
                                "subcategoryname":      "Discontinued",
                                "productcategoryid":    0,
                                "productmodelid":       0,
                                "sellenddate":          "2100-12-31",
                                "productsubcategoryid": 0,
                                "subcategoryname":      "Discontinued",
                                "productcategoryid":    0,
                                "categoryname":         "Discontinued",
                                "productmodelid":       0,
                                "sellenddate":          "2100-12-31",
                                "weight":               0.00})
                        )
        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)
        
def transformProductColumn(dataframe: DataFrame) -> DataFrame:
    
    try:
            
        dataframe = dataframe.select(
                            "productkey",
                            "ProductID",                "productname",
                            "productnumber",            "productsubcategoryid",
                            "subcategoryname",          "productcategoryid",
                            "categoryname",             "color",
                            "class",                    "size",
                            "productline",              "productmodelid",
                            "modelname",                "style",
                            "listprice",                "standardcost",
                            "manufacturyclass",         "sizemeasure",
                            "safetystocklevel",         "reorderpoint",
                            "weightunitmeasurecode",    "weight",
                            "daystomanufacture",        "sellstartdate",
                            "sellenddate",              "inserted_date",
                            "modified_date",            "eltversion")
        
        logger.info(f"The data source were successfully extracted: {dataframe}")
        return dataframe
    
    except TimeoutError as e:
        logger.error(f"Connection timeout. Please reconnect: {str(e)}")
        print_exc(e)
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. Verify: {str(e)}")
        print_exc(e)
        exit(1)

def writeProductDimension(dataframe:DataFrame, catalog:str, silver_icebergdb:str, table_name:str) -> DataFrame:
    
    try:
        lakehouse = f"{catalog}.{silver_icebergdb}.{table_name}"

        (
            dataframe
                .write
                .mode("append")
                .format("iceberg")
                .saveAsTable(f"{lakehouse}")
        )
        
        return logger.info(f"Table writing successfully completed to iceberg: {table_name}")
    
    except Py4JJavaError as e:
        logger.error(f"Java/Scala error while writing table {table_name} to iceberg: {str(e)}")
        return "Py4JJavaError"

    except Exception as e:
        logger.error(f"Unexpected error while writing table {table_name} to iceberg: {str(e)}")
        return "UnexpectedError"
            
def sparkStop(spark) -> str:
    try:
        if spark:
            spark.stop()

        return f"Spark Session stoped Successfully: {spark}"
    
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {str(e)}")
        print_exc(e)
        
def main():
    
    try:
        application_name = "dimension_product_table"
        table_name = "raw_dimproduct"
        dimension_table = "dim_product"
        
        
        spark, iceberg, iceberg_db, iceberg_catalog = load_source_configurations(application_name)
        
        dataframe = getCreateSurrogateKey(spark, iceberg_catalog, iceberg, table_name)
        logger.info(f"Dimension surrogate key created: {dataframe.schema}")
        
        dataframe = transformProductName(dataframe)
        logger.info(f"Column Business Type processed successfully: {dataframe.schema}")
        
        dataframe = transformProductModel(dataframe)
        logger.info(f"Revenue Column processed successfully: {dataframe.schema}")
        
        dataframe = transformProductClass(dataframe)
        logger.info(f"Brand column processed successfully: {dataframe.schema}")
        
        dataframe = transformProductLine(dataframe)
        logger.info(f"Columns processed successfully: {dataframe.schema}")
        
        dataframe = transformProductColor(dataframe)
        logger.info(f"Brand column processed successfully: {dataframe.schema}")
        
        dataframe = transformProductStyle(dataframe)
        logger.info(f"Columns processed successfully: {dataframe.schema}")
        
        dataframe = transformProductMeasureCode(dataframe)
        logger.info(f"Brand column processed successfully: {dataframe.schema}")
        
        dataframe = transformProductManufClass(dataframe)
        logger.info(f"Columns processed successfully: {dataframe.schema}")
        
        dataframe = transformProductSize(dataframe)
        logger.info(f"Columns processed successfully: {dataframe.schema}")
        
        dataframe = transformProductNull(dataframe)
        logger.info(f"Brand column processed successfully: {dataframe.schema}")
        
        dataframe = transformProductColumn(dataframe)
        logger.info(f"Columns processed successfully: {dataframe.schema}")

        writeProductDimension(dataframe, catalog=iceberg_catalog, silver_icebergdb= iceberg_db, table_name= dimension_table)
        logger.info(f"""
                        Dataframe has been writted successfully: {dimension_table}.
                        Storage Path: {iceberg_db}.{dimension_table}
                    """)
        
        sparkStop(spark)
        logger.info("Spark Session stopped")
        
        logger.info("All functions have been executed successfully")
        return "Success"
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return "UnexpectedError"
    
    finally:
        # Ensures the Spark session is closed, even in case of error
        if 'spark' in locals():
            sparkStop(spark)
            
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully") 