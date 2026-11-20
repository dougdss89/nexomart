import argparse

from pyspark.sql import SparkSession

from spark.src.domains.person.bronze.raw_customer import ingestion_customer_raw
from spark.src.domains.person.gold.customer import ingestion_customer_gold
from spark.src.domains.person.silver.customer import ingestion_customer_silver

LAYERS = ("bronze", "silver", "gold")

def main() -> None:
    
    parser = argparse.ArgumentParser(description="person.customer medallion layer runner")
    parser.add_argument("--layer", choices=LAYERS, required=True)
    parser.add_argument("--reference-date", default=None, help="YYYY-MM-DD, gold only")
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName(f"person_customer_{args.layer}").getOrCreate() # type: ignore
    
    try:
       
        if args.layer == "bronze":
            result = ingestion_customer_raw(spark)
            
        elif args.layer == "silver":
            result = ingestion_customer_silver(spark)
            
        else:
            result = ingestion_customer_gold(spark, reference_date=args.reference_date)
            
        print(f"[{args.layer}] {result}")
    
    finally:
        spark.stop()
        
        
if __name__ == "__main__":
    main()