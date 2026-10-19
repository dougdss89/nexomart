import random
import string
from datetime import datetime, timedelta

from pyspark.sql.session import SparkSession
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

spark = (
    SparkSession.builder.appName("spark_joins")
    .config("spark.driver.maxResultSize", "4g")
    .config("spark.executor.memory", "6g")
    .config("spark.executor.cores", "4")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

sc = spark.sparkContext

# Define schemas for the TPC-H tables
schemas = {
    "nation": StructType(
        [
            StructField("nationkey", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("regionkey", IntegerType(), False),
            StructField("comment", StringType(), True),
        ]
    ),
    "region": StructType(
        [
            StructField("regionkey", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("comment", StringType(), True),
        ]
    ),
    "part": StructType(
        [
            StructField("partkey", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("mfgr", StringType(), False),
            StructField("brand", StringType(), False),
            StructField("type", StringType(), False),
            StructField("size", IntegerType(), False),
            StructField("container", StringType(), False),
            StructField("retailprice", FloatType(), False),
            StructField("comment", StringType(), True),
        ]
    ),
    "supplier": StructType(
        [
            StructField("suppkey", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("address", StringType(), False),
            StructField("nationkey", IntegerType(), False),
            StructField("phone", StringType(), False),
            StructField("acctbal", FloatType(), False),
            StructField("comment", StringType(), True),
        ]
    ),
    "partsupp": StructType(
        [
            StructField("partkey", IntegerType(), False),
            StructField("suppkey", IntegerType(), False),
            StructField("availqty", IntegerType(), False),
            StructField("supplycost", FloatType(), False),
            StructField("comment", StringType(), True),
        ]
    ),
    "customer": StructType(
        [
            StructField("custkey", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("address", StringType(), False),
            StructField("nationkey", IntegerType(), False),
            StructField("phone", StringType(), False),
            StructField("acctbal", FloatType(), False),
            StructField("mktsegment", StringType(), False),
            StructField("comment", StringType(), True),
        ]
    ),
    "orders": StructType(
        [
            StructField("orderkey", IntegerType(), False),
            StructField("custkey", IntegerType(), False),
            StructField("orderstatus", StringType(), False),
            StructField("totalprice", FloatType(), False),
            StructField("orderdate", StringType(), False),
            StructField("orderpriority", StringType(), False),
            StructField("clerk", StringType(), False),
            StructField("shippriority", IntegerType(), False),
            StructField("comment", StringType(), True),
        ]
    ),
    "lineitem": StructType(
        [
            StructField("orderkey", IntegerType(), False),
            StructField("partkey", IntegerType(), False),
            StructField("suppkey", IntegerType(), False),
            StructField("linenumber", IntegerType(), False),
            StructField("quantity", FloatType(), False),
            StructField("extendedprice", FloatType(), False),
            StructField("discount", FloatType(), False),
            StructField("tax", FloatType(), False),
            StructField("returnflag", StringType(), False),
            StructField("linestatus", StringType(), False),
            StructField("shipdate", StringType(), False),
            StructField("commitdate", StringType(), False),
            StructField("receiptdate", StringType(), False),
            StructField("shipinstruct", StringType(), False),
            StructField("shipmode", StringType(), False),
            StructField("comment", StringType(), True),
        ]
    ),
}


# Function to generate sample data
def generate_random_string(length):
    """Generates a random string with uppercase letters"""
    return "".join(random.choices(string.ascii_uppercase, k=length))


def generate_random_phone():
    """Generates a random phone number"""
    return f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"


def generate_random_date(start_date="2000-01-01", end_date="2025-12-31"):
    """Generates a random date within the specified range"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")


def round_float(value, decimals=2):
    """Helper function to round float values without conflicting with PySpark"""
    return float(f"{value:.{decimals}f}")


def get_max_iceberg_id(catalog_name, table_name, id_column):
    """Gets the current max ID from the Iceberg table"""
    catalog_name = "icebergdw"

    try:
        full_table_name = f"{catalog_name}.iceberglab.{table_name}"

        df = spark.sql(f"SELECT MAX({id_column}) AS max_id FROM {full_table_name}")
        max_id = df.collect()[0]["max_id"]
        return max_id if max_id is not None else 0
    except Exception as e:
        print(f"Error getting max ID from table {table_name}: {str(e)}")
        return 0


def generate_sample_data(table_name, num_rows, catalog_name="my_catalog"):
    random.seed(datetime.now().timestamp())

    # Mapping of tables to their ID columns
    id_columns = {
        "nation": "nationkey",
        "region": "regionkey",
        "part": "partkey",
        "supplier": "suppkey",
        "customer": "custkey",
        "orders": "orderkey",
    }

    # For tables with their own IDs
    if table_name in id_columns:
        id_column = id_columns[table_name]
        start_id = get_max_iceberg_id(catalog_name, table_name, id_column) + 1
    else:
        start_id = 0  # For relationship tables

    if table_name == "nation":
        data = [
            (
                start_id + i,
                f"Nation_{generate_random_string(8)}",
                random.randint(0, 4),
                f"Comment_{generate_random_string(6)}",
            )
            for i in range(num_rows)
        ]
    elif table_name == "region":
        data = [
            (
                start_id + i,
                f"Region_{generate_random_string(8)}",
                f"Comment_{generate_random_string(6)}",
            )
            for i in range(num_rows)
        ]
    elif table_name == "part":
        data = [
            (
                start_id + i,
                f"Part_{generate_random_string(6)}",
                f"MFGR_{random.randint(1, 5)}",
                f"Brand_{random.randint(1, 10)}",
                f"Type_{random.randint(1, 10)}",
                random.randint(1, 100),
                f"Container_{random.choice(['SM', 'MD', 'LG', 'XL'])}",
                round_float(100.00 + random.uniform(0, 1000)),
                f"Comment_{generate_random_string(6)}",
            )
            for i in range(num_rows)
        ]
    elif table_name == "supplier":
        max_nation = get_max_iceberg_id(catalog_name, "nation", "nationkey")
        data = [
            (
                start_id + i,
                f"Supplier_{generate_random_string(8)}",
                f"Address_{random.randint(1, 1000)}",
                random.randint(0, max_nation),
                generate_random_phone(),
                round_float(500.00 + random.uniform(0, 5000)),
                f"Comment_{generate_random_string(6)}",
            )
            for i in range(num_rows)
        ]
    elif table_name == "partsupp":
        max_part = get_max_iceberg_id(catalog_name, "part", "partkey")
        max_supplier = get_max_iceberg_id(catalog_name, "supplier", "suppkey")
        data = [
            (
                random.randint(0, max_part),
                random.randint(0, max_supplier),
                random.randint(1, 10000),
                round_float(10.00 + random.uniform(0, 100)),
                f"Comment_{generate_random_string(6)}",
            )
            for i in range(num_rows)
        ]
    elif table_name == "customer":
        max_nation = get_max_iceberg_id(catalog_name, "nation", "nationkey")
        data = [
            (
                start_id + i,
                f"Customer_{generate_random_string(8)}",
                f"Address_{random.randint(1, 1000)}",
                random.randint(0, max_nation),
                generate_random_phone(),
                round_float(1000.00 + random.uniform(0, 10000)),
                random.choice(["AUTO", "BUILD", "FURNITURE", "HOUSEHOLD", "MACHINERY"]),
                f"Comment_{generate_random_string(6)}",
            )
            for i in range(num_rows)
        ]
    elif table_name == "orders":
        max_customer = get_max_iceberg_id(catalog_name, "customer", "custkey")
        data = [
            (
                start_id + i,
                random.randint(0, max_customer),
                random.choice(["O", "P", "F"]),
                round_float(1000.00 + random.uniform(0, 10000)),
                generate_random_date(),
                random.choice(["LOW", "MEDIUM", "HIGH", "URGENT", "CRITICAL"]),
                f"Clerk_{random.randint(1000, 9999)}",
                random.randint(0, 10),
                f"Comment_{generate_random_string(6)}",
            )
            for i in range(num_rows)
        ]
    elif table_name == "lineitem":
        max_order = get_max_iceberg_id(catalog_name, "orders", "orderkey")
        max_part = get_max_iceberg_id(catalog_name, "part", "partkey")
        max_supplier = get_max_iceberg_id(catalog_name, "supplier", "suppkey")
        data = [
            (
                random.randint(0, max_order),
                random.randint(0, max_part),
                random.randint(0, max_supplier),
                random.randint(1, 100),
                round_float(1.0 + random.uniform(0, 100)),
                round_float(10.00 + random.uniform(0, 1000)),
                round_float(random.uniform(0, 0.5)),
                round_float(random.uniform(0, 0.2)),
                random.choice(["N", "Y"]),
                random.choice(["O", "F"]),
                generate_random_date(),
                generate_random_date(),
                generate_random_date(),
                random.choice(["DELIVER", "PENDING", "SHIPPED", "RETURNED"]),
                random.choice(["AIR", "TRUCK", "RAIL", "SHIP", "MAIL"]),
                f"Comment_{generate_random_string(6)}",
            )
            for i in range(num_rows)
        ]
    else:
        raise ValueError(f"Unknown table: {table_name}")

    spark.createDataFrame(data, schema=schemas[table_name])

    # Writes the data to the Iceberg table
    return spark.createDataFrame(data, schema=schemas[table_name])


def main():
    # Table names
    nation_name = "nation"
    partsupp_name = "partsupp"
    region_name = "region"
    part_name = "part"
    supplier_name = "supplier"
    order_name = "orders"
    customer_name = "customer"
    lineitem_name = "lineitem"

    # Batch sizes
    nation_region_batch = 5000
    part_supplier_batch = 3000000
    order_customer_batch = 3000000

    # Generate sample data for each table
    print("Generating nation data...")
    sc.setJobDescription("TPC-H Data Generation location")
    nation_df = generate_sample_data(nation_name, nation_region_batch)
    nation_df.write.mode("append").format("iceberg").saveAsTable(
        f"icebergdw.iceberglab.{nation_name}"
    )

    print("Generating region data...")
    sc.setJobDescription("TPC-H Data Generation location")
    region_df = generate_sample_data(region_name, nation_region_batch)
    region_df.write.mode("append").format("iceberg").saveAsTable(
        f"icebergdw.iceberglab.{region_name}"
    )

    print("Generating part data...")
    sc.setJobDescription("TPC-H Data Generation part")
    part_df = generate_sample_data(part_name, part_supplier_batch)
    part_df.write.mode("append").format("iceberg").saveAsTable(f"icebergdw.iceberglab.{part_name}")

    sc.setJobDescription("TPC-H Data Generation partsupp_name")
    partsupp_df = generate_sample_data(partsupp_name, part_supplier_batch)
    partsupp_df.write.format("iceberg").mode("append").saveAsTable(
        f"icebergdw.iceberglab.{partsupp_name}"
    )

    print("Generating supplier data...")
    sc.setJobDescription("TPC-H Data Generation supplier_name")
    supplier_df = generate_sample_data(supplier_name, part_supplier_batch)
    supplier_df.write.mode("append").format("iceberg").saveAsTable(
        f"icebergdw.iceberglab.{supplier_name}"
    )

    print("Generating customer data...")
    sc.setJobDescription("TPC-H Data Generation customer_name")
    customer_df = generate_sample_data(customer_name, order_customer_batch)
    customer_df.write.mode("append").format("iceberg").saveAsTable(
        f"icebergdw.iceberglab.{customer_name}"
    )

    print("Generating orders data...")
    sc.setJobDescription("TPC-H Data Generation orders")
    orders_df = generate_sample_data(order_name, order_customer_batch)
    orders_df.write.mode("append").format("iceberg").saveAsTable(
        f"icebergdw.iceberglab.{order_name}"
    )

    print("Generating lineitem data...")
    sc.setJobDescription("TPC-H Data Generation lineitem")
    lineitem_df = generate_sample_data(lineitem_name, order_customer_batch)
    lineitem_df.write.mode("append").format("iceberg").saveAsTable(
        f"icebergdw.iceberglab.{lineitem_name}"
    )

    # Show some sample data
    print("Sample nation data:")
    nation_df.count()

    print("Sample region data:")
    region_df.count()

    print("count rows")


if __name__ == "__main__":
    main()

    spark.sql(""" select
                    (select count(*) from icebergdw.iceberglab.lineitem) as total_lineitem,
                    (select count(*) from icebergdw.iceberglab.nation) as total_nation,
                    (select count(*) from icebergdw.iceberglab.region) as total_region,
                    (select count(*) from icebergdw.iceberglab.part) as total_part,
                    (select count(*) from icebergdw.iceberglab.supplier) as total_supplier,
                    (select count(*) from icebergdw.iceberglab.partsupp) as total_partsupp,
                    (select count(*) from icebergdw.iceberglab.customer) as total_customer,
                    (select count(*) from icebergdw.iceberglab.orders) as total_orders """).show()

    spark.stop()
