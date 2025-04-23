from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import random
import string
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Configuração do Spark para Iceberg e MinIO
spark = SparkSession.builder \
    .appName("spark_sample") \
    .config("spark.driver.maxResultSize", "6g") \
    .config("spark.rpc.message.maxSize", "256")\
    .config("spark.executor.memory", "12g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

# Definir schemas das tabelas TPC-H
schemas = {
    "nation": StructType([
        StructField("nationkey", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("regionkey", IntegerType(), False),
        StructField("comment", StringType(), True)
    ]),
    "region": StructType([
        StructField("regionkey", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("comment", StringType(), True)
    ]),
    "part": StructType([
        StructField("partkey", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("mfgr", StringType(), False),
        StructField("brand", StringType(), False),
        StructField("type", StringType(), False),
        StructField("size", IntegerType(), False),
        StructField("container", StringType(), False),
        StructField("retailprice", FloatType(), False),
        StructField("comment", StringType(), True)
    ]),
    "supplier": StructType([
        StructField("suppkey", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("address", StringType(), False),
        StructField("nationkey", IntegerType(), False),
        StructField("phone", StringType(), False),
        StructField("acctbal", FloatType(), False),
        StructField("comment", StringType(), True)
    ]),
    "partsupp": StructType([
        StructField("partkey", IntegerType(), False),
        StructField("suppkey", IntegerType(), False),
        StructField("availqty", IntegerType(), False),
        StructField("supplycost", FloatType(), False),
        StructField("comment", StringType(), True)
    ]),
    "customer": StructType([
        StructField("custkey", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("address", StringType(), False),
        StructField("nationkey", IntegerType(), False),
        StructField("phone", StringType(), False),
        StructField("acctbal", FloatType(), False),
        StructField("mktsegment", StringType(), False),
        StructField("comment", StringType(), True)
    ]),
    "orders": StructType([
        StructField("orderkey", IntegerType(), False),
        StructField("custkey", IntegerType(), False),
        StructField("orderstatus", StringType(), False),
        StructField("totalprice", FloatType(), False),
        StructField("orderdate", StringType(), False),
        StructField("orderpriority", StringType(), False),
        StructField("clerk", StringType(), False),
        StructField("shippriority", IntegerType(), False),
        StructField("comment", StringType(), True)
    ]),
    "lineitem": StructType([
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
        StructField("comment", StringType(), True)
    ])
}

# Funções auxiliares mantidas conforme original
def generate_random_string(length):
    return ''.join(random.choices(string.ascii_uppercase, k=length))

def generate_random_phone():
    return f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

def generate_random_date(start_date="2000-01-01", end_date="2025-12-31"):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")

def round_float(value, decimals=2):
    return float(f"{value:.{decimals}f}")

def get_id_range(table_name, id_column):
    """Obtém o intervalo de IDs válidos de uma tabela"""
    try:
        full_table_name = f"icebergdw.iceberglab.{table_name}"
        df = spark.sql(f"SELECT MIN({id_column}) AS min_id, MAX({id_column}) AS max_id FROM {full_table_name}")
        result = df.collect()[0]
        return (result["min_id"], result["max_id"]) if result["min_id"] is not None else (0, 0)
    except Exception as e:
        print(f"Erro ao obter IDs da tabela {table_name}: {str(e)}")
        return (0, 0)

def generate_sample_data(table_name, num_rows):
    random.seed(datetime.now().timestamp())
    
    # Mapeamento de colunas de ID
    id_columns = {
        "nation":   "nationkey",
        "region":   "regionkey",
        "part":     "partkey",
        "supplier": "suppkey",
        "customer": "custkey",
        "orders":   "orderkey"
    }
    
    # Obter ID inicial para tabelas com IDs próprios
    if table_name in id_columns:
        id_col = id_columns[table_name]
        current_min, current_max = get_id_range(table_name, id_col)
        start_id = current_max + 1 if current_max is not None else 0
    else:
        start_id = 0

    # Geração de dados para cada tabela
    if table_name == "nation":
        data = [
            (start_id + i, f"Nation_{generate_random_string(8)}", 
             random.randint(1, 5), f"Comment_{generate_random_string(6)}")
            for i in range(num_rows)
        ]
    
    elif table_name == "region":
        data = [
            (random.randint(1, 5), f"Region_{generate_random_string(8)}", 
             f"Comment_{generate_random_string(6)}")
            for i in range(num_rows)
        ]
    
    elif table_name == "part":
        data = [
            (start_id + i, f"Part_{generate_random_string(6)}", 
             f"MFGR_{random.randint(1, 5)}", f"Brand_{random.randint(1, 10)}",
             f"Type_{random.randint(1, 10)}", random.randint(1, 100),
             f"Container_{random.choice(['SM', 'MD', 'LG', 'XL'])}",
             round_float(100.00 + random.uniform(0, 1000)), 
             f"Comment_{generate_random_string(6)}")
            for i in range(num_rows)
        ]
    
    elif table_name == "supplier":
        nation_min, nation_max = get_id_range("nation", "nationkey")
        data = [
            (start_id + i, f"Supplier_{generate_random_string(8)}",
             f"Address_{random.randint(1, 1000)}", 
             random.randint(nation_min, nation_max), generate_random_phone(),
             round_float(500.00 + random.uniform(0, 5000)), 
             f"Comment_{generate_random_string(6)}")
            for i in range(num_rows)
        ]
    
    elif table_name == "customer":
        nation_min, nation_max = get_id_range("nation", "nationkey")
        data = [
            (start_id + i, f"Customer_{generate_random_string(8)}",
             f"Address_{random.randint(1, 1000)}", 
             random.randint(nation_min, nation_max), generate_random_phone(),
             round_float(1000.00 + random.uniform(0, 10000)),
             random.choice(['AUTO', 'BUILD', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY']),
             f"Comment_{generate_random_string(6)}")
            for i in range(num_rows)
        ]
    
    elif table_name == "orders":
        cust_min, cust_max = get_id_range("customer", "custkey")
        data = [
            (start_id + i, random.randint(cust_min, cust_max),
             random.choice(['O', 'P', 'F']), 
             round_float(1000.00 + random.uniform(0, 10000)),
             generate_random_date(),
             random.choice(['LOW', 'MEDIUM', 'HIGH', 'URGENT', 'CRITICAL']),
             f"Clerk_{random.randint(1000, 9999)}", random.randint(0, 10),
             f"Comment_{generate_random_string(6)}")
            for i in range(num_rows)
        ]
    
    elif table_name == "partsupp":
        part_min, part_max = get_id_range("part", "partkey")
        supp_min, supp_max = get_id_range("supplier", "suppkey")
        data = [
            (random.randint(part_min, part_max), 
             random.randint(supp_min, supp_max),
             random.randint(1, 10000), 
             round_float(10.00 + random.uniform(0, 100)),
             f"Comment_{generate_random_string(6)}")
            for _ in range(num_rows)
        ]
    
    elif table_name == "lineitem":
        order_min, order_max = get_id_range("orders", "orderkey")
        part_min, part_max = get_id_range("part", "partkey")
        supp_min, supp_max = get_id_range("supplier", "suppkey")
        data = [
            (random.randint(order_min, order_max),
             random.randint(part_min, part_max),
             random.randint(supp_min, supp_max),
             random.randint(1, 100),
             round_float(1.0 + random.uniform(0, 100)),
             round_float(10.00 + random.uniform(0, 1000)),
             round_float(random.uniform(0, 0.5)),
             round_float(random.uniform(0, 0.2)),
             random.choice(['N', 'Y']), 
             random.choice(['O', 'F']),
             generate_random_date(), generate_random_date(), generate_random_date(),
             random.choice(['DELIVER', 'PENDING', 'SHIPPED', 'RETURNED']),
             random.choice(['AIR', 'TRUCK', 'RAIL', 'SHIP', 'MAIL']),
             f"Comment_{generate_random_string(6)}")
            for _ in range(num_rows)
        ]
    
    else:
        raise ValueError(f"Tabela desconhecida: {table_name}")
    
    return spark.createDataFrame(data, schema=schemas[table_name])

def main():
    # Ordem correta de geração considerando dependências
    tables_order = [
        ("nation",   0),
        ("region",   0),
        ("part",     2000000),
        ("supplier", 2000000),
        ("partsupp", 2000000),
        ("customer", 5000000),
        ("orders",   5000000),
        ("lineitem", 5000000)
    ]
    
    for table_name, batch_size in tables_order:
        print(f"Generating {table_name} data...")
        sc.setJobDescription(f"TPC-H Data Generation {table_name}")
        df = generate_sample_data(table_name, batch_size)
        
        df.write.mode("append").format("iceberg").saveAsTable(f"icebergdw.iceberglab.{table_name}")
    
    # Verificação final
    spark.sql("""
        SELECT 
            (SELECT COUNT(*) FROM icebergdw.iceberglab.lineitem) AS total_lineitem,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.nation) AS total_nation,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.region) AS total_region,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.part) AS total_part,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.supplier) AS total_supplier,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.partsupp) AS total_partsupp,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.customer) AS total_customer,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.orders) AS total_orders
    """).show()
    
    spark.sql(""" select 
                    (select max(orderkey) from icebergdw.iceberglab.lineitem) as    maxkey_lineitem,
                    (select max(partkey) from icebergdw.iceberglab.partsupp) as     maxkey_partsupp,
                    (select max(nationkey) from icebergdw.iceberglab.nation) as     maxkey_nation,
                    (select max(regionkey) from icebergdw.iceberglab.region) as     maxkey_region,
                    (select max(partkey) from icebergdw.iceberglab.part) as         maxkey_part,
                    (select max(suppkey) from icebergdw.iceberglab.supplier) as     maxkey_supplier,
                    (select max(custkey) from icebergdw.iceberglab.customer) as     maxkey_customer,
                    (select max(orderkey) from icebergdw.iceberglab.orders) as      maxkey_orders 
    """).show()

if __name__ == "__main__":
    main()
    spark.stop()