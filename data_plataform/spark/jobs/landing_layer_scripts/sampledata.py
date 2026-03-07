import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import * #col, expr, lit, struct, map, array, shuffle, slice, current_timestamp, rand
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, MapType, ArrayType, TimestampType

# Configuração do Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_all_id_ranges(spark, tables_with_ids):
    """
    Obtém TODOS os intervalos de ID necessários de uma só vez para evitar múltiplos jobs.
    'tables_with_ids' é um dicionário como: {"table_name": "id_column"}
    """
    id_ranges = {}
    logging.info("Buscando intervalos de ID (MIN/MAX) para as tabelas...")
    for table, id_col in tables_with_ids.items():
        try:
            full_table_name = f"icebergdw.iceberglab.{table}"
            df = spark.sql(f"SELECT MIN({id_col}) AS min_id, MAX({id_col}) AS max_id FROM {full_table_name}")
            result = df.collect()[0]
            min_val, max_val = (result["min_id"], result["max_id"])
            
            if min_val is None or max_val is None:
                logging.warning(f"Tabela '{table}' está vazia ou não foi encontrada. Usando intervalo (1, 1).")
                id_ranges[table] = (1, 1)
            else:
                id_ranges[table] = (min_val, max_val)
            logging.info(f"-> Intervalo para '{table}': {id_ranges[table]}")

        except Exception as e:
            logging.warning(f"Não foi possível obter o intervalo de ID para '{table}'. Usando (1, 1). Erro: {e}")
            id_ranges[table] = (1, 1)
    return id_ranges

# --- Funções de Geração Distribuída ---
# def generate_customer_marketing_distributed(spark, num_rows, start_id, id_ranges):
#     """
#     Gera dados para a tabela 'customer_marketing' com tipos complexos de forma distribuída.
#     """
#     cust_min, cust_max = id_ranges.get("customer", (1, 1))
    
#     # Expressões para gerar os dados aleatórios
#     # Traduzem a lógica das suas funções Python para Spark SQL
#     preferences_expr = map(
#         lit("newsletter"), expr("CASE CAST(rand()*4 AS INT) WHEN 0 THEN 'daily' WHEN 1 THEN 'weekly' WHEN 2 THEN 'monthly' ELSE 'none' END"),
#         lit("promotions"), expr("CASE CAST(rand()*3 AS INT) WHEN 0 THEN 'all' WHEN 1 THEN 'personalized' ELSE 'none' END"),
#         lit("channel"), expr("CASE CAST(rand()*4 AS INT) WHEN 0 THEN 'email' WHEN 1 THEN 'sms' WHEN 2 THEN 'push' ELSE 'all' END"),
#         lit("language"), expr("CASE CAST(rand()*4 AS INT) WHEN 0 THEN 'en' WHEN 1 THEN 'es' WHEN 2 THEN 'pt' ELSE 'fr' END")
#     )
    
#     demographic_expr = struct(
#         expr("CASE CAST(rand()*5 AS INT) WHEN 0 THEN '18-25' WHEN 1 THEN '26-35' WHEN 2 THEN '36-45' WHEN 3 THEN '46-55' ELSE '56+' END").alias("age_group"),
#         expr("CASE CAST(rand()*4 AS INT) WHEN 0 THEN 'low' WHEN 1 THEN 'medium' WHEN 2 THEN 'high' ELSE 'premium' END").alias("income_bracket"),
#         expr("CASE CAST(rand()*4 AS INT) WHEN 0 THEN 'high_school' WHEN 1 THEN 'bachelor' WHEN 2 THEN 'master' ELSE 'phd' END").alias("education_level")
#     )

#     categories_array = array(lit("electronics"), lit("clothing"), lit("home"), lit("books"), lit("groceries"), lit("toys"))
#     purchased_categories_expr = slice(shuffle(categories_array), 1, 3) # Pega 3 categorias aleatórias

#     # Gera um array de structs para as respostas de campanha
#     campaign_responses_expr = array(
#         struct(
#             (rand() * 9000 + 1000).cast("int").cast("string").alias("campaign_id"),
#             (current_timestamp() - expr("INTERVAL 1 DAY * CAST(rand() * 365 AS INT)")).alias("response_date"),
#             expr("CASE CAST(rand()*4 AS INT) WHEN 0 THEN 'click' WHEN 1 THEN 'open' WHEN 2 THEN 'conversion' ELSE 'unsubscribe' END").alias("response_type")
#         ),
#         struct(
#             (rand() * 9000 + 1000).cast("int").cast("string").alias("campaign_id"),
#             (current_timestamp() - expr("INTERVAL 1 DAY * CAST(rand() * 365 AS INT)")).alias("response_date"),
#             expr("CASE CAST(rand()*4 AS INT) WHEN 0 THEN 'click' WHEN 1 THEN 'open' WHEN 2 THEN 'conversion' ELSE 'unsubscribe' END").alias("response_type")
#         )
#     )

#     return spark.range(start_id, start_id + num_rows) \
#         .withColumn("marketing_id", col("id")) \
#         .withColumn("custkey", (rand() * (cust_max - cust_min) + cust_min).cast("long")) \
#         .withColumn("preferences_map", preferences_expr) \
#         .withColumn("demographic_info", demographic_expr) \
#         .withColumn("purchased_categories", purchased_categories_expr) \
#         .withColumn("last_update", current_timestamp()) \
#         .withColumn("campaign_responses", campaign_responses_expr) \
#         .drop("id")


def generate_campaign_analytics_distributed(spark, num_rows, start_id, id_ranges):
    """
    Gera dados para a tabela 'campaign_analytics' com tipos complexos de forma distribuída.
    """
    order_min, order_max = id_ranges.get("orders", (1, 1))
    
    metrics_expr = map(
        lit("impressions"), (rand() * 99000 + 1000).cast("string"),
        lit("clicks"), (rand() * 9900 + 100).cast("string"),
        lit("conversion_rate"), (rand() * 0.19 + 0.01).cast("string"),
        lit("roi"), (rand() * 4.0 + 1.0).cast("string")
    )
    
    segments_expr = array(
        (rand() * 100 + 1).cast("int").cast("string"),
        (rand() * 100 + 1).cast("int").cast("string"),
        (rand() * 100 + 1).cast("int").cast("string")
    )

    timestamps_expr = struct(
        (current_timestamp() - expr("INTERVAL 1 DAY * 30")).alias("start_time"),
        current_timestamp().alias("end_time"),
        current_timestamp().alias("processing_time")
    )

    return spark.range(start_id, start_id + num_rows) \
        .withColumn("campaign_id", col("id").cast("string")) \
        .withColumn("orderkey", (rand() * (order_max - order_min) + order_min).cast("long")) \
        .withColumn("metrics", metrics_expr) \
        .withColumn("customer_segments", segments_expr) \
        .withColumn("timestamps", timestamps_expr) \
        .drop("id")

def generate_part_distributed(spark, num_rows, start_id):
    return spark.range(start_id, start_id + num_rows) \
        .withColumn("partkey", col("id")) \
        .withColumn("name", expr("concat('Part #', id)")) \
        .withColumn("mfgr", expr("concat('Manufacturer #', (rand() * 100))")) \
        .withColumn("brand", expr("concat('Brand #', (rand() * 40))")) \
        .withColumn("type", expr("concat('Type #', (rand() * 10))")) \
        .withColumn("size", (rand() * 50 + 1).cast("int")) \
        .withColumn("container", expr("CASE WHEN rand() < 0.25 THEN 'SM' WHEN rand() < 0.5 THEN 'MD' WHEN rand() < 0.75 THEN 'LG' ELSE 'XL' END")) \
        .withColumn("retailprice", (rand() * 1000 + 100).cast("decimal(12, 2)")) \
        .withColumn("comment", lit("Generated by Spark")) \
        .drop("id")

def generate_supplier_distributed(spark, num_rows, start_id, id_ranges):
    nation_min, nation_max = id_ranges.get("nation", (1, 1))
    return spark.range(start_id, start_id + num_rows) \
        .withColumn("suppkey", col("id")) \
        .withColumn("name", expr("concat('Supplier #', id)")) \
        .withColumn("address", expr("concat('Address #', (rand() * 10000))")) \
        .withColumn("nationkey", (rand() * (nation_max - nation_min) + nation_min).cast("long")) \
        .withColumn("phone", expr("concat((rand()*900+100), '-', (rand()*900+100), '-', (rand()*9000+1000))")) \
        .withColumn("acctbal", (rand() * 5500 - 500).cast("decimal(12, 2)")) \
        .withColumn("comment", lit("Generated by Spark")) \
        .drop("id")

def generate_customer_distributed(spark, num_rows, start_id, id_ranges):
    nation_min, nation_max = id_ranges.get("nation", (1, 1))
    return spark.range(start_id, start_id + num_rows) \
        .withColumn("custkey", col("id")) \
        .withColumn("name", expr("concat('Customer #', id)")) \
        .withColumn("address", expr("concat('Address #', (rand() * 10000))")) \
        .withColumn("nationkey", (rand() * (nation_max - nation_min) + nation_min).cast("long")) \
        .withColumn("phone", expr("concat((rand()*900+100), '-', (rand()*900+100), '-', (rand()*9000+1000))")) \
        .withColumn("acctbal", (rand() * 11000 - 1000).cast("decimal(12, 2)")) \
        .withColumn("mktsegment", expr("CASE WHEN rand() < 0.2 THEN 'AUTO' WHEN rand() < 0.4 THEN 'BUILD' WHEN rand() < 0.6 THEN 'FURNITURE' WHEN rand() < 0.8 THEN 'HOUSEHOLD' ELSE 'MACHINERY' END")) \
        .withColumn("comment", lit("Generated by Spark")) \
        .drop("id")
        
def generate_orders_distributed(spark, num_rows, start_id, id_ranges):
    cust_min, cust_max = id_ranges.get("customer", (1, 1))
    return spark.range(start_id, start_id + num_rows) \
        .withColumn("orderkey", col("id")) \
        .withColumn("custkey", (rand() * (cust_max - cust_min) + cust_min).cast("long")) \
        .withColumn("orderstatus", expr("CASE WHEN rand() < 0.6 THEN 'O' WHEN rand() < 0.9 THEN 'F' ELSE 'P' END")) \
        .withColumn("totalprice", (rand() * 10000 + 1000).cast("decimal(12, 2)")) \
        .withColumn("orderdate", expr("date_sub(current_date(), cast(rand() * 1825 as int))")) \
        .withColumn("orderpriority", expr("CASE WHEN rand() < 0.2 THEN 'LOW' WHEN rand() < 0.4 THEN 'MEDIUM' WHEN rand() < 0.6 THEN 'HIGH' WHEN rand() < 0.8 THEN 'URGENT' ELSE 'CRITICAL' END")) \
        .withColumn("clerk", expr("concat('Clerk #', (rand() * 9000 + 1000))")) \
        .withColumn("shippriority", lit(0)) \
        .withColumn("comment", lit("Generated by Spark")) \
        .drop("id")

def generate_partsupp_distributed(spark, num_rows, id_ranges):
    part_min, part_max = id_ranges.get("part", (1, 1))
    supp_min, supp_max = id_ranges.get("supplier", (1, 1))
    return spark.range(num_rows) \
        .withColumn("partkey", (rand() * (part_max - part_min) + part_min).cast("long")) \
        .withColumn("suppkey", (rand() * (supp_max - supp_min) + supp_min).cast("long")) \
        .withColumn("availqty", (rand() * 9999 + 1).cast("int")) \
        .withColumn("supplycost", (rand() * 100 + 10).cast("decimal(12, 2)")) \
        .withColumn("comment", lit("Generated by Spark")) \
        .drop("id")
        
def generate_lineitem_distributed(spark, num_rows, id_ranges):
    order_min, order_max = id_ranges.get("orders", (1, 1))
    part_min, part_max = id_ranges.get("part", (1, 1))
    supp_min, supp_max = id_ranges.get("supplier", (1, 1))
    # Gerar chaves compostas para lineitem (orderkey, partkey, suppkey)
    # Aqui, para simplificar, geramos aleatoriamente. Em um cenário real, 
    # você poderia fazer um crossJoin de subconjuntos para garantir a validade.
    return spark.range(num_rows) \
        .withColumn("orderkey", (rand() * (order_max - order_min) + order_min).cast("long")) \
        .withColumn("partkey", (rand() * (part_max - part_min) + part_min).cast("long")) \
        .withColumn("suppkey", (rand() * (supp_max - supp_min) + supp_min).cast("long")) \
        .withColumn("linenumber", (rand() * 6 + 1).cast("int")) \
        .withColumn("quantity", (rand() * 49 + 1).cast("decimal(12, 2)")) \
        .withColumn("extendedprice", (rand() * 1000 + 10).cast("decimal(12, 2)")) \
        .withColumn("discount", (rand() * 0.5).cast("decimal(12, 2)")) \
        .withColumn("tax", (rand() * 0.2).cast("decimal(12, 2)")) \
        .withColumn("returnflag", expr("IF(rand() < 0.5, 'N', 'A')")) \
        .withColumn("linestatus", expr("IF(rand() < 0.5, 'O', 'F')")) \
        .withColumn("shipdate", expr("date_add(current_date(), cast(rand() * 120 as int))")) \
        .withColumn("commitdate", expr("date_add(current_date(), cast(rand() * 90 as int))")) \
        .withColumn("receiptdate", expr("date_add(current_date(), cast(rand() * 150 as int))")) \
        .withColumn("shipinstruct", expr("CASE WHEN rand() < 0.33 THEN 'DELIVER IN PERSON' WHEN rand() < 0.66 THEN 'COLLECT COD' ELSE 'TAKE BACK RETURN' END")) \
        .withColumn("shipmode", expr("CASE WHEN rand() < 0.2 THEN 'AIR' WHEN rand() < 0.4 THEN 'TRUCK' WHEN rand() < 0.6 THEN 'RAIL' WHEN rand() < 0.8 THEN 'SHIP' ELSE 'MAIL' END")) \
        .withColumn("comment", lit("Generated by Spark")) \
        .drop("id")
        
# --- Orquestrador Principal ---

def main():
    spark = SparkSession.builder \
        .appName("Distributed Data Generation for Iceberg") \
        .enableHiveSupport() \
        .getOrCreate()

    # Tabelas que não precisam de geração massiva (dimensões pequenas)
    # A geração no Driver para poucas linhas é aceitável e mais simples.
    logging.info("Gerando tabelas de dimensão pequenas...")
    nations_data = [(i, f"NATION_{i}", i % 5, f"Comment for nation {i}") for i in range(100)]
    spark.createDataFrame(nations_data, ["n_nationkey", "n_name", "n_regionkey", "n_comment"]) \
         .write.format("iceberg").mode("overwrite").saveAsTable("icebergdw.iceberglab.nation")

    regions_data = [(i, f"REGION_{i}", f"Comment for region {i}") for i in range(500)]
    spark.createDataFrame(regions_data, ["r_regionkey", "r_name", "r_comment"]) \
         .write.format("iceberg").mode("overwrite").saveAsTable("icebergdw.iceberglab.region")
    logging.info("Tabelas de dimensão geradas.")

    # 1. Definir quais tabelas precisam ter seus IDs lidos para chaves estrangeiras
    needed_ids = {
        "nation": "n_nationkey",
        "region": "r_regionkey",
        "part": "partkey",
        "supplier": "suppkey",
        "customer": "custkey",
        "orders": "orderkey"
    }
    id_ranges = get_all_id_ranges(spark, needed_ids)

    # 2. Ordem de geração das tabelas massivas
    tables_to_generate = [
        ("nation",              3000),
        ("region",              100),
        ("part", 5000000),
        ("supplier", 4100000),
        ("customer", 5500000),
        ("orders", 10000000),
        ("partsupp", 3000000),
        ("lineitem", 25000000),
        ("customer_marketing", 0),   # Adicionado
        ("campaign_analytics", 0)     # Adicionado
    ]
    
    for table_name, num_rows in tables_to_generate:
        logging.info(f"--- Iniciando geração da tabela: {table_name} com {num_rows} linhas ---")
        df = None
        
        # Obter o ID inicial para a tabela atual
        id_col = needed_ids.get(table_name)
        if id_col:
            _, current_max = id_ranges.get(table_name, (0, 0))
            start_id = current_max + 1 if current_max is not None else 1
        else:
            start_id = 1 # Para tabelas como partsupp, lineitem que não têm ID próprio

        if table_name == "part":
            df = generate_part_distributed(spark, num_rows, start_id)
        elif table_name == "supplier":
            df = generate_supplier_distributed(spark, num_rows, start_id, id_ranges)
        elif table_name == "customer":
            df = generate_customer_distributed(spark, num_rows, start_id, id_ranges)
        elif table_name == "orders":
            df = generate_orders_distributed(spark, num_rows, start_id, id_ranges)
        elif table_name == "partsupp":
            df = generate_partsupp_distributed(spark, num_rows, id_ranges)
        elif table_name == "lineitem":
            df = generate_lineitem_distributed(spark, num_rows, id_ranges)
        # elif table_name == "customer_marketing":
        #     df = generate_customer_marketing_distributed(spark, num_rows, start_id, id_ranges)
        # elif table_name == "campaign_analytics":
        #     df = generate_campaign_analytics_distributed(spark, num_rows, start_id, id_ranges)
        if df:
            logging.info(f"Gerado DataFrame para '{table_name}'. Salvando no Iceberg...")
            df.write.format("iceberg").mode("append").saveAsTable(f"icebergdw.iceberglab.{table_name}")
            logging.info(f"Tabela '{table_name}' salva com sucesso.")
        else:
            logging.warning(f"Nenhuma função de geração definida para a tabela: {table_name}")

    logging.info("Processo de geração de dados massivos concluído.")
                
    spark.sql("""
        SELECT 
            (SELECT COUNT(*) FROM icebergdw.iceberglab.lineitem) AS total_lineitem,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.nation) AS total_nation,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.region) AS total_region,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.part) AS total_part,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.supplier) AS total_supplier,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.partsupp) AS total_partsupp,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.customer) AS total_customer,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.orders) AS total_orders,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.customer_marketing) AS total_marketing,
            (SELECT COUNT(*) FROM icebergdw.iceberglab.campaign_analytics) AS total_campaigns
    """).show()
    
    spark.sql(""" SELECT * FROM icebergdw.iceberglab.orders LIMIT 10 """)
    
    spark.stop()

if __name__ == "__main__":
    main()