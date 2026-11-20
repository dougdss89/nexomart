import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from spark.src.common.reader import DeltaTableReader, LoggingReader
from spark.src.common.transformer import CompositeTransformer, LoggingTransformer
from spark.src.common.writer import DeltaTableWriter, LoggingWriter
from spark.src.domains.person.rules.customersegment import ClassifyCustomerTransformer
from spark.src.transformers.audit import AuditTransformer
from spark.src.domains.person.rules.customerconsent import CustomerConsentTransformer
from spark.src.domains.person.rules.customerlifecycle import CustomerLifecycleTransformer
from spark.src.domains.person.rules.customervalue import CustomerValueTransformer


GOLD_COLUMNS = [
    "customer_id",
    "customer_type",
    "metrics_count_orders",
    "metrics_total_purchased",
    "metrics_minimum_year_income",
    "metrics_maximum_year_income",
    "metrics_date_last_purchase",
    "recency_days",
    "recency_score",
    "frequency_score",
    "monetary_score",
    "potential_score",
    "segment",
    "growth_target",
    "unclassified_reason",
    "can_email",
    "can_sms",
    "is_contactable",
    "consent_missing",
    "avg_order_value",
    "annual_spend",
    "wallet_share",
    "lifecycle_stage",
    "reference_date",
    "_ingested_at",
    "_source_layer",
]


def ingestion_customer_gold(spark: SparkSession, reference_date: str | None = None) -> dict:
    """Silver -> Gold for person.customer_segment, recomputed in full on every run.
    reference_date ("YYYY-MM-DD") pins the recency calculation; default is today.
    """

    reader = LoggingReader(DeltaTableReader(layer="silver"))
    writer = LoggingWriter(DeltaTableWriter(mode="overwrite", partition_column="segment"))

    transform = CompositeTransformer(
        [
            LoggingTransformer(ClassifyCustomerTransformer(reference_date=reference_date)),
            LoggingTransformer(CustomerConsentTransformer()),
            LoggingTransformer(CustomerValueTransformer()),
            LoggingTransformer(CustomerLifecycleTransformer(reference_date=reference_date)),
            LoggingTransformer(AuditTransformer(layer="gold")),
        ]
    )

    df = transform.transform(reader.read(spark, schema="person", table="customer"))

    df = df.withColumn(
        "reference_date", F.lit(reference_date).cast("date") if reference_date else F.current_date()
    ).select(*GOLD_COLUMNS)

    return writer.write(spark, df, layer="gold", schema="person", table="customer_segment")
