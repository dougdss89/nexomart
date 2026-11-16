from pyspark.sql import SparkSession

from spark.src.common.incremental_reader import IncrementalReader
from spark.src.common.reader import DeltaTableReader, LoggingReader
from spark.src.common.table_validator import NoduplicateMergeKeysValidator
from spark.src.common.transformer import CompositeTransformer, LoggingTransformer
from spark.src.common.writer import DeltaMergeWriter, LoggingWriter
from spark.src.transformers.audit import AuditTransformer
from spark.src.transformers.fillnullvalues import FillNullValues
from spark.src.transformers.flatten import FlattenTransformer
from spark.src.transformers.keeplatestkey import KeepLatestTransformer
from spark.src.transformers.normalizegender import NormalizeGender
from spark.src.transformers.normalizemaritalstatus import NormalizeMaritalStatus
from spark.src.transformers.parseJson import ParseJsonTransformer
from spark.src.transformers.toDecimal import NormalizeToDecimal
from spark.src.transformers.tolower import NormalizeString
from spark.src.transformers.totimestamp import NormalizeToTimestamp


def ingestion_customer_silver(spark: SparkSession) -> dict:
    """ """

    reader = IncrementalReader(LoggingReader(DeltaTableReader(layer="bronze")), layer="silver")

    validator = NoduplicateMergeKeysValidator()

    writer = LoggingWriter(
        DeltaMergeWriter(
            validator=validator,
            merge_keys=["customer_id"],
            cluster_by=["customer_id", "date_partition"],
            schema_evolution=True,
        )
    )

    transform = CompositeTransformer(
        [
            LoggingTransformer(KeepLatestTransformer(keys=["customer_id"])),
            LoggingTransformer(NormalizeString(["demographics"])),
            LoggingTransformer(ParseJsonTransformer(["demographics"])),
            LoggingTransformer(FlattenTransformer(["demographics"])),
            LoggingTransformer(NormalizeGender(["social_gender"])),
            LoggingTransformer(NormalizeMaritalStatus(["social_marital_status"])),
            LoggingTransformer(NormalizeToTimestamp(["birthdate", "created_at", "updated_at"])),
            LoggingTransformer(
                NormalizeToDecimal(
                    [
                        "metrics_total_purchased",
                        "metrics_maximum_year_income",
                        "metrics_minimum_year_income",
                    ]
                )
            ),
            LoggingTransformer(
                FillNullValues(
                    [
                        "social_gender",
                        "social_marital_status",
                        "social_company_name",
                        "social_buying_contact_name ",
                        "social_buying_contact_role",
                        "customer_type",
                        "social_marital_status",
                        "social_occupation",
                        "social_tax_regime",
                        "social_buying_contact_name",
                        "social_buying_contact_phone_number",
                        "social_buying_contact_role",
                    ]
                )
            ),
            LoggingTransformer(
                NormalizeString(
                    [   "customer_name",
                        "last_name",
                        "metrics_total_purchased",
                        "metrics_maximum_year_income",
                        "metrics_minimum_year_income",
                        "social_gender",
                        "social_marital_status",
                        "social_company_name",
                        "social_buying_contact_name ",
                        "social_buying_contact_role",
                        "customer_type",
                        "social_marital_status",
                        "social_occupation",
                        "social_tax_regime",
                        "social_buying_contact_name",
                        "social_buying_contact_phone_number",
                        "social_buying_contact_role",
                    ]
                )
            ),
            LoggingTransformer(AuditTransformer(layer="silver")),
        ]
    )

    df = transform.transform(reader.read(spark, schema="person", table="customer"))

    return writer.write(spark, df, layer="silver", schema="person", table="customer")
