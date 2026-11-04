import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType

from spark.src.common.transformer import BaseTransformer


class MetadataTransformer(BaseTransformer):
    """Global: metadata struct type column - every table gets this crossing bronze"""

    def __init__(self, dataset: str, ingest_type: str = "batch", source: str = "nexomart"):
        self._dataset = dataset
        self._ingest_type = ingest_type
        self._source = source

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumns(
            {
                "__metadata__": F.struct(
                    F.lit(self._source).alias("source"),
                    F.lit(self._dataset).alias("dataset"),
                    F.lit(self._ingest_type).alias("ingest_type"),
                ),
                "date_partition": F.current_date().cast(DateType()),
            }
        )
