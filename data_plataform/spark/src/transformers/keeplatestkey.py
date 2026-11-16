import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

from spark.src.common.transformer import BaseTransformer


class KeepLatestTransformer(BaseTransformer):
    """Keeps one row per key: the most recent version. Bronze is append-only, so the same key
    can arrive many times; MERGE needs a single source row per key.

    Recency is the first non-null of order_columns (default updated_at, then created_at).
    Ties are broken by _ingested_at (later load wins) when that column exists."""

    def __init__(self, keys: list[str], order_columns: list[str] | None = None):
        self._keys = keys
        self._order_columns = order_columns or ["updated_at", "created_at"]

    def transform(self, df: DataFrame) -> DataFrame:
        present = [c for c in self._order_columns if c in df.columns]
        if not present:
            raise ValueError(f"None of {self._order_columns} found in the DataFrame")

        ordering = [F.coalesce(*[F.col(c) for c in present]).desc()]
        if "_ingested_at" in df.columns:
            ordering.append(F.col("_ingested_at").desc())

        window = Window.partitionBy(*self._keys).orderBy(*ordering)

        return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")
