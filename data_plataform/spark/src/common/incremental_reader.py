import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import TimestampType

from spark.src.common.delta import resolved_table_name
from spark.src.common.reader import LandingReader


class IncrementalReader(LandingReader):
    """Decorator: returns only rows newer than what the target layer already holds.
    First run (target table missing) returns everything. Tables without created_at/updated_at
    are returned whole, so those can still duplicate on re-runs."""

    def __init__(self, reader: LandingReader, layer: str = "bronze", project: str = "nexomart"):
        self._reader = reader
        self._layer = layer
        self._project = project

    def read(self, spark: SparkSession, schema: str, table: str, catalog: str = "") -> DataFrame:
        df = self._reader.read(spark, schema=schema, table=table, catalog=catalog)

        if "created_at" not in df.columns or "updated_at" not in df.columns:
            return df

        target = resolved_table_name(self._project, self._layer, schema, table)
        if not spark.catalog.tableExists(target):
            return df

        tgt = spark.table(target)
        tgt_stamp = F.coalesce(F.col("updated_at"), F.col("created_at"))

        if isinstance(tgt.schema["updated_at"].dataType, TimestampType):
            tgt_stamp = F.unix_millis(tgt_stamp) * 1000

        row = tgt.agg(F.max(tgt_stamp)).first()
        last = row[0] if row else None

        # last_ts = F.lit(last)
        # newer = (F.col("updated_at") > last_ts) | (
        #     F.col("updated_at").isNull() & (F.col("created_at") > last_ts)
        # )
        # return df if last is None else df.filter(newer)

        stamp = F.coalesce(F.col("updated_at"), F.col("created_at"))
        return df if last is None else df.filter(stamp > F.lit(last))
