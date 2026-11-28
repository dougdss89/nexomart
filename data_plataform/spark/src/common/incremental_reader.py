import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import TimestampType

from spark.src.common.delta import resolved_table_name
from spark.src.common.logger import get_logger
from spark.src.common.manifest import unconsumed_paths
from spark.src.common.reader import BaseReader


class IncrementalReader(BaseReader):
    """Decorator: returns only rows newer than what the target layer already holds.
    First run (target table missing) returns everything. Tables without created_at/updated_at
    are returned whole, so those can still duplicate on re-runs.

    When the wrapped reader can load explicit files (`supports_manifest`) the new files come from
    the ingestion manifest (ingestion_control.landing_runs) instead of listing the landing, which
    is what made the first Bronze loads take hours on MinIO's spinning disk. If the manifest can't
    prove it is complete (see `unconsumed_paths`) the whole landing is read as before. Either way
    the row filter below still runs, so a file read twice never duplicates rows."""

    def __init__(
        self,
        reader: BaseReader,
        layer: str = "bronze",
        project: str = "nexomart",
        use_manifest: bool = True,
    ):
        self._reader = reader
        self._layer = layer
        self._project = project
        self._use_manifest = use_manifest
        self._logger = get_logger(self.__class__.__name__)

    def read(self, spark: SparkSession, schema: str, table: str, catalog: str = "") -> DataFrame:
        def full() -> DataFrame:
            return self._reader.read(spark, schema=schema, table=table, catalog=catalog)

        target = resolved_table_name(self._project, self._layer, schema, table)
        if not spark.catalog.tableExists(target):
            return full()

        tgt = spark.table(target)
        if "created_at" not in tgt.columns or "updated_at" not in tgt.columns:
            return full()

        tgt_stamp = F.coalesce(F.col("updated_at"), F.col("created_at"))

        if isinstance(tgt.schema["updated_at"].dataType, TimestampType):
            tgt_stamp = F.unix_micros(tgt_stamp) * 1000

        row = tgt.agg(F.max(tgt_stamp)).first()
        last = row[0] if row else None
        if last is None:
            return full()

        df = None
        if self._use_manifest and self._reader.supports_manifest:
            paths = unconsumed_paths(spark, schema, table, int(last))
            if paths is not None:
                if not paths:
                    self._logger.info(f"{schema}.{table}: nothing new in the landing")
                    return tgt.limit(0)  # same columns as the target; downstream appends nothing
                df = self._reader.read_paths(spark, schema=schema, table=table, paths=paths)
        if df is None:
            df = full()

        stamp = F.coalesce(F.col("updated_at"), F.col("created_at"))
        return df.filter(stamp > F.lit(last))
