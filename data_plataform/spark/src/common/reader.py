from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from spark.src.common.delta import resolved_table_name
from spark.src.common.logger import get_logger


class BaseReader(ABC):
    """One concrete subclass per landing source. Clients depend only on this interface."""

    # True for readers that can load an explicit list of files from the landing (read_paths), which
    # lets IncrementalReader skip the directory listing by using the ingestion manifest.
    supports_manifest = False

    def read_paths(
        self, spark: SparkSession, schema: str, table: str, paths: list[str]
    ) -> DataFrame:
        raise NotImplementedError(f"{type(self).__name__} cannot read explicit landing paths")

    @abstractmethod
    def read(
        self,
        spark: SparkSession,
        schema: str,
        table: str,
        catalog: str = "",
    ) -> DataFrame:
        raise NotImplementedError


class ParquetLandingReader(BaseReader):
    """Reads Nexomart day-partitioned layout"""

    def __init__(
        self,
        bucket: str = "landing",
        project: str = "nexomart",
        options: dict[str, str] | None = None,
    ):
        self._bucket = bucket
        self._project = project
        self._options = options or {}

    def read(
        self,
        spark: SparkSession,
        schema: str,
        table: str,
        catalog: str = "",
    ) -> DataFrame:
        path = f"s3a://{self._bucket}/{self._project}/{schema}/{table}/*/"

        return spark.read.options(**self._options).parquet(path)

    supports_manifest = True

    def read_paths(
        self, spark: SparkSession, schema: str, table: str, paths: list[str]
    ) -> DataFrame:
        """Loads exactly these files, given relative to the table folder (`<day>/part-*.parquet`,
        as stored in ingestion_control.landing_runs). Nothing is listed."""
        root = f"s3a://{self._bucket}/{self._project}/{schema}/{table}/"
        return spark.read.options(**self._options).parquet(*[root + p for p in paths])


class DeltaTableReader(BaseReader):
    """Reads a Delta table already written to spark_catalog (e.g. silver reading from bronze)."""

    def __init__(
        self,
        layer: str,
        project: str = "nexomart",
        options: dict[str, str] | None = None,
    ):
        self._layer = layer
        self._project = project
        self._options = options or {}

    def read(
        self,
        spark: SparkSession,
        schema: str,
        table: str,
        catalog: str = "",
    ) -> DataFrame:
        target_table = resolved_table_name(self._project, self._layer, schema, table)

        return spark.read.options(**self._options).table(target_table)


class LoggingReader(BaseReader):
    """Decorator: adds a logging layer aroung any BaseReader, without changing it."""

    def __init__(self, reader: BaseReader):
        self._reader = reader
        self._logger = get_logger(self.__class__.__name__)

    @property
    def supports_manifest(self) -> bool:  # type: ignore[override]
        return self._reader.supports_manifest

    def read_paths(
        self, spark: SparkSession, schema: str, table: str, paths: list[str]
    ) -> DataFrame:
        self._logger.info(f"Reading {len(paths):,} listed files of {schema}.{table}")
        return self._reader.read_paths(spark, schema=schema, table=table, paths=paths)

    def read(self, spark: SparkSession, schema: str, table: str, catalog: str = "") -> DataFrame:
        self._logger.info(f"Reading {schema}.{table} via {type(self._reader).__name__}")
        df = self._reader.read(spark, schema=schema, table=table, catalog=catalog)
        self._logger.info(f"Finished reading {schema}.{table}")

        return df
