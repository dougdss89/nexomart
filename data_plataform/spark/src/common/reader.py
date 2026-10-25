from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from spark.src.common.delta import resolved_table_name
from spark.src.common.logger import get_logger


class LandingReader(ABC):
    """One concrete subclass per landing source. Clients depend only on this interface."""

    @abstractmethod
    def read(self, spark: SparkSession, schema: str, table: str, catalog: str = "") -> DataFrame:
        raise NotImplementedError


class ParquetLandingReader(LandingReader):
    """Reads Nexomart day-partitioned layout"""

    def __init__(self, bucket: str = "landing", project: str = "nexomart"):
        self._bucket = bucket
        self._project = project

    def read(self, spark: SparkSession, schema: str, table: str, catalog: str = "") -> DataFrame:
        path = f"s3a://{self._bucket}/{self._project}/{schema}/{table}/*/"
        return spark.read.parquet(path)


class DeltaTableReader(LandingReader):
    """Reads a Delta table already written to spark_catalog (e.g. silver reading from bronze)."""

    def __init__(self, layer: str, project: str = "nexomart"):
        self._layer = layer
        self._project = project

    def read(self, spark: SparkSession, schema: str, table: str, catalog: str = "") -> DataFrame:
        target_table = resolved_table_name(self._project, self._layer, schema, table)
        return spark.read.table(target_table)


class LoggingReader(LandingReader):
    """Decorator: adds a logging layer aroung any LandingReader, without changing it."""

    def __init__(self, reader: LandingReader):
        self._reader = reader
        self._logger = get_logger(self.__class__.__name__)

    def read(self, spark: SparkSession, schema: str, table: str, catalog: str = "") -> DataFrame:
        self._logger.info(f"Reading {schema}.{table} via {type(self._reader).__name__}")
        df = self._reader.read(spark, schema=schema, table=table, catalog=catalog)
        self._logger.info(f"Finished reading {schema}.{table}")

        return df
