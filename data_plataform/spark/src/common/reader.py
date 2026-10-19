from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from spark.src.common.logger import get_logger


class LandingReader(ABC):
    """One concrete subclass per landing source. Clients depend only on this interface."""

    @abstractmethod
    def read(self, spark: SparkSession, schema: str, table: str, catalog: str) -> DataFrame:
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
    """Reads Spark Delta table"""

    def __init__(self, catalog: str, schema: str, table: str):
        self._catalog = catalog
        self._schema = schema
        self._table = table

    def read(self, spark: SparkSession, schema: str, table: str, catalog: str = "") -> DataFrame:
        self._catalog = catalog
        self._schema = schema
        self._table = table
        table_path = f"{catalog}.{schema}.{table}"
        return spark.read.table(table_path)


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
