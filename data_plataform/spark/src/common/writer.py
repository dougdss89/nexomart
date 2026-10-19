from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from spark.src.common.logger import get_logger


class TableWriter(ABC):
    """One concrete subclass per bronze storage format. Writes the row as-is."""

    @abstractmethod
    def write(self, spark: SparkSession, df: DataFrame, schema: str, table: str) -> None:
        raise NotImplementedError


class DeltaTableWriter(TableWriter):
    """Persists to spark_catalog data from landing layer"""

    def __init__(self, project: str = "nexomart"):
        self._project = project

    def write(self, spark: SparkSession, df: DataFrame, schema: str, table: str) -> None:
        from spark.src.common.delta import write_delta

        write_delta(spark, df, f"{self._project}.bronze.{table}", mode="append")


class DeltaMergeWriter(TableWriter):
    """Write a merge operation to avoid data duplications and optimize incremental load"""

    pass


class LoggingWriter(TableWriter):
    """Decorator: adds a logging layer around any TableWriter, without changing it."""

    def __init__(self, writer: TableWriter):
        self._writer = writer
        self._logger = get_logger(self.__class__.__name__)

    def write(self, spark: SparkSession, df: DataFrame, schema: str, table: str) -> None:
        self._logger.info(f"Writing {schema}.{table} via {type(self._writer).__name__}")
        try:
            self._writer.write(spark, df, schema, table)
        except Exception:
            self._logger.exception(f"Failed to write {schema}.{table}")
            raise
        self._logger.info(f"Finished writing {schema}.{table}")
