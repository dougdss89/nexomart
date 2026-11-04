from abc import ABC, abstractmethod
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from spark.src.common.delta import qualified_table_name, resolved_table_name, write_delta
from spark.src.common.logger import get_logger
from spark.src.common.table_validator import TableValidator


class TableWriter(ABC):
    """One concrete subclass per storage format. Writes the row as-is."""

    @abstractmethod
    def write(
        self,
        spark: SparkSession,
        df: DataFrame,
        layer: str,
        schema: str,
        table: str,
        partition_column: str,
    ) -> Any:
        raise NotImplementedError


class DeltaTableWriter(TableWriter):
    """Persists to spark_catalog.<project>_<layer>.<schema>_<table>, append-only."""

    def __init__(self, project: str = "nexomart"):
        self._project = project

    def write(
        self,
        spark: SparkSession,
        df: DataFrame,
        layer: str,
        schema: str,
        table: str,
        partition_column: str,
    ) -> Any:
        full_table = qualified_table_name(self._project, layer, schema, table)

        write_delta(spark, df, full_table, partition_column, mode="append")

        return {"status": "success", "table": f"{full_table}"}


class DeltaMergeWriter(TableWriter):
    """Upsert operation to avoid data duplication and optimize incremental load."""

    def __init__(self, merge_keys: list[str], validator: TableValidator, project: str = "nexomart"):
        self._merge_keys = merge_keys
        self._validator = validator
        self._project = project

    def write(
        self,
        spark: SparkSession,
        df: DataFrame,
        layer: str,
        schema: str,
        table: str,
        partition_column: str,
    ) -> Any:
        self._validator.validate(spark, self._project, layer, schema, table, df, self._merge_keys)

        target_table = resolved_table_name(self._project, layer, schema, table)

        delta_table = DeltaTable.forName(spark, target_table)

        merge_condition = " AND ".join(f"target.{key} = source.{key}" for key in self._merge_keys)

        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        return {"status": "success", "table": f"{target_table}"}


class LoggingWriter(TableWriter):
    """Decorator: adds a logging layer around any TableWriter, without changing it."""

    def __init__(self, writer: TableWriter):
        self._writer = writer
        self._logger = get_logger(self.__class__.__name__)

    def write(
        self,
        spark: SparkSession,
        df: DataFrame,
        layer: str,
        schema: str,
        table: str,
        partition_column: str,
    ) -> Any:
        self._logger.info(f"Writing {layer}.{schema}.{table} via {type(self._writer).__name__}")
        try:
            result = self._writer.write(
                spark=spark,
                df=df,
                layer=layer,
                schema=schema,
                table=table,
                partition_column=partition_column,
            )

        except Exception:
            self._logger.exception(f"Failed to write {layer}.{schema}.{table}")
            raise

        self._logger.info(f"Finished writing {layer}.{schema}.{table}")

        return result
