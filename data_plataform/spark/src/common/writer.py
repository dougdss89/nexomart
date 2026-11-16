from abc import ABC, abstractmethod
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from spark.src.common.delta import (
    create_clustered_delta,
    qualified_table_name,
    resolved_table_name,
    write_delta,
)
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
        partition_column: str | None = None,
    ) -> Any:
        raise NotImplementedError


class DeltaTableWriter(TableWriter):
    """Persists to spark_catalog.<project>_<layer>.<schema>_<table>, append-only."""

    def __init__(
        self,
        project: str = "nexomart",
        mode: str = "append",
        options: dict[str, str] | None = None,
        partition_column: str | None = None,
    ):
        self._project = project
        self._options: dict[str, Any] = options or {}
        self._mode = mode
        self._partition = partition_column

    def write(
        self,
        spark: SparkSession,
        df: DataFrame,
        layer: str,
        schema: str,
        table: str,
        partition_column: str | None = None,
    ) -> Any:
        full_table = qualified_table_name(self._project, layer, schema, table)

        partition_column = partition_column or self._partition

        if partition_column is None:
            raise ValueError("DeltaTableWriter needs partition_column")

        write_delta(spark, df, full_table, partition_column, mode=self._mode, options=self._options)

        return {"status": "success", "table": f"{full_table}"}


class DeltaMergeWriter(TableWriter):
    """Upsert via MERGE. Creates the (empty) table on the first run, then merges.
    schema_evolution lets batches add or omit columns (JSON-driven schemas change per batch)."""

    def __init__(
        self,
        merge_keys: list[str],
        validator: TableValidator,
        project: str = "nexomart",
        cluster_by: list[str] | None = None,
        schema_evolution: bool = True,
        options: dict[str, str] | None = None,
        partition_column: str | None = None,
    ):
        self._merge_keys = merge_keys
        self._validator = validator
        self._project = project
        self._cluster = cluster_by
        self._schema_evolution = schema_evolution
        self._options: dict[str, Any] = options or {}
        self._partition = partition_column

    def write(self, spark, df, layer, schema, table, partition_column=None) -> Any:
        self._validator.validate(spark, self._project, layer, schema, table, df, self._merge_keys)

        full_table = qualified_table_name(self._project, layer, schema, table)
        target_table = resolved_table_name(self._project, layer, schema, table)
        partition_column = partition_column or self._partition

        # First run: create the empty table through the same helper the append writer uses.
        if not spark.catalog.tableExists(target_table):
            if self._cluster:
                create_clustered_delta(spark, df, full_table, self._cluster)
            else:
                if partition_column is None:
                    raise ValueError("partition_column is required when cluster_by is not set")
                write_delta(spark, df.limit(0), full_table, partition_column, mode="append")

        for key, value in self._options.items():
            spark.conf.set(key, value)

        condition = " AND ".join(f"t.{k} = s.{k}" for k in self._merge_keys)
        merge = (
            DeltaTable.forName(spark, target_table.removeprefix("spark_catalog."))
            .alias("t")
            .merge(df.alias("s"), condition)
        )

        if self._schema_evolution:
            merge = merge.withSchemaEvolution()

        merge.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        return {"status": "success", "table": target_table}


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
        partition_column: str | None = None,
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
