from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from spark.src.common.delta import resolved_table_name


class TableValidator(ABC):
    """One subclass per MERGE precondition. Raises on failure, returns None on success."""

    @abstractmethod
    def validate(
        self,
        spark: SparkSession,
        project: str,
        layer: str,
        schema: str,
        table: str,
        df: DataFrame,
        merge_keys: list[str],
    ) -> None:
        raise NotImplementedError


class TableExistsValidator(TableValidator):
    """MERGE INTO, unlike append, won't create the target table - fail fast with a clear message instead."""

    def validate(
        self,
        spark: SparkSession,
        project: str,
        layer: str,
        schema: str,
        table: str,
        df: DataFrame,
        merge_keys: list[str],
    ) -> None:
        target_table = resolved_table_name(project, layer, schema, table)

        if not spark.catalog.tableExists(target_table):
            raise ValueError(f"Cannot MERGE into {target_table}: table does not exist yet")


class NoduplicateMergeKeysValidator(TableValidator):
    """Delta's MERGE fails at runtime if two source rows match the same target row - catch it before the merge."""

    def validate(
        self,
        spark: SparkSession,
        project: str,
        layer: str,
        schema: str,
        table: str,
        df: DataFrame,
        merge_keys: list[str],
    ) -> None:
        if not merge_keys:
            return None

        duplicates = df.groupBy(*merge_keys).count().filter("count > 1")

        if duplicates.count() > 0:
            target_table = resolved_table_name(project, layer, schema, table)
            raise ValueError(f"Duplicate merge keys found in {target_table}: {merge_keys}")
        return None


class CompositeValidator(TableValidator):
    """Runs several validators in sequence - the writer stays agnostic of how many validators exist."""

    def __init__(self, validators: list[TableValidator]):
        self._validators = validators

    def validate(
        self,
        spark: SparkSession,
        project: str,
        layer: str,
        schema: str,
        table: str,
        df: DataFrame,
        merge_keys: list[str],
    ) -> None:
        for validator in self._validators:
            validator.validate(spark, project, layer, schema, table, df, merge_keys)
