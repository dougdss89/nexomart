import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from spark.src.common.column_validator import ColumnValidator
from spark.src.common.transformer import BaseTransformer


class NormalizeMaritalStatus(BaseTransformer):
    """Normalize Marital Status values in the specified columns to 'Single', 'Married', or 'Other'."""

    def __init__(
        self, columns: list[str], validator: ColumnValidator = ColumnValidator(StringType)
    ):
        self._columns = columns
        self._validator = validator

    def transform(self, df: DataFrame) -> DataFrame:
        valid_columns = self._validator.validate(df, self._columns)

        if not valid_columns:
            return df

        df = df.withColumns(
            {
                col_name: F.when(F.lower(F.col(col_name)).isin("male", "m"), "male")
                .when(F.lower(F.col(col_name)).isin("female", "f"), "female")
                .otherwise("other")
                for col_name in valid_columns
            }
        )

        return df
