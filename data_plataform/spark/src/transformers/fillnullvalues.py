from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from spark.src.common.column_validator import ColumnValidator
from spark.src.common.transformer import BaseTransformer


class FillNullValues(BaseTransformer):
    """Fill null values in the specified columns with the provided fill value.
    Non-string columns are intentionally left untouched (nulls preserved).
    """

    def __init__(
        self,
        columns: list[str],
        fill_value: str = "Not Available",
        validator: ColumnValidator = ColumnValidator(StringType),
    ):
        self._columns = columns
        self._fill_value = fill_value
        self._validator = validator

    def transform(self, df: DataFrame) -> DataFrame:
        valid_columns = self._validator.validate(df, self._columns)

        if not valid_columns:
            return df

        df = df.fillna(dict.fromkeys(valid_columns, self._fill_value))

        return df
