import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType

from spark.src.common.column_validator import ColumnValidator
from spark.src.common.transformer import BaseTransformer


class ExplodeTransformer(BaseTransformer):
    """One row per array element. Changes the grain of the table
    (e.g. order -> order_line).
    Rows whose array is empty or null are kept, with a null element"""

    def __init__(self, column: str, validator: ColumnValidator = ColumnValidator(ArrayType)):
        self._column = column
        self._validator = validator

    def transform(self, df: DataFrame) -> DataFrame:
        if not self._validator.validate(df, [self._column]):
            return df

        df = df.withColumn(self._column, F.explode_outer(self._column))

        return df
