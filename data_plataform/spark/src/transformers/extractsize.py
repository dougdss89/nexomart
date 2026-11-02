import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from spark.src.common.column_validator import ColumnValidator
from spark.src.common.transformer import BaseTransformer

# "Tam 36", "Tam-36", "TAM ÚNICO" at the end of the text. \S+ (not \w+) so accented sizes match.
SIZE_PATTERN = r"(?i)\btam[- ](\S+)$"
SIZE_SUFFIX_PATTERN = r"(?i)[- ]?\btam[- ]\S+$"


class ExtractSizeTransformer(BaseTransformer):
    """Moves a trailing size ("Tam 36", "Tam Único") out of `column` into `size_column`.
    Sizes are optional: rows without one get a null size and keep `column` as it was."""

    def __init__(
        self,
        column: str,
        size_column: str = "size",
        validator: ColumnValidator = ColumnValidator(StringType),
    ):
        self._column = column
        self._validator = validator
        self._sizecolumn = size_column

    def transform(self, df: DataFrame) -> DataFrame:
        if not self._validator.validate(df, [self._column]):
            return df

        text = F.col(self._column)
        size = F.regexp_extract(text, SIZE_PATTERN, 1)

        df = df.withColumn(
            self._sizecolumn, F.when(size == "", F.lit(None)).otherwise(size)
        ).withColumn(self._column, F.trim(F.regexp_replace(text, SIZE_SUFFIX_PATTERN, "")))

        return df
