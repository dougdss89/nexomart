import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from spark.src.common.column_validator import ColumnValidator
from spark.src.common.transformer import BaseTransformer

# " - " in plain text, or "---" once the name was slugified (spaces -> "-" turns " - " into "---").
SEPARATOR_PATTERN = r"\s+-\s+|-{3,}"


class SplitNameTransformer(BaseTransformer):
    """Splits a name column into positional parts, one output column per entry in output_columns.
    Handles both "A B - C D" and the slug form "A-B---C-D". In a slug (no whitespace at all) the
    remaining single hyphens are spaces in disguise and are turned back into spaces. Missing parts
    come out as null. The original column is untouched."""

    def __init__(
        self,
        column: str,
        output_column: list[str],
        validator: ColumnValidator = ColumnValidator(StringType),
    ):
        self._column = column
        self._output_column = output_column
        self._validator = validator

    def transform(self, df: DataFrame) -> DataFrame:
        if not self._validator.validate(df, [self._column]):
            return df

        text = F.trim(F.col(self._column))
        is_slug = ~text.contains(" ") & text.contains("-")
        parts = F.split(text, SEPARATOR_PATTERN)

        for position, name in enumerate(self._output_column):
            part = parts.getItem(position)
            part = F.when(is_slug, F.regexp_replace(part, "-", " ")).otherwise(part)

            df = df.withColumn(name, F.trim(part))

        return df
