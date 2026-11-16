import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType
from spark.src.common.transformer import BaseTransformer

from spark.src.common.column_validator import ColumnValidator


class CleanCustomerType(BaseTransformer):
    def __init__(
        self,
        columns: list[str],
        mapping: dict[str, str] | None = None,
        default: str = "Not Available",
        validator: ColumnValidator = ColumnValidator(StringType),
    ):
        self._columns = columns
        self._validator = validator
        self._mapping = mapping or {"vc": "vendor_contact", 
                                    "sc": "social_contact"}
        self._default = default

    def transform(self, df: DataFrame) -> DataFrame:
        valid_columns = self._validator.validate(df, self._columns)

        if not valid_columns:
            return df

        mapping = F.create_map(*[F.lit(x) for kv in self._mapping.items() for x in kv])

        
        df = df.withColumns(
            {c: F.coalesce(mapping[F.lower(F.trim(F.col(c)))], F.lit(self._default))
             for c in valid_columns}
        )                

        return df
