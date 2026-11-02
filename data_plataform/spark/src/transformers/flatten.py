import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import StructType

from spark.src.common.column_validator import ColumnValidator
from spark.src.common.transformer import BaseTransformer


class FlattenTransformer(BaseTransformer):
    """Flattens struct columns into plain columns. Root fields keep their bare name (the original
    column's name is dropped as a prefix); deeper levels are prefixed by their parent
    (social_buying_contact_name). Only columns produced by this transformer are expanded, tracked
    explicitly, so unrelated structs elsewhere in the df (e.g. __metadata__) are left alone."""

    def __init__(
        self,
        columns: list[str],
        validator: ColumnValidator = ColumnValidator(StructType),
    ):
        self._validator = validator
        self._columns = columns

    def transform(self, df: DataFrame) -> DataFrame:
        for col_name in self._validator.validate(df, self._columns):
            df = self._flatten_column(df, col_name)

        return df

    def _flatten_column(self, df: DataFrame, col_name: str) -> DataFrame:
        root_type = df.schema[col_name].dataType
        if not isinstance(root_type, StructType):
            return df

        other_cols = [c for c in df.columns if c != col_name]

        df = df.select(
            *other_cols, *[F.col(f"{col_name}.{f.name}").alias(f.name) for f in root_type.fields]
        )
        owned = {f.name for f in root_type.fields}

        while True:
            struct_cols = {
                f.name: f.dataType
                for f in df.schema.fields
                if f.name in owned and isinstance(f.dataType, StructType)
            }
            if not struct_cols:
                return df

            flat_cols: list[Column | str] = [c for c in df.columns if c not in struct_cols]
            owned -= set(struct_cols)

            for struct_col, struct_type in struct_cols.items():
                for field in struct_type.fields:
                    new_name = f"{struct_col}_{field.name}"
                    flat_cols.append(F.col(f"{struct_col}.{field.name}").alias(new_name))
                    owned.add(new_name)

            df = df.select(*flat_cols)
