import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from spark.src.common.column_validator import ColumnValidator
from spark.src.common.transformer import BaseTransformer

class ParseJsonTransformer(BaseTransformer):
    """Parses raw JSON string columns (e.g. customer.demographics) into nested structs, inferring
    the schema from every non-null row of the batch so no key is lost. Flattening is a separate
    step (FlattenTransformer). Rows with invalid JSON become null in the parsed column; with
    keep_raw=True (default) the original text survives in <column>_raw for auditing."""

    def __init__(self,
                 columns: list[str],
                 validator:ColumnValidator = ColumnValidator(StringType),
                 keep_raw: bool = True
                ):

        self._validator = validator
        self._columns = columns
        self._keep_raw = keep_raw

    def transform(self,
                  df: DataFrame) -> DataFrame:

        valid_columns = self._validator.validate(df, self._columns)

        if not valid_columns:
            return df

        for col_name in valid_columns:
            if self._keep_raw:
                df = df.withColumn(f"{col_name}_raw", F.col(col_name))

            # get_json_object returns null for empty or malformed JSON, which would otherwise
            # break the "[...]" array built below (e.g. "[{...},,{...}]").
            joined_row = (
                df.select(col_name)
                    .filter(F.get_json_object(F.col(col_name), "$").isNotNull())
                    .agg(F.concat_ws(",", F.collect_list(col_name)).alias("joined"))
                    .first()
            )
            joined = joined_row["joined"] if joined_row else None

            if joined:
                schema_row = (
                    df.sparkSession.range(1)
                        .select(F.schema_of_json(F.lit(f"[{joined}]")))
                        .first()
                )
                if schema_row is None:
                    continue

                array_ddl = schema_row[0]
                object_ddl = array_ddl[len("ARRAY<"):-1] #schema_of_json on an array always return ARRAY <...>
                
                df = df.withColumn(col_name, F.from_json(F.col(col_name), object_ddl))
                
        return df