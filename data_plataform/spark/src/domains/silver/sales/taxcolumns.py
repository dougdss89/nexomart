import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from spark.src.common.transformer import BaseTransformer


class TaxColumnsTransformer(BaseTransformer):
    """Specific to salesorderitem: moves the first tax of the "taxes" array into plain columns.
    Tax count keeps the original array size, Sso items with 2+ taxes (extra taxes are dropped)
    stay detectable.
    tax_count is -1 the whole items is missing (order without items).
    """

    def transform(self, df: DataFrame) -> DataFrame:
        first = F.col("taxes").getItem(0)

        df = df.withColumns(
            {
                "tax_name": first["name"],
                "tax_rate": first["rate"],
                "tax_amount": first["amount"],
                "tax_count": F.size("taxes"),
            }
        ).drop("taxes")

        return df
