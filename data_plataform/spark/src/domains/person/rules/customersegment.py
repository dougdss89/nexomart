import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from spark.src.common.transformer import BaseTransformer

#cut points per dimension. recency days: upper bounds (fewer days == better)
# the others: lower bounds for scores 2, 3 and 4 (score 1 is anything below the first cut).

DEFAULT_THRESHOLDS: dict[str, list[float]] = {
    
    "recency_days": [90, 365, 730],
    "frequency": [40, 80, 115],
    "monetary": [3500, 7500, 11000],
    "potential": [90000, 125000, 160000],    
}

class ClassifyCustomerTransformer(BaseTransformer):
    pass