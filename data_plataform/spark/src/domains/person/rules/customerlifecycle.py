import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from spark.src.common.transformer import BaseTransformer
from spark.src.domains.person.rules._cast import as_date, reference_day

class CustomerLifecycleTransformer(BaseTransformer):
    
    """ Where the customer is in the relationship (not how they score -> RFM)
    
    new:     first purchase within new_days
    active:  last purchase within lapsing_days
    lapsing: last purchase between lapsing_days and churn_days ago
    churned: last purchase more than churn_days ago
    unknown: missing/inconsistent dates.
    'reactivated' needs order-level history (a gap followed by a new order) and is not
    derivable from the customer table alone."""
    
    def __init__(
        self,
        reference_date: str | None = None,
        new_days: int = 90,
        lapsing_days: int = 90,
        churn_days: int = 365,
        first_col: str = "metrics_date_of_first_purchase",
        last_col: str = "metrics_date_last_purchase",
    ):
        
        self._reference_date = reference_date
        self._new = new_days
        self._lapsing = lapsing_days
        self._churn = churn_days
        self._first = first_col
        self._last = last_col
        
    def transform(self, df: DataFrame) -> DataFrame:
        
        today = reference_day(self._reference_date)
        first, last = as_date(self._first), as_date(self._last)
        since_first, since_last = F.date_diff(today, first), F.date_diff(today, last)
        
        stage = (
            F.when(
                first.isNull() | last.isNull() | (F.date_diff(last, first) < 0) | (since_last < 0),
                "unknown",
            )
            .when(since_first <= self._new, "new")
            .when(since_last > self._churn, "churned")
            .when(since_last > self._lapsing, "lapsing")
            .otherwise("active")
        )
        
        df = df.withColumns({"lifecycle_stage": stage})
        
        return df