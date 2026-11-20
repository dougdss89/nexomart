import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from spark.src.common.transformer import BaseTransformer
from spark.src.domains.person.rules._cast import as_date, as_double

class CustomerValueTransformer(BaseTransformer):
    
    """Average order value and wallet share.

    avg_order_value: total purchased / orders.
    annual_spend: total spread over the purchase span (at least one year, so a one-week
    customer is not annualised into a huge number).
    wallet_share: annual_spend / maximum yearly income. Low share with high income is room to grow.
    Negative totals (known seeder anomaly) and zero orders give null."""
    
    def __init__(
        self,
        orders_col: str = "metrics_count_orders",
        total_col: str = "metrics_total_purchased",
        income_max_col: str = "metrics_maximum_year_income",
        first_col: str = "metrics_date_of_first_purchase",
        last_col: str = "metrics_date_last_purchase",
    ):
    
        self._orders = orders_col
        self._total = total_col
        self._income_max = income_max_col
        self._first = first_col
        self._last = last_col
        
    def transform(self, df: DataFrame) -> DataFrame:
        
        orders = as_double(self._orders)
        total = as_double(self._total)
        income_max = as_double(self._income_max)
        span_days = F.date_diff(as_date(self._last), as_date(self._first))
        
        valid = (orders > 0) & (total >= 0) & (span_days >= 0)
        years = F.greatest(span_days / 365.0, F.lit(1.0))
        annual_spend = F.when(valid, total / years)
        
        df = df.withColumns({
            "avg_order_value": F.when(valid, F.round(total / orders, 2)),
            "annual_spend": F.round(annual_spend, 2),
            "wallet_share": F.when(income_max > 0, F.round(annual_spend / income_max, 4)),
        })
        
        return df