import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from spark.src.common.transformer import BaseTransformer

# cut points per dimension. recency days: upper bounds (fewer days == better)
# the others: lower bounds for scores 2, 3 and 4 (score 1 is anything below the first cut).

DEFAULT_THRESHOLDS: dict[str, list[float]] = {
    "recency_days": [90, 365, 730],
    "frequency": [4, 13, 53],
    "monetary": [425, 2008, 15846],
    "potential": [77000, 101000, 162000],
}


class ClassifyCustomerTransformer(BaseTransformer):
    """Scores each customer 1-4 recency, frequency and monetary value (RMF)
    derives a segment from those scores, and scores income potential on a separate axis
    so purchasing power never inflates behavior. Row with missing or impossible inputs get segment
    unclassified plus a reason.
    Recency depends on reference_date (default: today), so compute the whole table instead of merging
    incrementaly"""

    def __init__(
        self,
        thresholds: dict[str, list[float]] | None = None,
        thresholds_by_type: dict[str, dict[str, list[float]]] | None = None,
        type_col: str = "customer_type",
        reference_date: str | None = None,
        orders_col: str = "metrics_count_orders",
        total_col: str = "metrics_total_purchased",
        income_min_col: str = "metrics_minimum_year_income",
        income_max_col: str = "metrics_maximum_year_income",
        last_purchase_col: str = "metrics_date_last_purchase",
    ):
        self._t = thresholds or DEFAULT_THRESHOLDS
        self._by_type = thresholds_by_type or {}
        self._reference_date = reference_date
        self._orders = orders_col
        self._total = total_col
        self._income_min = income_min_col
        self._income_max = income_max_col
        self._last = last_purchase_col
        self._type = type_col

    @staticmethod
    def _num(col: str) -> Column:
        # try_cast turns text like "abc" into null instead of failing the job
        # (ANSI mode)

        return F.expr(f"try_cast(`{col}` as double)")

    @staticmethod
    def _ascending(value: Column, cuts: list[float]) -> Column:
        """1+ how many cut points the value reache: higher value, higher income"""

        return F.lit(1) + sum((value >= F.lit(col)).cast("int") for col in cuts)
    
    def _score(self, value: Column, dimension:str) -> Column:
        """ Defaults cuts, overriden per customer_type when thresholds_by_type has that dimension"""
        
        score = self._ascending(value, self._t[dimension])
        kind = F.lower(F.trim(F.col(self._type)))
        
        for name, cuts in self._by_type.items():
            if dimension in cuts:
                score = ((F.when(kind == name.lower(), self._ascending(value, cuts[dimension]))
                            .otherwise(score)
                        ))
        return score

    def transform(self, df: DataFrame) -> DataFrame:
        if self._reference_date:
            today = F.lit(self._reference_date).cast("date")

        else:
            today = F.current_date()

        orders = self._num(self._orders)
        total = self._num(self._total)
        income_min = self._num(self._income_min)
        income_max = self._num(self._income_max)
        last = F.expr(f"try_cast(`{self._last}` as date)")
        days = F.date_diff(today, last)

        reason = (
            F.when(
                orders.isNull()
                | total.isNull()
                | income_min.isNull()
                | income_max.isNull()
                | last.isNull(),
                "missing_input",
            )
            .when(total < 0, "negative_total")
            .when(income_min > income_max, "income_range_inverted")
            .when(days < 0, "future_purchase_date")
        )

        valid = reason.isNull()
        recency = F.lit(4) - sum((days > F.lit(col)).cast("int") for col in self._t["recency_days"])

        scored = df.withColumns(
            {
                "recency_days": F.when(valid, days),
                "recency_score": F.when(valid, recency),
                "frequency_score": F.when(valid, self._score(orders, "frequency")),
                "monetary_score": F.when(valid, self._score(total, "monetary")),
                "potential_score": F.when(valid, self._score(income_max, "potential")),
                "unclassified_reason": reason,
            }
        )

        r, f, m = F.col("recency_score"), F.col("frequency_score"), F.col("monetary_score")

        segment = (
            F.when(F.col("unclassified_reason").isNotNull(), "unclassified")
            .when((r >= 3) & (f == 4) & (m == 4), "champion")
            .when((f >= 3) & (m >= 3) & (r >= 3), "loyal")  # bought within the last year
            .when(
                (f >= 3) & (m >= 3) & (r == 2), "needs_attention"
            )  # good history, 1-2 years without buying
            .when(((f >= 3) | (m >= 3)) & (r == 1), "at_risk")
            .when(r == 1, "dormant")
            .when(r >= 3, "promising")
            .otherwise("low_value")
        )

        return scored.withColumns(
            {
                "segment": segment,
                # can afford more than they spend: the growth target
                "growth_target": F.coalesce(
                    (F.col("potential_score") >= 3) & (m <= 2), F.lit(False)
                ),
            }
        )
