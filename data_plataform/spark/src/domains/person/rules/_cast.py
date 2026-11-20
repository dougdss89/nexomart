import pyspark.sql.functions as F
from pyspark.sql import Column


def as_double(col: str) -> Column:
    """try_cast truns text like "abc" into null instead of failing the job (ANSI mode)"""

    return F.expr(f"try_cast(`{col}` as double)")


def as_date(col: str) -> Column:
    return F.expr(f"try_cast(`{col}` as date)")


def reference_day(reference_date: str | None) -> Column:
    """Fixed dates for reproducible runs, today otherwise"""

    return F.lit(reference_date).cast("date") if reference_date else F.current_date()
