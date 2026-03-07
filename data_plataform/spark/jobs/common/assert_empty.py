def assert_not_empty(df, table):
    if df.rdd.isEmpty():
        raise ValueError(f"{table} returned empty dataframe")