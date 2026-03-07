def write_iceberg(df, table, mode="append"):
    (
        df.write
        .format("iceberg")
        .mode(mode)
        .save(table)
    )