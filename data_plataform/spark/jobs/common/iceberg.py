# Mantido por compatibilidade com jobs antigos. Novos jobs devem usar delta.py.
def write_iceberg(df, table, mode="append"):
    df.write.format("delta").mode(mode).save(table)
