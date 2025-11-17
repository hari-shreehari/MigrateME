from pyspark.sql import DataFrame

def write_postgresql(df: DataFrame, url: str, table: str, properties: dict, mode: str = "overwrite"):
    df.write.jdbc(url=url, table=table, mode=mode, properties=properties)
