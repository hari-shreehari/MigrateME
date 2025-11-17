from pyspark.sql import DataFrame

def drop_nulls(df: DataFrame) -> DataFrame:
    return df.dropna()
