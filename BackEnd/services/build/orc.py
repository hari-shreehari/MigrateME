from pyspark.sql import DataFrame

def write_orc(df: DataFrame, path: str):
    df.write.mode("overwrite").orc(path)
