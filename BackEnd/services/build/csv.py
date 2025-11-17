from pyspark.sql import DataFrame


def write_csv(df: DataFrame, path: str):
    (
        df.write
        .option("header", "true")
        .mode("overwrite")
        .csv(path)
    )
