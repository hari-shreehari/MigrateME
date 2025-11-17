from pyspark.sql import DataFrame

def write_parquet(df: DataFrame, output_path: str):
    df.write.parquet(output_path)
