from pyspark.sql import DataFrame

def write_json(df: DataFrame, output_path: str):
    df.write.json(output_path)
