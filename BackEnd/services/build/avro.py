from pyspark.sql import DataFrame

def write_avro(df: DataFrame, output_path: str):
    df.write.format("avro").save(output_path)
