from pyspark.sql import SparkSession

def read_json(spark: SparkSession, input_path: str):
    return spark.read.json(input_path)
