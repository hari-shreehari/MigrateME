from pyspark.sql import SparkSession

def read_avro(spark: SparkSession, input_path: str):
    return spark.read.format("avro").load(input_path)
