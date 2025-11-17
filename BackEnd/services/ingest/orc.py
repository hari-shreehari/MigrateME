from pyspark.sql import SparkSession

def read_orc(spark: SparkSession, path: str):
    return spark.read.orc(path)
