from pyspark.sql import SparkSession


def read_parquet(spark: SparkSession, path: str):
    return spark.read.parquet(path)

