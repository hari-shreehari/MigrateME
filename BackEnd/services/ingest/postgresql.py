from pyspark.sql import SparkSession


def read_postgresql(spark: SparkSession, url: str, table: str, properties: dict):
    query = f'(SELECT * FROM "public"."{table}") AS t'
    return spark.read.jdbc(url=url, table=query, properties=properties)


def get_table_names(spark: SparkSession, url: str, properties: dict):
    query = "(SELECT tablename FROM pg_tables WHERE schemaname = 'public') AS table_names"
    df = spark.read.jdbc(url=url, table=query, properties=properties)
    return [row.tablename for row in df.collect()]
