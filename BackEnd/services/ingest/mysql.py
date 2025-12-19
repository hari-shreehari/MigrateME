from pyspark.sql import SparkSession

def read_mysql(spark: SparkSession, url: str, table: str, properties: dict):
    return spark.read.jdbc(url=url, table=table, properties=properties)

def get_table_names(spark: SparkSession, url: str, properties: dict):
    query = "(SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()) AS table_names"
    df = spark.read.jdbc(url=url, table=query, properties=properties)
    return [row.table_name for row in df.collect()]
