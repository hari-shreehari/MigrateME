from pyspark.sql import SparkSession

def read_sqlite(spark: SparkSession, path: str, table: str):
    url = f"jdbc:sqlite:{path}"
    return spark.read.format("jdbc").option("url", url).option("dbtable", table).load()

def get_table_names(spark: SparkSession, path: str):
    url = f"jdbc:sqlite:{path}"
    query = "(SELECT name FROM sqlite_master WHERE type='table') AS table_names"
    df = spark.read.format("jdbc").option("url", url).option("dbtable", query).load()
    return [row.name for row in df.collect()]
