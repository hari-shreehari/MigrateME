from pyspark.sql import DataFrame

def write_mongodb(df: DataFrame, uri: str, database: str, collection: str):
    df.write.format("mongodb") \
        .option("connection.uri", uri) \
        .option("database", database) \
        .option("collection", collection) \
        .mode("overwrite") \
        .save()

