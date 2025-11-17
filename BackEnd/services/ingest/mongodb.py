from pyspark.sql import SparkSession
from pymongo import MongoClient

def read_mongodb(spark: SparkSession, uri: str, database_name: str, collection_name: str):
    return spark.read.format("mongodb") \
        .option("connection.uri", uri) \
        .option("database", database_name) \
        .option("collection", collection_name) \
        .load()

def get_collection_names(uri: str, database_name: str):
    client = MongoClient(uri)
    db = client[database_name]
    return db.list_collection_names()

def get_database_names(uri: str):
    client = MongoClient(uri)
    excluded_dbs = ['admin', 'local', 'config']
    return [db_name for db_name in client.list_database_names() if db_name not in excluded_dbs]

