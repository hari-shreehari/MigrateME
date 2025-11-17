from pyspark.sql import DataFrame

def write_sqlite(df: DataFrame, path: str, table: str):
    url = f"jdbc:sqlite:{path}"
    (
        df.write
        .mode("overwrite")
        .jdbc(url=url, table=table, properties={"driver": "org.sqlite.JDBC"})
    )
