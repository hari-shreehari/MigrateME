from pyspark.sql import SparkSession

def get_spark_session():
    spark = (
        SparkSession.builder.appName("MigrateMe")
        .master("local[*]")
        .config("spark.driver.memory", "16g")
        .config("spark.jars.packages", ",".join([
        "org.xerial:sqlite-jdbc:3.50.3.0",
        "org.postgresql:postgresql:42.7.7",
        "com.mysql:mysql-connector-j:9.4.0",
        "org.apache.spark:spark-avro_2.13:4.0.0",
        "org.mongodb.spark:mongo-spark-connector_2.13:10.5.0"
        ]))
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    return spark
