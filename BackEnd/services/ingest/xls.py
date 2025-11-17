from pyspark.sql import SparkSession
import pandas as pd

def read_xls(spark: SparkSession, path: str):
    pd_df = pd.read_excel(path, engine='xlrd')
    return spark.createDataFrame(pd_df)
