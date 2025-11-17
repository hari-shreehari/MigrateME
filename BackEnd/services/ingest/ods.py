from pyspark.sql import SparkSession
import pandas as pd

def read_ods(spark: SparkSession, path: str):
    pd_df = pd.read_excel(path, engine='odf')
    return spark.createDataFrame(pd_df)
