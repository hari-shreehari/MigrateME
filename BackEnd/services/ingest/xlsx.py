from pyspark.sql import SparkSession
import pandas as pd

def read_xlsx(spark: SparkSession, path: str):
    pd_df = pd.read_excel(path, engine='openpyxl')
    return spark.createDataFrame(pd_df)
