from pyspark.sql import DataFrame
import pandas as pd

def write_ods(df: DataFrame, path: str):
    pd_df = df.toPandas()
    pd_df.to_excel(path, index=False, engine='odf')
