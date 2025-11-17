from pyspark.sql import DataFrame
import pyexcel

def write_xls(df: DataFrame, path: str):
    pd_df = df.toPandas()
    data = [pd_df.columns.values.tolist()] + pd_df.values.tolist()
    pyexcel.save_as(array=data, dest_file_name=path)
