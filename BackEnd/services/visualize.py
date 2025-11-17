from pyspark.sql import DataFrame
import plotly.express as px

def create_chart(df: DataFrame):
    pd_df = df.toPandas()
    fig = px.scatter(pd_df, x=pd_df.columns[0], y=pd_df.columns[1])
    return fig.to_json()
