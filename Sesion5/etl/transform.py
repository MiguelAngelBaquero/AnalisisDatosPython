import pandas as pd

def transform_time_from_order(conn, query):
  return pd.read_sql(query, con=conn)