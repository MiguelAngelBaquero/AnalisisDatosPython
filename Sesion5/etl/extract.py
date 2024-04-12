import pandas as pd

def extract_table(table, conn):
  df = pd.read_sql(f'select * from {table}', con = conn)
  return df