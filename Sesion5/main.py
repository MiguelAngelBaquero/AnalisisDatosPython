# import required libraries
import pandas as pd
from utils import connection as con
from etl import extract as ex
from etl import load
from etl import transform as t

# establish connection to databases
conn_retail_db = con.connect_retail_db()
conn_dw_retail = con.connect_dw_retail()

# select info from database into variables
df_customers = ex.extract_table('customers', conn_retail_db)
df_orders = ex.extract_table('orders', conn_retail_db)
df_order_items = ex.extract_table('order_items', conn_retail_db)
df_products = ex.extract_table('products', conn_retail_db)
df_categories = ex.extract_table('categories', conn_retail_db)
df_departments = ex.extract_table('departments', conn_retail_db)

# load into dimension tables
load.load_dw_retail('dimension_customer', conn_dw_retail, df_customers.drop('customer_password', axis=1))
load.load_dw_retail('dimension_order', conn_dw_retail, df_orders)
load.load_dw_retail('dimension_department', conn_dw_retail, df_departments)
load.load_dw_retail('dimension_category', conn_dw_retail, df_categories)
load.load_dw_retail('dimension_product', conn_dw_retail, df_products)
load.load_dw_retail('dimension_order_item', conn_dw_retail, df_order_items)

# define query to transform data
query = """
SELECT DISTINCT
  DATE_FORMAT(order_date, '%%Y%%m%%d') AS time_id,
  YEAR(order_date) AS year,
  QUARTER(order_date) AS quarter,
  MONTH(order_date) AS month,
  DAY(order_date) AS day,
  DAYOFWEEK(order_date) AS day_of_week,
  DAYOFYEAR(order_date) AS day_of_year,
  WEEK(order_date) AS week_of_year,
  CASE 
    WHEN DAYOFWEEK(order_date) IN (1,7) THEN TRUE 
    ELSE FALSE 
  END AS is_weekend
FROM retail_db.orders
"""
df_dimension_time = t.transform_time_from_order(conn_retail_db, query)

# load transformed data into dimension table
load.load_dw_retail('dimension_time', conn_dw_retail, df_dimension_time)

# define query to transform data
query = """
SELECT
  o.order_id,
  o.order_date,
  o.order_customer_id,
  o.order_status,
  SUM(oi.order_item_quantity) AS total_items,
  SUM(oi.order_item_subtotal) AS total_amount,
  DATE_FORMAT(o.order_date, '%%Y%%m%%d') as time_id
FROM retail_db.orders o
INNER JOIN retail_db.order_items oi ON o.order_id = oi.order_item_order_id
INNER JOIN dw_retail.dimension_time dt ON DATE_FORMAT(o.order_date, '%%Y%%m%%d') = dt.time_id
GROUP BY o.order_id
"""
df_fact_orders = t.transform_time_from_order(conn_dw_retail, query)

# load transformed data into dimension table
load.load_dw_retail('fact_orders', conn_dw_retail, df_fact_orders)

# commit transaction
conn_dw_retail.commit()

# close connection
conn_dw_retail.close()