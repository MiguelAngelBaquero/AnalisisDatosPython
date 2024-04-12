import sqlalchemy as db

def connect_retail_db():
  engine = db.create_engine('mysql://root:root@192.168.100.31:3310/retail_db')
  conn = engine.connect()
  return conn

def connect_dw_retail():
  engine = db.create_engine('mysql://root:root@192.168.100.31:3310/dw_retail')
  conn = engine.connect()
  return conn