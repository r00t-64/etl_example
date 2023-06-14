from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

#get passwords from environment var
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']
# sql db details
driver = '{SQL Server Native Client 11.0}'
server = 'haq-PC'
database  = 'AdventureWorksDW2019'

#extract data from sql server
def extract():
  try:
      src_conn =  pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + '\SQLEXPRESS'+ ';DATABASE=' + ';UID=' + uid  + ';PWD=' +pwd) 
      sql_cursor = src_conn.cursor()
      
      src_cursor.execute("""  select t.name as table_name
          from sys.tables t where t.name in ('DisProduct', 'DisProductSubcategory' , 'DisProductSubcategory', 'DisProductSubcategory' , 'DimSalesFactory', 'FactInternetSales'
      """)
      src_tables =  src_cursor.fetchall()
      for tbl in src_tables:
          #query and load save data do dataframe
          df = pd.read_sql_query(f'select * FROM {tbl[0]}' , src_conn)
          load(df, tbl[0])      
  except Exception as e:
      print("Data extract error:"  +  str(e))
  finally:
      src_conn.close()

