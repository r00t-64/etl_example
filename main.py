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
          from sys.tables t where t.name in ('DisProduct', ' DisProductSubcategory , ''
      """)
      src_tables =  src_cursor.fetchall()
      
  except Exception as e:
      
  finally:
      

