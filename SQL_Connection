import pandas as pd
import pyodbc
connection_string(
    'DRIVER = MySQL ODBC 8.0 ANSI Driver;'
    'SERVER = ip_servidor;'
    'DATABASE = database_name'
    'UID = user_name;'
    'PWD = password;'
    'CHARSET = deafult_charset;'
)

conn = pyodbc.connect(connection_string)
cur = conn.cursor()

sql_query = pd.read_sql_query('SELECT * FROM table_name', conn)
print(sql_query)
