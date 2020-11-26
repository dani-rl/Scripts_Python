## -*- coding: utf-8 -*-
"""
@author: dani-rl
"""
import pandas as pd
import pyodbc

def creaConexion(driver, ip_servidor, database_name, user_name, password, charset):
    
    """
    Esta funcion nos permite crear la conexion con nuestra BBDD
    y seleccionar una tabla. Esta tabla se devuelve como dataframe.

    El controlador instalado debe ser Connector/ODBC x64. La instalacion
    se puede hacer desde MySQL Installer


    Parametros
    ----------
    driver : string
        Driver necesario para conctarse al servidor
    ip_servidor : string
        Nombre o direccion ip del servidor 
    database_name : string
        Nombre de la base de datos
    user_name : string
        Usuario para acceder a la base de datos
    password : string
        Contraseña para acceder a la base de datos
    charset : string
        Conjunto de caracteres alfanumericos que utiliza la BBDD
     

    Return
    ------
    conn
        Devuelve la conexion establecida con la BBDD

    """
    
    #Crea conexion a BBDD
     connection_string = (
        'DRIVER = driver;'
        'SERVER = ip_servidor;'
        'DATABASE = database_name;'
        'UID = user_name;'
        'PWD = password;'
        'CHARSET = charset;'
    )

    conn = pyodbc.connect(connection_string)
    cur = conn.cursor()
    
    return conn
    
def obtenerTabla(conn, table_name):

    """
    Esta funcion nos permite crear la conexion con nuestra BBDD
    y seleccionar una tabla. Esta tabla se devuelve como dataframe.

    El controlador instalado debe ser Connector/ODBC x64. La instalacion
    se puede hacer desde MySQL Installer


    Parametros
    ----------
    table_name : string
        Nombre de la tabla en SQL que queremos seleccionar

    Return
    ------
    dataframe
        Dataframe de la tabla completa seleccionada

    """
    #Selecciona una tabla de una base de datos de MySQL
    sql_query = pd.read_sql_query('SELECT * FROM ' + table_name, conn)
    print(sql_query)

    conn.close()
    
    
def insertaDatos(conn, df, table_name):
        
    """
    Esta funcion nos permite insertar datos en una base de datos existente
    en nuestro servidor de Mysql

    El controlador instalado debe ser Connector/ODBC x64. La instalacion
    se puede hacer desde MySQL Installer


    Parametros
    ----------
    df: DataFrame
        Dataframe con el conjunto de datos que se quiere insertar
    table_name : string
        Nombre de la tabla en SQL que queremos seleccionar

    Return
    ------
    No devuleve ningun valor

    """
    
    #Insertar datos de DataFrame en MySQL:
    for index, row in df.iterrows():
        cur.execute("INSERT INTO"+ table_name "+(campo1,campo2,campo3...) values(?,?,?...)", row.campo1,row.campo2, row.campo3,...)

    conn.close()

    

def dataframeTocsv():
    #Convertir un dataframe en csv y devolverlo con su ruta
def cargaMasiva():
    #Cargar en una tabla de una BBDD un csv con LOAD DATA INFILE ...
