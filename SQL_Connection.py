## -*- coding: utf-8 -*-
"""
@author: dani-rl
"""
import pandas as pd
import pyodbc


def obtenerTabla(table_name):

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

    connection_string(
        'DRIVER = MySQL ODBC 8.0 ANSI Driver;'
        'SERVER = ip_servidor;'
        'DATABASE = database_name;'
        'UID = user_name;'
        'PWD = password;'
        'CHARSET = deafult_charset;'
    )

    conn = pyodbc.connect(connection_string)
    cur = conn.cursor()

    sql_query = pd.read_sql_query('SELECT * FROM ' + table_name, conn)
    print(sql_query)

    conn.close()
