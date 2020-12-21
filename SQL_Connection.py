# -*- coding: utf-8 -*-

import pandas as pd
import pyodbc

class funcionesSQL:
    
    """
    
    Conjunto de funciones creadas para mostrar, obtener e insertar
    datos en una BBDD de MySQL
    
    """

    def creaConexion(driver, ip_servidor, database_name,
                     user_name, password, charset):
        
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
            'DRIVER={'+driver+'};'
            'SERVER='+ip_servidor+';'
            'DATABASE='+database_name+';'
            'UID='+user_name+';'
            'PWD='+password+';'
            'CHARSET='+charset+';'
        )
        
        conn = pyodbc.connect(connection_string)
        #cur = conn.cursor()
        
        return conn
        
    def mostrarTabla(conn, table_name):
    
        """
        
        Esta funcion nos permite crear la conexion con nuestra BBDD
        y seleccionar una tabla.
        El controlador instalado debe ser Connector/ODBC x64. La instalacion
        se puede hacer desde MySQL Installer
        
        Parametros
        ----------
        conn: connection
            Conexion creada con la BBDD de MySQL
        table_name : string
            Nombre de la tabla en SQL que queremos seleccionar
            
        Return
        ------
        No devuelve ningun valor
        
        """
        #Selecciona una tabla de una base de datos de MySQL
        sql_query = pd.read_sql_query('SELECT * FROM ' + table_name, conn)
        print(sql_query)
    
    
    def obtenerTabla(conn, table_name):
    
        """
        
        Esta funcion nos permite crear la conexion con nuestra BBDD
        y seleccionar una tabla. Esta tabla se devuelve como dataframe.
        El controlador instalado debe ser Connector/ODBC x64. La instalacion
        se puede hacer desde MySQL Installer
        
        Parametros
        ----------
        conn: connection
            Conexion creada con la BBDD de MySQL
        table_name : string
            Nombre de la tabla en SQL que queremos seleccionar
            
        Return
        ------
        DataFrame
            Devuelve un dataframe con la tabla seleccionada
        
        """
        #Selecciona una tabla de una base de datos de MySQL
        sql_query = pd.read_sql_query('SELECT * FROM ' + table_name, conn)
        return sql_query
        
    def insertaDatos(conn,df, table_name):
            
        """
        
        Esta funcion permite insertar los datos de un DataFrame en una BBDD
        de MySQL. 
        
        Parametros
        ----------
        conn: connection
            Conexion creada con la BBDD de MySQL
        table_name : string
            Nombre de la tabla en SQL donde queremos insertar los datos
        path: string
            Ruta completa del csv que queremos insertar en la tabla
        file_name: string
            Nombre completo del csv con la extensión .csv
            
        Return
        ------
        No devuleve ningun valor
        
        """
        cur = conn.cursor()
        #Insertar datos de DataFrame en MySQL:
    
        cols = ",".join([str(i) for i in df.columns.tolist()])

        # Insert DataFrame recrds one by one.
        for i,row in df.iterrows():
            sql = "INSERT INTO "+table_name+" (" +cols + ") VALUES (" + "?,"*(len(row)-1) + "?)"

            cur.execute(sql, tuple(row))

        cur.commit()
    
    
    #------------------------------ PRUEBAS ------------------------------#
def main():
    
    
    
    # Datos Conexion #
    driver = 'MySQL ODBC 8.0 ANSI Driver'
    ip_servidor = 'bbdd.braintrust-cs.org'
    database_name = 'btcs'
    user_name = 'btcs'
    password = 'SERVERtrust1.0+-'
    charset = 'latin1'
    
    # Crea conexion #
    conn = funcionesSQL.creaConexion(driver, ip_servidor,
                        database_name, user_name, password, charset)
    
    
    # Insertar datos en tabla #
    
    # ----- EN PRRUEBAS ----- #
    
    path = 'C:/Users/drlopez/Desktop/Braintrust/Mutua/AUTO/Trabajando/Prueba.csv'
    
    df = pd.read_csv(path,sep=';')
    

    
    try:
       
        funcionesSQL.insertaDatos(conn
             ,df
             ,'Parque_DGT_copy')
        
    except:
        print("Ha ocurrido un error")
    finally:
        conn.close()
    
    
    
    # Ejecuta consulta de seleccion #
    # try:
    #     funcionesSQL.mostrarTabla(conn, 'V_Parque_DGT')
    # except:
    #     print("Ha ocurrido un error")
    # finally:
    #     conn.close()


if __name__ == "__main__":
    main()
