
import pymysql
from sqlalchemy import create_engine
import pandas as pd

def create_conection(connection):
    ##Prod ambiante
    DB_HOST = "#" #ip interna para deploy
    #DB_HOST = "23.21.45.161" #ip publica para realizar pruebas
    DB_USER = "#"
    DB_PASS = "#"
    DB_NAME = "#"

  

    try:
        if connection == "engine":
            engine = create_engine("mysql+pymysql://{user}:{pw}@{ht}/{db}"
                                   .format(user=DB_USER,
                                           pw=DB_PASS,
                                           ht=DB_HOST,
                                           db=DB_NAME))
            return engine
        elif connection == "connection":
            con = pymysql.connect(host=DB_HOST,
                                  user=DB_USER,
                                  password=DB_PASS,
                                  db=DB_NAME,
                                  cursorclass=pymysql.cursors.DictCursor)
            return con
        else:
            return
    except Exception as e:
        print(str(e))
        raise e


def Db_datos(query):
    con = create_conection(connection="connection")
    try:
        with con.cursor() as cur:
            if query.upper().startswith('SELECT') or query.upper().startswith('WITH'):
                cur.execute(query)
                data = cur.fetchall()  # Traer los resultados de un select
            else:
                cur.execute(query)  # Hacer efectiva la escritura de datos
                con.commit()
                data = None
        cur.close()  # Cerrar el cursor
        con.close()  # Cerrar la conexión
        return data
    except Exception as e:
        print(str(e))
        raise e


def Db_datos_pd(dataframe, table):
    engine = create_conection(connection="engine")
    try:
        frame = dataframe.to_sql(table, con=engine, if_exists='append', index=False)
        print(f"Process Done correctly - {frame}")
        return
    except Exception as e:
        print("Error "+str(e))
        raise e




