from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv('../config/.env')

PDB_USERNAME = os.getenv('PRO_DB_USERNAME')
PDB_PASSWORD = os.getenv('PRO_DB_PASSWORD')
PDB_HOST = os.getenv('PRO_HOST')
PDB_PORT = os.getenv('PRO_PORT')

CDB_USERNAME = os.getenv('DEST_DB_USERNAME')
CDB_PASSWORD = os.getenv('DEST_DB_PASSWORD')
CDB_HOST = os.getenv('DEST_HOST')
CDB_PORT = os.getenv('DEST_PORT')

def prod_connection(database_name):
    conn_url = URL.create(
        "mysql+mysqlconnector",
        username=PDB_USERNAME,
        password=PDB_PASSWORD,
        host=PDB_HOST,
        port=PDB_PORT,
        database=database_name
    )
    return conn_url

def dest_connection(database_name):
    conn_url = URL.create(
        "mysql+mysqlconnector",
        username=CDB_USERNAME,
        password=CDB_PASSWORD,
        host=CDB_HOST,
        port=CDB_PORT,
        database=database_name
    )
    return conn_url

def table_df(database_name,table_name):
    conn_url = prod_connection(database_name)
    engine = create_engine(conn_url)
    con = engine.connect()

    query = f'SELECT * FROM {table_name}'
    # df = pd.read_sql(sql=query, con=con)
    df = pd.read_sql(sql=query, con=con)
    con.close()
    return df


def df_table(dataframe, database_name, table_name):
    conn_url = dest_connection(database_name)
    engine = create_engine(conn_url)
    con = engine.connect()
    dataframe.to_sql(table_name, con=con, if_exists='append', index=False)
    # dataframe.to_sql(table_name, con=engine, if_exists='replace', index=False)
    con.close()
