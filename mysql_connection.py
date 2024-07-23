from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd

import os
from dotenv import load_dotenv

load_dotenv()

DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')


def table_df(database_name, table_name):
    conn_url = URL.create(
        "mysql+mysqlconnector",
        username=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        database=database_name
    )

    engine = create_engine(conn_url)
    con = engine.connect()

    query = f'SELECT * FROM {table_name}'
    df = pd.read_sql(sql=query, con=con)
    con.close()
    return df


def df_table(dataframe, database_name, table_name):
    conn_url = URL.create(
        "mysql+mysqlconnector",
        username=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        database=database_name
    )

    engine = create_engine(conn_url)
    con = engine.connect()
    dataframe.to_sql(table_name, con=con, if_exists='replace', index=False)
    con.close()


