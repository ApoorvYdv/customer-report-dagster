import os
import pyodbc
from sqlalchemy import create_engine

#

def get_sql_conn():
    """return db connection."""
    # define the server and the database
    server = 'APOORV\SQLEXPRESS' 
    database = 'customer-details'

    # Define the connection string
    conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server}; \
                SERVER='+ server +'; \
                DATABASE='+ database +';\
                Trusted_Connection=yes;'
              )
    try:
        return conn
    except:
        print("Error loading the config file.")


def get_postgres_creds():
    #get password from environmnet var
    # pwd = os.environ['PGPASS']
    # uid = os.environ['PGUID']
    #
    server = 'localhost'
    db =  'customer-details'
    uid = 'postgres'
    pwd = 'apoorv' 
    port = 5432
    cs = create_engine(f'postgresql://{uid}:{pwd}@{server}:{port}/{db}')
    try:
        return cs
    except:
        print("Error loading the config file.")