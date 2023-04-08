import yaml 
import os
from io import StringIO
from google.cloud.storage import Client
from azure.storage.blob import ContainerClient
from pymongo import MongoClient
import pyodbc
import sqlalchemy as db

with open('/user/app/ProyectoEndToEndPython/Proyecto/config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)
    
def get_cliente_cloud_storage():    
    vGCP_CONC_BUCKET = config["cloud_storage"]["GCP_CONC_BUCKET"]
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=f"/user/app/ProyectoEndToEndPython/Clases/{vGCP_CONC_BUCKET}"
    client = Client()
    return client

def get_cliente_azure_storage(containerName):
    conn_str = config["azure_storage"]["AZURE_CONN_STR"]
    container = containerName
    container_client = ContainerClient.from_connection_string(
                        conn_str=conn_str,
                        container_name=container
                        )
    return container_client

def get_mongo_client(databaseName):
    CONNECTION_STRING = config["mongo_db"]["MONGODB_CONN_STR"]
    client = MongoClient(CONNECTION_STRING)
    dbname = client[databaseName]
    return dbname

def get_db_conn(databaseName, dbtype='mysql'):
    if (dbtype == 'mysql'):
        dbcol = 'database'
        dbprefix = 'mysql'
        dbsufix = ''
    elif (dbtype == 'sqlserver'):
        dbcol = 'databasems'
        dbprefix = 'mssql+pyodbc'
        dbsufix = '?driver=ODBC+Driver+17+for+SQL+Server'
    
    host= config[dbcol]["host"]
    user= config[dbcol]["user"]
    pwd = config[dbcol]["pass"]
    port= config[dbcol]["port"]
    engine = db.create_engine(f"{dbprefix}://{user}:{pwd}@{host}:{port}/{databaseName}{dbsufix}")
    # print(f"{dbprefix}://{user}:{pwd}@{host}:{port}/{databaseName}{dbsufix}")
    conn = engine.connect()
    return conn, engine

def get_db_conn_raw(databaseName, dbtype='mysql'):
    if (dbtype == 'mysql'):
        dbcol = 'database'
        dbprefix = 'mysql'
        dbsufix = ''
    elif (dbtype == 'sqlserver'):
        dbcol = 'databasems'
        dbprefix = 'mssql+pyodbc'
        dbsufix = '?driver=ODBC+Driver+17+for+SQL+Server'
    
    host= config[dbcol]["host"]
    user= config[dbcol]["user"]
    pwd = config[dbcol]["pass"]
    port= config[dbcol]["port"]
    engine = db.create_engine(f"{dbprefix}://{user}:{pwd}@{host}:{port}/{databaseName}{dbsufix}")
    # print(f"{dbprefix}://{user}:{pwd}@{host}:{port}/{databaseName}{dbsufix}")
    conn = engine.raw_connection()
    return conn, engine