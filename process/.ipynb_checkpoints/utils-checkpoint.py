import os
from io import StringIO
from google.cloud.storage import Client
from azure.storage.blob import ContainerClient
from pymongo import MongoClient
import sqlalchemy as db
from sqlalchemy import text
import pandas as pd
from pandas import DataFrame
from dotenv import load_dotenv

def create_connections():
    load_dotenv()

    vSQL_CONN_STRING     = os.getenv("SQL_CONN_STRING")
    vGCP_CONC_BUCKET     = os.getenv("GCP_CONC_BUCKET")
    vAZURE_CONN_STR      = os.getenv("AZURE_CONN_STR")
    vMONGODB_CONN_STR    = os.getenv("MONGODB_CONN_STR")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=f"/user/app/ProyectoEndToEndPython/Clases/{vGCP_CONC_BUCKET}"

    #mySQL connection
    engine = db.create_engine(vSQL_CONN_STRING)
    conn = engine.connect()
    #mongoDB connection
    mongoClient = MongoClient(vMONGODB_CONN_STR)
    #GCP Client Conn
    client = Client() 
    
    return engine, conn, mongoClient, client