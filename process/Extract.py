# from ProyectoEndToEndPython.Proyecto.utils.utils import *
import os
from io import StringIO
from google.cloud.storage import Client
from azure.storage.blob import ContainerClient
from pymongo import MongoClient
import sqlalchemy as db
from sqlalchemy import text
import pandas as pd
from pandas import DataFrame
from utils import utilitarios as u

class Extract():            
    
    def __init__(self):          
        self.process = 'Extract Process'    
        print(self.process)
        
    def read_my_sql(self, db, tbl_name):                        
        try:
            df = pd.read_sql_query(text(f'SELECT * FROM {tbl_name}'), con=u.get_db_conn(db)[0])
        except Exception as err:
            print(f"Data read from mySql Error: {err=}, {type(err)=}")            
            raise
        return df                
    
    def read_mongodb(self, db, tbl_name):
        try:            
            dbname = u.get_mongo_client(db)            
            collection_name = dbname[tbl_name]
            tbl = collection_name.find({})
            df = DataFrame(tbl)        
        except Exception as err:
            print(f"Data read from MongoDB Error: {err=}, {type(err)=}")            
            raise
        return df                
    
    def read_gcp_bucket(self, container_name, path_file):    
        try:            
            bucket_client = u.get_cliente_cloud_storage()
            bucket = bucket_client.get_bucket(container_name)
            blob = bucket.get_blob(path_file)
            downloaded = blob.download_as_text(encoding="utf-8")
            df = pd.read_csv(StringIO(downloaded))        
        except Exception as err:
            print(f"Data read from GCP Bucket Error: {err=}, {type(err)=}")            
            raise
        return df      
    
    def read_azure_storage(self, container_name, path_file):
        try:            
            container_client = u.get_cliente_azure_storage(container_name)            
            blob = container_client.download_blob(path_file)
            df = pd.read_csv(StringIO(blob.content_as_text()))                
        except Exception as err:
            print(f"Data read from Azure Blob Storage Error: {err=}, {type(err)=}")            
            raise
        return df    