import io
import os
from io import StringIO
from azure.storage.blob import ContainerClient
from google.cloud.storage import Client
from azure.storage.blob import ContainerClient
from pymongo import MongoClient
import sqlalchemy as db
import pandas as pd
from utils import utilitarios as u

class Load():                  
    
    def __init__(self):          
        self.process = 'Load Process'   
        print(self.process)        
    
    def load_mongodb(self, df, db, tbl_name):
        try:            
            dfin = df.copy()
            dfin.reset_index(inplace=False)
            df_to_dict = dfin.to_dict("records")
            dbname = u.get_mongo_client(db)
            dbname[tbl_name].insert_many(df_to_dict)
            print(f"{tbl_name} Data loaded to MongoDB Successfully!") 
        except Exception as err:
            print(f"Data load from MongoDB Error: {err=}, {type(err)=}")            
            raise
    
    def load_gcp_bucket(self, df, container_name, path_file):    
        try:            
            bucket_client = u.get_cliente_cloud_storage()
            bucket = bucket_client.get_bucket(container_name)                      
            blob = bucket.get_blob(path_file)                       
            bucket.blob(path_file).upload_from_string(df.to_csv(encoding = "utf-8", index=False), 'text/csv')
            print(f"{path_file} Data loaded to GCP Bucket Successfully!")        
        except Exception as err:
            print(f"Data load from GCP Bucket Error: {err=}, {type(err)=}")            
            raise
    
    def load_azure_storage(self, df, container_name, path_file):
        try:                        
            container_client = u.get_cliente_azure_storage(container_name)                        
            output = io.StringIO()
            output = df.to_csv(encoding = "utf-8", index=False)
            container_client.upload_blob(path_file, output, overwrite=True, encoding='utf-8')
            print(f"{path_file} Data loaded to Azure Blob Storage Successfully!")        
        except Exception as err:
            print(f"Data load from Azure Blob Storage Error: {err=}, {type(err)=}")            
            raise
            
    ######### <load to mySQL or SQL Server> #########
    def dtype_mapping(self):
        return {'object' : 'TEXT',
                'int64' : 'INT',
                'float64' : 'FLOAT',
                'datetime64' : 'DATETIME',
                'bool' : 'TINYINT',
                'category' : 'TEXT',
                'timedelta[ns]' : 'TEXT'}
            
    def gen_tbl_cols_sql(self, df, dbtype):
        try:
            dmap = self.dtype_mapping()
            if (dbtype == 'mysql'):
                sql = "pi_db_uid INT AUTO_INCREMENT PRIMARY KEY"
            elif (dbtype == 'sqlserver'):
                sql = "pi_db_uid INT IDENTITY(1,1) PRIMARY KEY"
            df1 = df.rename(columns = {"" : "nocolname"})
            hdrs = df1.dtypes.index
            hdrs_list = [(hdr, str(df1[hdr].dtype)) for hdr in hdrs]
            for hl in hdrs_list:
                sql += " ,{0} {1}".format(hl[0], dmap[hl[1]])
        except Exception as err:
            print(f"Error generating table columns statement for: {tbl_name}, error: {err=}, {type(err)=}")            
            raise
        return sql
            
    def create_mysql_tbl_schema(self, df, conn, db, tbl_name, dbtype='mysql'):
        try:        
            tbl_cols_sql = self.gen_tbl_cols_sql(df, dbtype)
            if (dbtype == 'mysql'):
                sql = "USE {0}; CREATE TABLE IF NOT EXISTS {1} ({2})".format(db, tbl_name, tbl_cols_sql)
            elif (dbtype == 'sqlserver'):
                sql = "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{1}' and xtype='U') CREATE TABLE {1} ({2})".format(db, tbl_name, tbl_cols_sql)
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            conn.commit()
        except Exception as err:
            print(f"Error Creating table {tbl_name}, error: {err=}, {type(err)=}")            
            raise
            
    def df_to_sql(self, df, dbtype, db, tbl_name):
        try:
            self.create_mysql_tbl_schema(df, u.get_db_conn_raw(db, dbtype)[0], db, tbl_name, dbtype)
            df.to_sql(tbl_name, u.get_db_conn_raw(db, dbtype)[1], if_exists='replace')
            print(f"Table {tbl_name} loaded succesfully!")
        except Exception as err:
            print(f"Error Loading table {tbl_name}, error: {err=}, {type(err)=}")
            raise
    ######### </load to mySQL or SQL Server> #########