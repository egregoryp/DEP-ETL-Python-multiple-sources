from process.Extract import Extract
# from process.Transform import Transform
from process.Load import Load
import pandas as pd

extract = Extract()
# transform = Transform()
load = Load()

print("Inicio Extraccion de Datos de landing Zone GCP")
#from GCP
q1_df = extract.read_gcp_bucket('dep-etl-source-objects', 'gold/enunciado1')
q2_df = extract.read_gcp_bucket('dep-etl-source-objects', 'gold/enunciado2')
q3_df = extract.read_gcp_bucket('dep-etl-source-objects', 'gold/enunciado3')
q4_df = extract.read_gcp_bucket('dep-etl-source-objects', 'gold/enunciado4')
q5_df = extract.read_gcp_bucket('dep-etl-source-objects', 'gold/enunciado5')
q6_df = extract.read_gcp_bucket('dep-etl-source-objects', 'gold/enunciado6')
print("Termina Extraccion de Datos de landing Zone GCP")

#load to DB DWH mySQL
# print("Inicio Carga de Datos a Azure SQL DB")
# load.df_to_sql(q1_df, 'mysql', 'retail_dw', 'enunciado1')
# load.df_to_sql(q2_df, 'mysql', 'retail_dw', 'enunciado2')
# load.df_to_sql(q3_df, 'mysql', 'retail_dw', 'enunciado3')
# load.df_to_sql(q4_df, 'mysql', 'retail_dw', 'enunciado4')
# load.df_to_sql(q5_df, 'mysql', 'retail_dw', 'enunciado5')
# load.df_to_sql(q6_df, 'mysql', 'retail_dw', 'enunciado6')
# print("Termina Carga de Datos a Azure SQL DB")

print("Inicio Carga de Datos a Azure SQL DB")
#load to DB DWH Azure SQL Server - Must Have ODBC driver
load.df_to_sql(q1_df, 'sqlserver', 'test_db', 'enunciado1')
load.df_to_sql(q2_df, 'sqlserver', 'test_db', 'enunciado2')
load.df_to_sql(q3_df, 'sqlserver', 'test_db', 'enunciado3')
load.df_to_sql(q4_df, 'sqlserver', 'test_db', 'enunciado4')
load.df_to_sql(q5_df, 'sqlserver', 'test_db', 'enunciado5')
load.df_to_sql(q6_df, 'sqlserver', 'test_db', 'enunciado6')
print("Termina Carga de Datos a Azure SQL DB")