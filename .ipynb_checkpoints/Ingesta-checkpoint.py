from process.Extract import Extract
# from process.Transform import Transform
from process.Load import Load
import pandas as pd

extract = Extract()
# transform = Transform()
load = Load()

print("Inicio Extraccion de Datos")

print("Inicio Extraccion de Datos de mySQL")
customers_df = extract.read_my_sql('retail_db', 'customers')
orders_df = extract.read_my_sql('retail_db', 'orders')
order_items_df = extract.read_my_sql('retail_db', 'order_items')
print("Termina Extraccion de Datos de mySQL")

print("Inicio Extraccion de Datos de GCP")
products_df = extract.read_gcp_bucket('dep-etl-source-objects', 'retail/products')
print("Termina Extraccion de Datos de GCP")

print("Inicio Extraccion de Datos de MongoDB")
categories_df = extract.read_mongodb('retail_db', 'categories')
departments_df =  extract.read_mongodb('retail_db', 'departments')
print("Termina Extraccion de Datos de MongoDB")

print("Termina Extraccion de Datos Total")

print("Inicia Carga de Datos Landing")
#load GCP cloud
load.load_gcp_bucket(customers_df  , 'dep-etl-source-objects', 'landing/customers')
load.load_gcp_bucket(orders_df     , 'dep-etl-source-objects', 'landing/orders')
load.load_gcp_bucket(order_items_df, 'dep-etl-source-objects', 'landing/order_items')
load.load_gcp_bucket(products_df   , 'dep-etl-source-objects', 'landing/products')
load.load_gcp_bucket(categories_df , 'dep-etl-source-objects', 'landing/categories')
load.load_gcp_bucket(departments_df, 'dep-etl-source-objects', 'landing/departments')
print("Termina Carga de Datos Landing")