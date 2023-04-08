from process.Extract import Extract
from process.Transform import Transform
from process.Load import Load
import pandas as pd

extract = Extract()
transform = Transform()
load = Load()

print("Inicio Extraccion de Datos de landing Zone GCP")
#load from GCP adls landing zone
customers_df   = extract.read_gcp_bucket( 'dep-etl-source-objects', 'landing/customers')
orders_df      = extract.read_gcp_bucket( 'dep-etl-source-objects', 'landing/orders')
order_items_df = extract.read_gcp_bucket( 'dep-etl-source-objects', 'landing/order_items')
products_df    = extract.read_gcp_bucket( 'dep-etl-source-objects', 'landing/products')
categories_df  = extract.read_gcp_bucket( 'dep-etl-source-objects', 'landing/categories')
departments_df = extract.read_gcp_bucket( 'dep-etl-source-objects', 'landing/departments')
print("Termina Extraccion de Datos de GCP")

print("Inicio Tansformacion Enunciado 1")
#Enunciando 1 - Top 5 Clientes con mas ordenes
q1_df = transform.transform_from_adls_q1(orders_df, customers_df)
# q1_df
print("Termina Tansformacion Enunciado 1")

print("Inicio Tansformacion Enunciado 2")
#Enunciado 2 - Top 5 Clientes con mas ordenes Canceladas
q2_df = transform.transform_from_adls_q2(orders_df, customers_df)
# q2_df
print("Termina Tansformacion Enunciado 2")

print("Inicio Tansformacion Enunciado 3")
#Enunciado 3 - Cantidad de Ordenes por mes
q3_df = transform.transform_from_adls_q3(orders_df)
# q3_df
print("Termina Tansformacion Enunciado 3")

print("Inicio Tansformacion Enunciado 4")
#Enunciado 4 - Top 5 clientes con mayor facturacion
q4_df = transform.transform_from_adls_q4(orders_df, customers_df, order_items_df)
# q4_df
print("Termina Tansformacion Enunciado 4")

print("Inicio Tansformacion Enunciado 5")
#Enunciado 5 - Dias de la semana de Mayor Facturacion
q5_df = transform.transform_from_adls_q5(orders_df, order_items_df)
# q5_df.sort_values(by=['weekday_number'])
print("Termina Tansformacion Enunciado 5")

print("Inicio Tansformacion Enunciado 6")
#Enunciado 6 - Top 5 tipos de Items (Productos) mas vendidos
q6_df = transform.transform_from_adls_q6(orders_df, order_items_df, products_df)
# q6_df
print("Termina Tansformacion Enunciado 6")

print("Inicio Carga de Datos a Gold Zone GCP")
load.load_gcp_bucket(q1_df, 'dep-etl-source-objects', 'gold/enunciado1')
load.load_gcp_bucket(q2_df, 'dep-etl-source-objects', 'gold/enunciado2')
load.load_gcp_bucket(q3_df, 'dep-etl-source-objects', 'gold/enunciado3')
load.load_gcp_bucket(q4_df, 'dep-etl-source-objects', 'gold/enunciado4')
load.load_gcp_bucket(q5_df, 'dep-etl-source-objects', 'gold/enunciado5')
load.load_gcp_bucket(q6_df, 'dep-etl-source-objects', 'gold/enunciado6')
print("Termia Carga de Datos a Gold Zone GCP")