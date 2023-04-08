import pandas as pd

class Transform():    
    
    def transform_from_adls_q1(self, orders_df, customers_df):    
        try:
            #Enunciando 1 - Top 5 Clientes con mas ordenes
            top_customers_df = orders_df.merge(customers_df, left_on='order_customer_id', right_on='customer_id', how='inner')
            top_customers_df = (top_customers_df[['customer_id', 'customer_fname', 'customer_lname', 'order_id']]
                                .groupby(['customer_id', 'customer_fname', 'customer_lname'])
                                .count())
            top_customers_df = (top_customers_df.rename({'order_id': 'order_qty'}, axis=1)
                                .sort_values(by=['order_qty'], ascending=False))
            #reset index because by default it creates an index when grouping
            top_customers_df.reset_index(inplace=True)
        except Exception as err:
            print(f"Error In Transform for Enunciado 1: {err=}, {type(err)=}")            
            raise
        return top_customers_df.head()
    
    def transform_from_adls_q2(self, orders_df, customers_df):    
        try:
            #Enunciado 2 - Top 5 Clientes con mas ordenes Canceladas
            top_customers_canc_df = orders_df.merge(customers_df, left_on='order_customer_id', right_on='customer_id', how='inner')
            top_customers_canc_df = top_customers_canc_df[ (top_customers_canc_df['order_status']=='CANCELED') ]
            top_customers_canc_df = (top_customers_canc_df[['customer_id', 'customer_fname', 'customer_lname', 'order_id']]
                                .groupby(['customer_id', 'customer_fname', 'customer_lname'])
                                .count())
            top_customers_canc_df = (top_customers_canc_df.rename({'order_id': 'order_qty'}, axis=1)
                                .sort_values(by=['order_qty'], ascending=False))
            #reset index because by default it creates an index when grouping
            top_customers_canc_df.reset_index(inplace=True)
        except Exception as err:
            print(f"Error In Transform for Enunciado 2: {err=}, {type(err)=}")            
            raise
        return top_customers_canc_df.head()
    
    def transform_from_adls_q3(self, orders_df):    
        try:
            #Enunciado 3 - Cantidad de Ordenes por mes
            orders_x_month = orders_df[['order_date', 'order_id']]
            orders_x_month['order_date'] = pd.to_datetime(orders_x_month['order_date'], errors='coerce')
            orders_x_month['Month'] = orders_x_month['order_date'].dt.strftime('%m')
            orders_x_month = (orders_x_month[['Month', 'order_id']]
                                .groupby(['Month'])
                                .count())
            orders_x_month = orders_x_month.rename({'order_id': 'order_qty'}, axis=1)
            #reset index because by default it creates an index when grouping
            orders_x_month.reset_index(inplace=True)
        except Exception as err:
            print(f"Error In Transform for Enunciado 3: {err=}, {type(err)=}")            
            raise
        return orders_x_month.head(12)
    
    def transform_from_adls_q4(self, orders_df, customers_df, order_items_df):    
        try:
            #Enunciado 4 - Top 5 clientes con mayor facturacion
            top_customers_fact_df = orders_df.merge(customers_df, left_on='order_customer_id', right_on='customer_id', how='inner')
            top_customers_fact_df = top_customers_fact_df.merge(order_items_df, left_on='order_id', right_on='order_item_order_id', how='inner')
            top_customers_fact_df = (top_customers_fact_df[['customer_id', 'customer_fname', 'customer_lname', 'order_item_subtotal']]
                                .groupby(['customer_id', 'customer_fname', 'customer_lname'])
                                .sum())
            top_customers_fact_df = (top_customers_fact_df.rename({'order_item_subtotal': 'purchases_total'}, axis=1)
                                .sort_values(by=['purchases_total'], ascending=False))
            #reset index because by default it creates an index when grouping
            top_customers_fact_df.reset_index(inplace=True)
        except Exception as err:
            print(f"Error In Transform for Enunciado 4: {err=}, {type(err)=}")            
            raise
        return top_customers_fact_df.head()
    
    def transform_from_adls_q5(self, orders_df, order_items_df):   
        try:
            #Enunciado 5 - Dias de la semana de Mayor Facturacion
            orders_x_day = orders_df.merge(order_items_df, left_on='order_id', right_on='order_item_order_id', how='inner')
            orders_x_day = orders_x_day[['order_date', 'order_item_subtotal']]
            orders_x_day['order_date'] = pd.to_datetime(orders_x_day['order_date'], errors='coerce')
            orders_x_day['weekday'] = orders_x_day['order_date'].dt.day_name()
            orders_x_day['weekday_number'] = orders_x_day['order_date'].dt.dayofweek
            orders_x_day = (orders_x_day[['weekday_number', 'weekday', 'order_item_subtotal']]
                                .groupby(['weekday_number', 'weekday'])
                                .sum())
            orders_x_day = (orders_x_day.rename({'order_item_subtotal': 'purchases_total'}, axis=1)
                            .sort_values(by=['purchases_total'], ascending=False))
            #reset index because by default it creates an index when grouping
            orders_x_day.reset_index(inplace=True)
        except Exception as err:
            print(f"Error In Transform for Enunciado 5: {err=}, {type(err)=}")            
            raise
        return orders_x_day.head(12)

    def transform_from_adls_q6(self, orders_df, order_items_df, products_df):   
        try:
            top_items_fact_df = orders_df.merge(order_items_df, left_on='order_id', right_on='order_item_order_id', how='inner')
            top_items_fact_df = top_items_fact_df.merge(products_df, left_on='order_item_product_id', right_on='product_id', how='inner')
            top_items_fact_df = (top_items_fact_df[['product_id', 'product_category_id', 'product_name', 'order_item_id']]
                                .groupby(['product_id', 'product_category_id', 'product_name'])
                                .count())
            top_items_fact_df = (top_items_fact_df.rename({'order_item_id': 'item_qty_purchased'}, axis=1)
                                .sort_values(by=['item_qty_purchased'], ascending=False))
            #reset index because by default it creates an index when grouping
            top_items_fact_df.reset_index(inplace=True)
        except Exception as err:
            print(f"Error In Transform for Enunciado 6: {err=}, {type(err)=}")            
            raise
        return top_items_fact_df.head()