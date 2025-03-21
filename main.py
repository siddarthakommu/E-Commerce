from data_cleaning import clean_and_transform_data
from fake_data_generation import generate_fake_data
import pandas as pd
from data_cleaning import load_data_to_bigquery
from config import PROJECT_ID,DATASET_ID
from google.cloud import bigquery
#from kpi import execute_kpi_queries
from kpi2 import kpi_queries



def main():

#     # Download and load dataset
#     #download_kaggle_dataset('olistbr/brazilian-ecommerce', path='./data')

#     #Load individual DataFrames
#     customers = pd.read_csv(r"data/olist_customers_dataset.csv")
#     geolocation = pd.read_csv(r"data/olist_geolocation_dataset.csv")
#     order_items = pd.read_csv(r"data/olist_order_items_dataset.csv")
#     order_payments = pd.read_csv(r"data/olist_order_payments_dataset.csv")
#     order_reviews = pd.read_csv(r"data/olist_order_reviews_dataset.csv") 
#     orders = pd.read_csv(r"data/olist_orders_dataset.csv")
#     products = pd.read_csv(r"data/olist_products_dataset.csv")
#     sellers = pd.read_csv(r"data/olist_sellers_dataset.csv")
#     category_translation = pd.read_csv(r"data/product_category_name_translation.csv")

#     # Merge DataFrames
#     df = orders.merge(customers, on="customer_id", how="left")
#     df = df.merge(order_payments, on="order_id", how="left")
#     df = df.merge(order_reviews, on="order_id", how="left")
#     df = df.merge(order_items, on="order_id", how="left")
#     df = df.merge(products, on="product_id", how="left")
#     df = df.merge(category_translation, on="product_category_name", how="left")
#     df = df.merge(sellers, on="seller_id", how="left")

#     #Clean and transform data  gi
#     df = clean_and_transform_data(df)
#     df.to_csv('firstframe.csv',index=False)

#     #Generating Fake Data
#     fake_df = generate_fake_data(20000)
#     fake_df.to_csv('secondframe.csv',index=False)

#     # Append fake data to original data
#     df_merged = pd.concat([df, fake_df], ignore_index=True)
#     # Save merged DataFrame to CSV
#     df_merged.to_csv('dataframe.csv', index=False)


#     def load_data_to_bigquery(df, table_name):
#         client = bigquery.Client(project=PROJECT_ID)
#         table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
#         job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
#         job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
#         job.result()  # Wait for the job to complete
#         print(f"Data loaded to {table_name} successfully.")


#     fact_orders = df[[
#     "order_id", "customer_id", "seller_id", "product_id",
#     "payment_installments", "payment_value","payment_type", "price", "freight_value", "review_score"]].copy()


#     dim_customers = df[[    
#     "customer_id", "customer_zip_code_prefix", "customer_city", "customer_state"]].drop_duplicates().copy()

#     dim_sellers = df[[
#     "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]].drop_duplicates().copy()

#     dim_orders = df[[
#     "order_id", "order_status", "order_purchase_timestamp",
#     "order_approved_at", "order_delivered_carrier_date",
#     "order_delivered_customer_date", "order_estimated_delivery_date"]].drop_duplicates().copy()


#     dim_products = df[[
#     "product_id", "product_category_name_english",
#     "product_photos_qty", "product_weight_g",
#     "product_length_cm", "product_height_cm", "product_width_cm"]].drop_duplicates().copy()

#     dim_payment_types = df[["payment_type"]].drop_duplicates().copy()
#     dim_payment_types["payment_type_id"] = dim_payment_types.index.astype(str)

# # Map `payment_type_id` to `fact_orders`
#     dim_payment_types = df[["payment_type"]].drop_duplicates().reset_index(drop=True)  # Reset index
#     dim_payment_types["payment_type_id"] = dim_payment_types.index  # Assign numeric IDs

#     #Load data into BigQuery
#     load_data_to_bigquery(fact_orders, 'fact_orders')
#     load_data_to_bigquery(dim_customers, 'dim_customers')
#     load_data_to_bigquery(dim_products, 'dim_products')
#     load_data_to_bigquery(dim_orders, 'dim_orders')
#     load_data_to_bigquery(dim_sellers, 'dim_sellers')
#     load_data_to_bigquery(dim_payment_types, 'dim_payment_types')

    #execute_kpi_queries()
    kpi_queries()
   

if __name__ == "__main__":
    main()
