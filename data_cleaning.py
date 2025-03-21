
import kaggle
import pandas as pd
import random
import uuid
from faker import Faker
# from google.cloud import bigquery
# from config import PROJECT_ID, DATASET_ID


# Initialize the BigQuery client
#client = bigquery.Client()


# def importing_data():
#     kaggle.api.authenticate()
#     kaggle.api.dataset_download_files('olistbr/brazilian-ecommerce',path='./data', unzip=True)

# import pandas as pd
# from google.cloud import bigquery
# #from config import PROJECT_ID, DATASET_ID#import KAGGLE_DATASET 
# #import kaggle
# from faker import Faker
# import random
# import uuid

#Initialize the BigQuery client
#client = bigquery.Client(project=PROJECT_ID)




import pandas as pd

def clean_and_transform_data(df):
    """Cleans and transforms the data to retain only required columns for analysis and visualization."""

    # Remove duplicate rows
    df = df.drop_duplicates()
    import pandas as pd
    df['seller_zip_code_prefix'] = pd.to_numeric(df['seller_zip_code_prefix'], errors='coerce').fillna(0).astype(int)


    # Convert timestamps to datetime format
    date_cols = [
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date"
    ]
    import pandas as pd
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Handling missing values
    df['review_score'] = df['review_score'].fillna(df['review_score'].mean())  # Fill missing review scores with mean
    df['payment_installments'] = df['payment_installments'].fillna(df['payment_installments'].median())  # Fill with median

    # Fill categorical missing values
    # df[['payment_type', 'product_category_name_english', 'seller_city', 'seller_state', 'customer_city', 'customer_state']] = \
    #     df[['payment_type', 'product_category_name_english', 'seller_city', 'seller_state', 'customer_city', 'customer_state']].fillna("Unknown")
    
    import random
    product_categories = ['housewares', 'electronics', 'clothing', 'home', 'books', 'sports']
    payment_methods = ['credit_card', 'paypal', 'apple_pay', 'boleto']

    # Fill missing `product_category_name_english` with a random category
    df['product_category_name_english'] = df['product_category_name_english'].apply(
    lambda x: random.choice(product_categories) if pd.isna(x) else x)


    # Fill missing `payment_type` with a random payment method
    df['payment_type'] = df['payment_type'].apply(
    lambda x: random.choice(payment_methods) if pd.isna(x) else x)

    import random
    import pandas as pd

    # List of product categories (including new ones)
    product_categories = [
    "housewares", "electronics", "clothing", "home", "books", "sports",
    "baby", "computers_accessories", "health_beauty", "auto"]

    # List of major Brazilian cities (in English) with their respective state abbreviations
    brazil_cities_states = [
    ("Sao Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"), 
    ("Brasilia", "DF"), ("Salvador", "BA"), ("Curitiba", "PR"), ("Fortaleza", "CE"), 
    ("Manaus", "AM"), ("Recife", "PE"), ("Porto Alegre", "RS"), ("Belem", "PA"), 
    ("Goiania", "GO"), ("Florianopolis", "SC"), ("Cuiaba", "MT"), ("Natal", "RN")]

    # Payment methods
    payment_methods = ["credit_card", "paypal", "apple_pay", "boleto"]

    # Function to randomly select a city and state
    def get_random_brazilian_location():
        return random.choice(brazil_cities_states)

    # Fill missing `product_category_name_english` with a random category
    df['product_category_name_english'] = df['product_category_name_english'].apply(
    lambda x: random.choice(product_categories) if pd.isna(x) else x)

    # Fill missing `payment_type` with a random payment method
    df['payment_type'] = df['payment_type'].apply(
    lambda x: random.choice(payment_methods) if pd.isna(x) else x)

    # Fill missing `seller_city`, `seller_state` with random values
    df[['seller_city', 'seller_state']] = df[['seller_city', 'seller_state']].apply(
    lambda x: get_random_brazilian_location() if pd.isna(x).any() else x, axis=1)

    # Fill missing `customer_city`, `customer_state` with random values
    df[['customer_city', 'customer_state']] = df[['customer_city', 'customer_state']].apply(
    lambda x: get_random_brazilian_location() if pd.isna(x).any() else x, axis=1)


    # Replace NaN with median for numerical columns
    num_cols = [
        'price', 'freight_value', 'product_photos_qty', 'product_weight_g',
        'product_length_cm', 'product_height_cm', 'product_width_cm'
    ]

    df[num_cols] = df[num_cols].apply(lambda x: x.fillna(x.median()), axis=0)

    # Drop unnecessary columns not required for analysis
    df = df.drop([
        'customer_unique_id', 'payment_sequential', 'review_id', 'review_comment_title',
        'review_comment_message', 'review_creation_date', 'review_answer_timestamp',
        'order_item_id', 'shipping_limit_date', 'product_category_name',
        'product_name_lenght', 'product_description_lenght'
    ], axis=1, errors='ignore')

    # Fill timestamps with relevant values where missing
    df['order_approved_at'] = df['order_approved_at'].fillna(df['order_purchase_timestamp'])
    df['order_delivered_carrier_date'] = df['order_delivered_carrier_date'].fillna(df['order_approved_at'])
    df['order_delivered_customer_date'] = df['order_delivered_customer_date'].fillna(df['order_estimated_delivery_date'])

    # Drop remaining records containing null values
    df = df.dropna()

    return df

# def load_data_to_bigquery(df, table_name):
#     """Loads the cleaned data into BigQuery."""
#     table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
#     job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
#     job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
#     job.result()  # Wait for the job to complete
#     print(f"Data loaded to {table_name} successfully.")


# def load_data_to_bigquery(df, table_name):
#     """Loads the cleaned data into BigQuery."""
#     table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

#     # Print DataFrame Columns
#     print("Columns in DataFrame before upload:", df.columns.tolist())

#     # Ensure 'review_creation_date' exists before adding it to schema
#     expected_columns = ['order_purchase_timestamp', 'order_delivered_customer_date', 'order_estimated_delivery_date']
#     available_columns = [col for col in expected_columns if col in df.columns]

#     # Convert to datetime format
#     for col in available_columns:
#         df[col] = pd.to_datetime(df[col], errors='coerce')

#     # Define BigQuery Schema dynamically based on available columns
#     schema = [bigquery.SchemaField(col, "TIMESTAMP") for col in available_columns]

#     job_config = bigquery.LoadJobConfig(
#         schema=schema,
#         write_disposition="WRITE_TRUNCATE",
#     )

#     # Upload to BigQuery
#     job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
#     job.result()  # Wait for job completion
#     print(f"Data loaded to {table_name} successfully.")













