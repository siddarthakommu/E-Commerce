
import kaggle
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

import pandas as pd

def clean_and_transform_data(df):
    """Cleans and transforms the data to retain only required columns for analysis and visualization."""

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Convert timestamps to datetime format
    date_cols = [
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date"
    ]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Handling missing values
    df['review_score'] = df['review_score'].fillna(df['review_score'].mean())  # Fill missing review scores with mean
    df['payment_installments'] = df['payment_installments'].fillna(df['payment_installments'].median())  # Fill with median

    # Fill categorical missing values
    df[['payment_type', 'product_category_name_english', 'seller_city', 'seller_state', 'customer_city', 'customer_state']] = \
        df[['payment_type', 'product_category_name_english', 'seller_city', 'seller_state', 'customer_city', 'customer_state']].fillna("Unknown")

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

# # def load_data_to_bigquery(df, table_name):
# #     """Loads the cleaned data into BigQuery."""
# #     table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
# #     job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
# #     job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
# #     job.result()  # Wait for the job to complete
# #     print(f"Data loaded to {table_name} successfully.")
# def load_data_to_bigquery(df, table_name):
#     """Loads the cleaned data into BigQuery."""
#     table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

#     # Print DataFrame Columns
#     print("Columns in DataFrame before upload:", df.columns.tolist())

#     # Ensure 'review_creation_date' exists before adding it to schema
#     expected_columns = ['order_purchase_timestamp', 'order_delivered_customer_date', 'order_estimated_delivery_date', 'review_creation_date']
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



# def generate_fake_data(num_records):
#     """Generates fake data."""
#     fake = Faker()
#     data = {
#         'order_id': [str(uuid.uuid4()) for _ in range(num_records)],
#         'customer_id': [str(uuid.uuid4()) for _ in range(num_records)],
#         'order_status': random.choices(['delivered', 'processing', 'shipped', 'canceled'], k=num_records),
#         'order_purchase_timestamp': [fake.date_time_between(start_date='-5y', end_date='now').isoformat().replace("T", " ") for _ in range(num_records)],
#         'order_delivered_customer_date': [fake.date_time_between(start_date='-5y', end_date='now').isoformat().replace("T", " ") for _ in range(num_records)],
#         'order_estimated_delivery_date': [fake.date_time_between(start_date='now', end_date='+30d').isoformat().replace("T", " ") for _ in range(num_records)],
#         'customer_city': [fake.city() for _ in range(num_records)],
#         'customer_state': [fake.state_abbr() for _ in range(num_records)],
#         'payment_sequential': [float(random.randint(1, 5)) for _ in range(num_records)],
#         'payment_type': random.choices(['credit_card', 'paypal', 'apple_pay', 'boleto'], k=num_records),
#         'payment_installments': [float(random.randint(1, 12)) for _ in range(num_records)],
#         'payment_value': [round(random.uniform(50, 500), 2) for _ in range(num_records)],
#         'review_id': [str(uuid.uuid4()) for _ in range(num_records)],
#         'review_score': [float(random.randint(1, 5)) for _ in range(num_records)],
#         'review_creation_date': [fake.date_time_between(start_date='-5y', end_date='now').isoformat() for _ in range(num_records)],
#         'product_id': [str(uuid.uuid4()) for _ in range(num_records)],
#         'seller_id': [str(uuid.uuid4()) for _ in range(num_records)],
#         'product_photos_qty': [float(random.randint(1, 5)) for _ in range(num_records)],
#         'product_weight_g': [float(random.randint(100, 5000)) for _ in range(num_records)],
#         'product_length_cm': [float(random.randint(10, 100)) for _ in range(num_records)],
#         'product_height_cm': [float(random.randint(10, 100)) for _ in range(num_records)],
#         'product_width_cm': [float(random.randint(10, 100)) for _ in range(num_records)],
#         'product_category_name_english': random.choices(['Electronics', 'Clothing', 'Home', 'Books', 'Sports'], k=num_records),
#         'seller_city': [fake.city() for _ in range(num_records)],
#         'seller_state': [fake.state_abbr() for _ in range(num_records)]
#     }
#     return pd.DataFrame(data)


