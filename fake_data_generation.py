import pandas as pd
import random
import uuid
from faker import Faker

def generate_fake_data(num_records):
    """Generates fake data for the specified columns with matching data types."""
    fake = Faker()
    
    data = {
        'order_id': [str(uuid.uuid4()) for _ in range(num_records)],  # String (UUID)
        'customer_id': [str(uuid.uuid4()) for _ in range(num_records)],  # String (UUID)
        'order_status': random.choices(['delivered', 'processing', 'shipped', 'canceled'], k=num_records),  # String (Categorical)
        'order_purchase_timestamp': [fake.date_time_between(start_date='-5y', end_date='now') for _ in range(num_records)],  # Datetime
        'order_approved_at': [fake.date_time_between(start_date='-5y', end_date='now') for _ in range(num_records)],  # Datetime
        'order_delivered_carrier_date': [fake.date_time_between(start_date='-5y', end_date='now') for _ in range(num_records)],  # Datetime
        'order_delivered_customer_date': [fake.date_time_between(start_date='-5y', end_date='now') for _ in range(num_records)],  # Datetime
        'order_estimated_delivery_date': [fake.date_time_between(start_date='now', end_date='+30d') for _ in range(num_records)],  # Datetime
        'customer_zip_code_prefix': [random.randint(1000, 9999) for _ in range(num_records)],  # Integer (4-digit zip)
        'customer_city': [fake.city() for _ in range(num_records)],  # String
        'customer_state': [fake.state_abbr() for _ in range(num_records)],  # String (State Abbreviation)
        'payment_type': random.choices(['credit_card', 'paypal', 'apple_pay', 'boleto'], k=num_records),  # String (Categorical)
        'payment_installments': [random.randint(1, 12) for _ in range(num_records)],  # Integer
        'payment_value': [round(random.uniform(50, 500), 2) for _ in range(num_records)],  # Float
        'review_score': [random.randint(1, 5) for _ in range(num_records)],  # Integer
        'product_id': [str(uuid.uuid4()) for _ in range(num_records)],  # String (UUID)
        'seller_id': [str(uuid.uuid4()) for _ in range(num_records)],  # String (UUID)
        'price': [round(random.uniform(10, 1000), 2) for _ in range(num_records)],  # Float
        'freight_value': [round(random.uniform(5, 100), 2) for _ in range(num_records)],  # Float
        'product_photos_qty': [float(random.randint(1, 5)) for _ in range(num_records)],  # Float
        'product_weight_g': [float(random.randint(100, 5000)) for _ in range(num_records)],  # Float
        'product_length_cm': [float(random.randint(10, 100)) for _ in range(num_records)],  # Float
        'product_height_cm': [float(random.randint(10, 100)) for _ in range(num_records)],  # Float
        'product_width_cm': [float(random.randint(10, 100)) for _ in range(num_records)],  # Float
        'product_category_name_english': random.choices(['housewares', 'electronics', 'clothing', 'home', 'books', 'sports'], k=num_records),  # String (Categorical)
        'seller_zip_code_prefix': [float(random.randint(1000, 9999)) for _ in range(num_records)],  # Float (Ensure consistent with dataset)
        'seller_city': [fake.city() for _ in range(num_records)],  # String
        'seller_state': [fake.state_abbr() for _ in range(num_records)]  # String (State Abbreviation)
    }
    
    return pd.DataFrame(data)