import pandas as pd
import random
import uuid
from faker import Faker


def generate_fake_data(num_records):
    """Generates fake data with Brazilian cities and states."""
    fake = Faker()

    # List of major Brazilian cities with their state abbreviations
    brazil_cities_states = [
        ("Sao Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"), 
        ("Brasilia", "DF"), ("Salvador", "BA"), ("Curitiba", "PR"), ("Fortaleza", "CE"), 
        ("Manaus", "AM"), ("Recife", "PE"), ("Porto Alegre", "RS"), ("Belem", "PA"), 
        ("Goiânia", "GO"), ("Florianopolis", "SC"), ("Cuiaba", "MT"), ("Natal", "RN")
    ]

    # Product categories (including new ones)
    product_categories = [
        "housewares", "electronics", "clothing", "home", "books", "sports",
        "baby", "computers_accessories", "health_beauty", "auto"
    ]

    # Payment methods
    payment_methods = ["credit_card", "paypal", "apple_pay", "boleto"]

    # Function to select a random Brazilian city-state pair
    def get_random_brazilian_location():
        return random.choice(brazil_cities_states)

    data = {
        'order_id': [str(uuid.uuid4()) for _ in range(num_records)],
        'customer_id': [str(uuid.uuid4()) for _ in range(num_records)],
        'seller_id': [str(uuid.uuid4()) for _ in range(num_records)],
        'product_id': [str(uuid.uuid4()) for _ in range(num_records)],
        'order_status': random.choices(['delivered', 'processing', 'shipped', 'canceled'], k=num_records),
        'order_purchase_timestamp': [fake.date_time_between(start_date='-5y', end_date='now') for _ in range(num_records)],
        'order_approved_at': [fake.date_time_between(start_date='-5y', end_date='now') for _ in range(num_records)],
        'order_delivered_carrier_date': [fake.date_time_between(start_date='-5y', end_date='now') for _ in range(num_records)],
        'order_delivered_customer_date': [fake.date_time_between(start_date='-5y', end_date='now') for _ in range(num_records)],
        'order_estimated_delivery_date': [fake.date_time_between(start_date='now', end_date='+30d') for _ in range(num_records)],
        'customer_zip_code_prefix': [random.randint(1000, 9999) for _ in range(num_records)],
        'payment_type': random.choices(payment_methods, k=num_records),
        'payment_installments': [float(random.randint(1, 12)) for _ in range(num_records)],
        'payment_value': [round(random.uniform(50, 500), 2) for _ in range(num_records)],
        'review_score': [random.randint(1, 5) for _ in range(num_records)],
        'price': [round(random.uniform(10, 1000), 2) for _ in range(num_records)],
        'freight_value': [round(random.uniform(5, 100), 2) for _ in range(num_records)],
        'product_photos_qty': [random.randint(1, 5) for _ in range(num_records)],
        'product_weight_g': [random.randint(100, 5000) for _ in range(num_records)],
        'product_length_cm': [random.randint(10, 100) for _ in range(num_records)],
        'product_height_cm': [random.randint(10, 100) for _ in range(num_records)],
        'product_width_cm': [random.randint(10, 100) for _ in range(num_records)],
        'product_category_name_english': random.choices(product_categories, k=num_records),
        'seller_zip_code_prefix': [random.randint(1000, 9999) for _ in range(num_records)],
    }

    # Assign random Brazilian cities & states
    seller_locations = [get_random_brazilian_location() for _ in range(num_records)]
    customer_locations = [get_random_brazilian_location() for _ in range(num_records)]

    # Unpack city & state pairs
    data['seller_city'], data['seller_state'] = zip(*seller_locations)
    data['customer_city'], data['customer_state'] = zip(*customer_locations)

    return pd.DataFrame(data)


