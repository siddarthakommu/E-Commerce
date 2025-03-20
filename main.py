from data_cleaning import clean_and_transform_data
import pandas as pd


def main():

    # Download and load dataset
    #download_kaggle_dataset('olistbr/brazilian-ecommerce', path='./data')

    # Load individual DataFrames
    customers = pd.read_csv(r"data/olist_customers_dataset.csv")
    geolocation = pd.read_csv(r"data/olist_geolocation_dataset.csv")
    order_items = pd.read_csv(r"data/olist_order_items_dataset.csv")
    order_payments = pd.read_csv(r"data/olist_order_payments_dataset.csv")
    order_reviews = pd.read_csv(r"data/olist_order_reviews_dataset.csv")
    orders = pd.read_csv(r"data/olist_orders_dataset.csv")
    products = pd.read_csv(r"data/olist_products_dataset.csv")
    sellers = pd.read_csv(r"data/olist_sellers_dataset.csv")
    category_translation = pd.read_csv(r"data/product_category_name_translation.csv")

    # Merge DataFrames
    df = orders.merge(customers, on="customer_id", how="left")
    df = df.merge(order_payments, on="order_id", how="left")
    df = df.merge(order_reviews, on="order_id", how="left")
    df = df.merge(order_items, on="order_id", how="left")
    df = df.merge(products, on="product_id", how="left")
    df = df.merge(category_translation, on="product_category_name", how="left")
    df = df.merge(sellers, on="seller_id", how="left")

    # Clean and transform data  
    df = clean_and_transform_data(df)
    df.to_csv('firstframe.csv',index=False)

if __name__ == "__main__":
    main()
