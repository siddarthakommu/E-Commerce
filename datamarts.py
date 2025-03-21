from google.cloud import bigquery
from config import PROJECT_ID, DATASET_ID  # Ensure you have a config file
import time

# Initialize BigQuery Client
client = bigquery.Client(project=PROJECT_ID)

def execute_query(query, table_name):
    """Executes a query and writes results to a BigQuery table."""
    print(f" Running query for {table_name}...")

    try:
        query_job = client.query(query)  # Run query directly without setting a destination table
        query_job.result()  # Wait for the query to finish
        print(f"{table_name} updated successfully!")
    except Exception as e:
        print(f"Error executing query for {table_name}: {e}")

def create_data_marts():
    """Creates Data Marts in BigQuery."""
    
    data_mart_queries = {
        "sales_data_mart": """
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.sales_data_mart` AS
            SELECT 
                f.order_id,
                f.payment_value AS revenue,
                p.product_category_name_english AS category,
                c.customer_state,
                f.review_score
            FROM `{PROJECT_ID}.{DATASET_ID}.fact_orders` f
            JOIN `{PROJECT_ID}.{DATASET_ID}.dim_products` p 
            ON f.product_id = p.product_id
            JOIN `{PROJECT_ID}.{DATASET_ID}.dim_customers` c 
            ON f.customer_id = c.customer_id;
        """,

        "customer_insights_data_mart": """
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.customer_insights_data_mart` AS
            SELECT 
                c.customer_id,
                c.customer_city,
                c.customer_state,
                COUNT(f.order_id) AS total_orders,
                SUM(f.payment_value) AS total_spent,
                AVG(f.payment_value) AS avg_spent_per_order
            FROM `{PROJECT_ID}.{DATASET_ID}.fact_orders` f
            JOIN `{PROJECT_ID}.{DATASET_ID}.dim_customers` c 
            ON f.customer_id = c.customer_id
            GROUP BY c.customer_id, c.customer_city, c.customer_state;
        """,

        "product_performance_data_mart": """
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.product_performance_data_mart` AS
            SELECT 
                p.product_id,
                p.product_category_name_english AS category,
                COUNT(f.order_id) AS total_orders,
                SUM(f.payment_value) AS total_revenue,
                AVG(f.review_score) AS avg_review_score
            FROM `{PROJECT_ID}.{DATASET_ID}.fact_orders` f
            JOIN `{PROJECT_ID}.{DATASET_ID}.dim_products` p 
            ON f.product_id = p.product_id
            GROUP BY p.product_id, category;
        """
    }

    for table_name, query in data_mart_queries.items():
        query = query.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID)  # Inject project & dataset
        execute_query(query, table_name)
        time.sleep(2)  # Prevent API rate limits

    print("All Data Marts Updated in BigQuery!")

