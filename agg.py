from google.cloud import bigquery
from config import PROJECT_ID, DATASET_ID  
import time

# Initialize BigQuery Client
client = bigquery.Client(project=PROJECT_ID)

def execute_query(query, table_name):
    """Executes a query and writes results to a BigQuery table."""
    print(f"🚀 Running query for {table_name}...")

    try:
        query_job = client.query(query)  # Run query directly without setting a destination table
        query_job.result()  # Wait for the query to finish
        print(f"✅ {table_name} updated successfully!")
    except Exception as e:
        print(f"❌ Error executing query for {table_name}: {e}")

# -------------------- Aggregation Queries --------------------

def create_aggregates():
    """Creates aggregated tables in BigQuery."""
    
    agg_queries = {
        # "agg_order_summary": """
        #     CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.agg_order_summary` AS
        #     SELECT 
        #         order_status,
        #         COUNT(order_id) AS total_orders,
        #         SUM(payment_value) AS total_revenue,
        #         AVG(payment_value) AS avg_order_value
        #     FROM `{PROJECT_ID}.{DATASET_ID}.fact_orders`
        #     GROUP BY order_status;
        # """,

        # "agg_customer_orders": """
        #     CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.agg_customer_orders` AS
        #     SELECT 
        #         c.customer_id,
        #         c.customer_city,
        #         c.customer_state,
        #         COUNT(f.order_id) AS total_orders,
        #         SUM(f.payment_value) AS total_spent
        #     FROM `{PROJECT_ID}.{DATASET_ID}.fact_orders` f
        #     JOIN `{PROJECT_ID}.{DATASET_ID}.dim_customers` c 
        #     ON f.customer_id = c.customer_id
        #     GROUP BY c.customer_id, c.customer_city, c.customer_state;
        # """,

        # "agg_product_performance": """
        #     CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.agg_product_performance` AS
        #     SELECT 
        #         p.product_category_name_english AS category,
        #         COUNT(f.order_id) AS total_orders,
        #         SUM(f.payment_value) AS total_revenue,
        #         AVG(f.review_score) AS avg_review_score
        #     FROM `{PROJECT_ID}.{DATASET_ID}.fact_orders` f
        #     JOIN `{PROJECT_ID}.{DATASET_ID}.dim_products` p 
        #     ON f.product_id = p.product_id
        #     GROUP BY category;
        # """,

        # "agg_state_revenue": """
        #     CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.agg_state_revenue` AS
        #     SELECT 
        #         c.customer_state,
        #         SUM(f.payment_value) AS total_revenue
        #     FROM `{PROJECT_ID}.{DATASET_ID}.fact_orders` f
        #     JOIN `{PROJECT_ID}.{DATASET_ID}.dim_customers` c 
        #     ON f.customer_id = c.customer_id
        #     GROUP BY c.customer_state;
        # """
        "agg_orders_per_installment": """
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.agg_orders_per_installment` AS
        SELECT 
            payment_installments,
            COUNT(order_id) AS total_orders
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_orders`
        GROUP BY payment_installments
        ORDER BY payment_installments;
    """,
    
    "agg_payment_type_ratios": """
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.agg_payment_type_ratios` AS
        SELECT 
            p.payment_type,
            COUNT(f.order_id) AS total_orders,
            COUNT(f.order_id) * 100.0 / SUM(COUNT(f.order_id)) OVER() AS percentage_of_orders
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_orders` f
        JOIN `{PROJECT_ID}.{DATASET_ID}.dim_payment_types` p ON f.payment_type = p.payment_type
        GROUP BY p.payment_type
        ORDER BY total_orders DESC;
    """
    }

    for table_name, query in agg_queries.items():
        query = query.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID)  # Inject project & dataset
        execute_query(query, table_name)
        time.sleep(2)  # Prevent API rate limits

    print("All Aggregates Updated in BigQuery!")

