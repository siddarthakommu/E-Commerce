from google.cloud import bigquery
from config import PROJECT_ID, DATASET_ID

# Initialize the BigQuery client
client = bigquery.Client(project=PROJECT_ID)

def execute_kpi_queries():
    """Executes KPI queries and stores results in separate tables."""

    kpi_queries = {
        # "average_order_value": {
        #     "query": """
        #         SELECT 
        #             'average_order_value' AS kpi_name,
        #             SUM(f.price) / COUNT(DISTINCT f.order_id) AS kpi_value
        #         FROM 
        #             `e-commerce-453806.e_commerce.fact_orders` f;
        #     """,
        #     "table_name": "average_order_value"
        # },
        "customer_lifetime_value": {
            "query": """
                WITH repeat_customers AS (
                    SELECT f.customer_id, COUNT(f.order_id) AS order_count
                    FROM `e-commerce-453806.e_commerce.fact_orders` f
                    GROUP BY f.customer_id
                    HAVING COUNT(f.order_id) > 1
                )
                SELECT 
                    'customer_lifetime_value' AS kpi_name,
                    (SUM(f.payment_value) / COUNT(DISTINCT f.customer_id)) * 3 AS kpi_value
                FROM 
                    `e-commerce-453806.e_commerce.fact_orders` f
                WHERE 
                    f.customer_id IN (SELECT customer_id FROM repeat_customers);
            """,
            "table_name": "customer_lifetime_value"
        },
        "seller_performance_score": {
            "query": """
                WITH avg_review AS (
                    SELECT f.seller_id, AVG(f.review_score) AS avg_review_score
                    FROM `e-commerce-453806.e_commerce.fact_orders` f
                    GROUP BY f.seller_id
                ),
                on_time_deliveries AS (
                    SELECT o.seller_id,
                        COUNT(CASE WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 1 END) * 100.0 / COUNT(o.order_id) AS on_time_rate
                    FROM `e-commerce-453806.e_commerce.fact_orders` o
                    JOIN `e-commerce-453806.e_commerce.dim_orders` d
                    ON o.order_id = d.order_id
                    GROUP BY o.seller_id
                )
                SELECT 
                    'seller_performance_score' AS kpi_name,
                    (AVG(a.avg_review_score) * 0.6) + (AVG(o.on_time_rate) * 0.4) AS kpi_value
                FROM 
                    avg_review a
                JOIN 
                    on_time_deliveries o
                ON 
                    a.seller_id = o.seller_id;
            """,
            "table_name": "seller_performance_score"
        },
        "order_cancellation_rate": {
            "query": """
                SELECT 
                    'order_cancellation_rate' AS kpi_name,
                    COUNT(CASE WHEN d.order_status = 'canceled' THEN 1 END) * 100.0 / COUNT(*) AS kpi_value
                FROM 
                    `e-commerce-453806.e_commerce.dim_orders` d;
            """,
            "table_name": "order_cancellation_rate"
        },
        "high_value_order_rate": {
            "query": """
                WITH avg_order AS (
                    SELECT SUM(f.price) / COUNT(DISTINCT f.order_id) AS aov
                    FROM `e-commerce-453806.e_commerce.fact_orders` f
                )
                SELECT 
                    'high_value_order_rate' AS kpi_name,
                    COUNT(CASE WHEN f.price > (1.5 * (SELECT aov FROM avg_order)) THEN 1 END) * 100.0 / COUNT(f.order_id) AS kpi_value
                FROM 
                    `e-commerce-453806.e_commerce.fact_orders` f;
            """,
            "table_name": "high_value_order_rate"
        }
    }

    schema = [
        bigquery.SchemaField("kpi_name", "STRING"),
        bigquery.SchemaField("kpi_value", "FLOAT"),
    ]

    for kpi_name, kpi_data in kpi_queries.items():
        query = kpi_data["query"]
        table_name = kpi_data["table_name"]
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        table_ref = client.dataset(DATASET_ID).table(table_name)

        try:
            client.get_table(table_ref)  # Check if table exists
            print(f"✅ Table '{table_name}' exists.")
        except:
            print(f"❌ Table '{table_name}' not found. Creating...")
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"✅ Created table '{table_name}'.")

        print(f"Executing KPI query: {kpi_name}")
        query_job = client.query(query)
        results = query_job.result()

        rows_to_insert = [{"kpi_name": row.kpi_name, "kpi_value": row.kpi_value} for row in results]

        if rows_to_insert:
            client.insert_rows_json(table_ref, rows_to_insert)
            print(f"✅ Inserted results for {kpi_name} into '{table_name}'.")
        else:
            print(f"⚠ No data returned for {kpi_name}.")

    print("✅ KPI queries executed successfully.")

# Run the KPI execution function
execute_kpi_queries()
