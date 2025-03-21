import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px

# Initialize BigQuery Client
client = bigquery.Client()

# Function to fetch data from BigQuery
@st.cache_data
def fetch_data(query):
    query_job = client.query(query)
    df = query_job.to_dataframe()
    return df

# Streamlit UI
st.set_page_config(page_title="E-Commerce KPI Dashboard", layout="wide")

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Overview", "Schema", "KPIs", "Aggregates", "Data Marts"])

# -------------------------------------- #
# 🏠 Overview Page
# -------------------------------------- #
if page == "Overview":
    st.title(" E-Commerce Data Analysis")
    st.write("This dashboard provides insights into **order trends, customer behavior, and product performance**.")

# -------------------------------------- #
# 📂 Database Schema Page
# -------------------------------------- #
elif page == "Schema":
    st.title(" Database Schema")

    schema_options = ["fact_orders", "dim_customers", "dim_products", "dim_orders", "dim_sellers"]
    selected_table = st.selectbox("Select Table", schema_options)

    query = f"SELECT * FROM `e-commerce-453806.New_E_Commerce.{selected_table}`LIMIT 10000"
    df = fetch_data(query)
    st.dataframe(df)

# -------------------------------------- #
# 📊 KPIs by State & City
# -------------------------------------- #
elif page == "KPIs":
    st.title(" Key Performance Indicators (KPIs)")

    # Fetch unique states & cities
    location_query = """
        SELECT DISTINCT customer_state, customer_city
        FROM `e-commerce-453806.New_E_Commerce.dim_customers`
        ORDER BY customer_state, customer_city;
    """
    location_df = fetch_data(location_query)

    # Toggle: State & City OR Only State
    view_option = st.radio("View Type", ["State & City", "Only State"])

    if view_option == "State & City":
        selected_state = st.selectbox("Select State", location_df["customer_state"].unique())
        filtered_cities = location_df[location_df["customer_state"] == selected_state]["customer_city"].unique()
        selected_city = st.selectbox("Select City", filtered_cities)

        # KPI Query: Most & Least Sold Product Category
        category_query = f"""
            SELECT 
                p.product_category_name_english AS category,
                COUNT(f.order_id) AS total_orders
            FROM `e-commerce-453806.New_E_Commerce.fact_orders` f
            JOIN `e-commerce-453806.New_E_Commerce.dim_customers` c ON f.customer_id = c.customer_id
            JOIN `e-commerce-453806.New_E_Commerce.dim_products` p ON f.product_id = p.product_id
            WHERE c.customer_state = '{selected_state}' AND c.customer_city = '{selected_city}'
            GROUP BY category
            ORDER BY total_orders DESC;
        """
    else:  # Only State Selected
        selected_state = st.selectbox("Select State", location_df["customer_state"].unique())

        # KPI Query: Most & Least Sold Product Category for a STATE
        category_query = f"""
            SELECT 
                p.product_category_name_english AS category,
                COUNT(f.order_id) AS total_orders
            FROM `e-commerce-453806.New_E_Commerce.fact_orders` f
            JOIN `e-commerce-453806.New_E_Commerce.dim_customers` c ON f.customer_id = c.customer_id
            JOIN `e-commerce-453806.New_E_Commerce.dim_products` p ON f.product_id = p.product_id
            WHERE c.customer_state = '{selected_state}'
            GROUP BY category
            ORDER BY total_orders DESC;
        """

    df_category = fetch_data(category_query)

    if not df_category.empty:
        most_sold = df_category.iloc[0]
        least_sold = df_category.iloc[-1]

        st.success(f" Most Sold Category: **{most_sold['category']}** ({most_sold['total_orders']} orders)")
        st.error(f" Least Sold Category: **{least_sold['category']}** ({least_sold['total_orders']} orders)")

        # Visualization
        fig_category = px.bar(df_category, x="category", y="total_orders", title="Sales by Category", text_auto=True)
        st.plotly_chart(fig_category)
    else:
        st.warning("⚠ No data found for the selected location.")

    # -------------------------------------- #
    # 📈 High-Performing Product Categories KPI
    # -------------------------------------- #
    st.title(" KPI: High-Performing Product Categories Above Average Revenue")

    high_perf_query = """
        WITH category_revenue AS (
            SELECT 
                p.product_category_name_english AS category,
                SUM(f.payment_value) AS total_revenue
            FROM `e-commerce-453806.New_E_Commerce.fact_orders` f
            JOIN `e-commerce-453806.New_E_Commerce.dim_products` p 
            ON f.product_id = p.product_id
            GROUP BY category
        ),
        avg_revenue AS (
            SELECT AVG(total_revenue) AS avg_revenue
            FROM category_revenue
        )
        SELECT 
            cr.category, 
            cr.total_revenue
        FROM category_revenue cr, avg_revenue ar
        WHERE cr.total_revenue > ar.avg_revenue
        ORDER BY cr.total_revenue DESC;
    """
    
    df_kpi = fetch_data(high_perf_query)

    if not df_kpi.empty:
        st.subheader(" Categories with Revenue Above Average")
        fig_high_perf = px.bar(df_kpi, x="category", y="total_revenue", title="High-Performing Product Categories", text_auto=True)
        st.plotly_chart(fig_high_perf)
    else:
        st.warning("⚠ No high-performing categories found.")



# -------------------------------------- #
# 📊 Aggregates Page
# -------------------------------------- #
elif page == "Aggregates":
    st.title(" Aggregated Metrics")

    aggregate_options = ["agg_customer_orders", "agg_product_performance", "agg_state_revenue",
                         "agg_orders_per_installment", "agg_payment_type_ratios"]
    selected_agg = st.selectbox("Select Aggregate Table", aggregate_options, key="agg_table_select")

    # Fetch Data
    query = f"SELECT * FROM `e-commerce-453806.New_E_Commerce.{selected_agg}` LIMIT 10000"
    df_agg = fetch_data(query)

    if not df_agg.empty:
        st.dataframe(df_agg)

        # **Visualization Based on Selected Aggregate**
        if selected_agg == "agg_customer_orders":
            st.subheader(" Customer Order Distribution")
            fig = px.bar(df_agg, x="customer_state", y="total_orders", color="customer_state", 
                         title="Total Orders by State", text_auto=True)
            st.plotly_chart(fig)

        elif selected_agg == "agg_product_performance":
            st.subheader(" Product Performance Metrics")
            fig = px.bar(df_agg, x="category", y="total_orders", color="category", 
                         title="Total Orders per Product Category", text_auto=True)
            st.plotly_chart(fig)

        elif selected_agg == "agg_state_revenue":
            st.subheader(" State Revenue Insights")
            fig = px.bar(df_agg, x="customer_state", y="total_revenue", 
                         title="Revenue by State", text_auto=True)
            st.plotly_chart(fig)

        elif selected_agg == "agg_payment_type_ratios":
            st.subheader(" Payment Type Ratios")
            fig_payment = px.pie(df_agg, names="payment_type", values="percentage_of_orders",
                                 title="Payment Type Distribution", hole=0.4)
            st.plotly_chart(fig_payment)

        elif selected_agg == "agg_orders_per_installment":
            st.subheader(" Orders Per Installment")
            fig_installments = px.bar(df_agg, x="payment_installments", y="total_orders",
                                      text_auto=True, title="Total Orders Per Installment",
                                      labels={"payment_installments": "Number of Installments", 
                                              "total_orders": "Total Orders"})
            st.plotly_chart(fig_installments)
    
    else:
        st.warning(f"⚠ No data available for `{selected_agg}`.")
# -------------------------------------- #
#  Data Marts Page
# -------------------------------------- #
# elif page == "Data Marts":
#     st.title("Data Marts")

#     data_mart_options = ["sales_data_mart", "customer_insights_data_mart", "product_performance_data_mart"]
#     selected_mart = st.selectbox("Select Data Mart Table", data_mart_options)

#     query = f"SELECT * FROM `e-commerce-453806.New_E_Commerce.{selected_mart}` LIMIT 1000"
#     df_mart = fetch_data(query)

#     st.dataframe(df_mart)
elif page == "Data Marts":
    st.title(" Data Marts Insights")

    data_mart_options = ["sales_data_mart", "customer_insights_data_mart", "product_performance_data_mart"]
    selected_mart = st.selectbox("Select Data Mart Table", data_mart_options)

    query = f"SELECT * FROM `e-commerce-453806.New_E_Commerce.{selected_mart}` LIMIT 1000"
    df_mart = fetch_data(query)

    st.dataframe(df_mart)

    if selected_mart == "sales_data_mart":
        st.subheader("Sales Performance")
        fig = px.bar(df_mart, x="customer_state", y="revenue", color="customer_state",
                     title="Total Sales by State", text_auto=True)
        st.plotly_chart(fig)

    elif selected_mart == "customer_insights_data_mart":
        st.subheader(" Customer Insights")
        fig = px.pie(df_mart, names="customer_city", values="total_orders", 
                     title="Customer Distribution by City")
        st.plotly_chart(fig)

    elif selected_mart == "product_performance_data_mart":
        st.subheader(" Product Performance Insights")
        fig = px.scatter(df_mart, x="category", y="total_orders",
                 size="total_revenue", color="category",
                 title="Total Orders vs Revenue by Product Category")
        st.plotly_chart(fig)