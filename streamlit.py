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
    st.title("E-Commerce Dashboard")

    st.markdown("""
    ##  Unlock Actionable Insights from E-Commerce Data  

    Welcome to the **E-Commerce KPI Dashboard**, designed to help businesses track, analyze, and optimize key performance indicators (KPIs).  
    This dashboard enables **data-driven decision-making** by providing **real-time insights** into sales, customer trends, and product performance.

    ### Key Features  
    - **Database Schema**: Understand the data model and structure.  
    - **Yearly KPIs**: Track revenue growth, customer retention, and order volume.  
    - **Product Performance**: Identify best-selling and least-selling product categories.  
    - **Regional Insights**: Analyze sales trends at **state** and **city** levels.  
    - **Aggregated Business Metrics**: Get pre-processed insights on customer behavior, payment trends, and order patterns.  
    - **Data Marts**: Optimized datasets for efficient business intelligence reporting.  

    **Use the sidebar** to navigate through different sections and explore the data-driven insights.""")

# -------------------------------------- #
# 📂 Database Schema Page
# -------------------------------------- #
# -------------------------------------- #
# 📂 Database Schema Page
# -------------------------------------- #
elif page == "Schema":
    st.title("Database Schema")

    st.markdown("""
    ## Understanding the Data Model  

    The database follows a **star schema** model with a central **fact table** and multiple **dimension tables** for analytical efficiency.  

    ### **Core Tables**  
    - **fact_orders**: Stores transaction-level sales and order data.  
    - **dim_customers**: Contains customer demographics and location details.  
    - **dim_products**: Holds product catalog data, including categories and attributes.  
    - **dim_orders**: Provides details on order processing, shipping, and delivery.  
    - **dim_sellers**: Includes seller information and operational metrics.  
    - **dim_payment_types**: Has the data aboout the differernt payment types.

    **Explore the schema below:**  
    """)

    # Display schema image without aggregates and data marts
    st.image("schema.png", caption="Database Schema", use_container_width=True)

    st.markdown("""
    ## Additional Data Processing Layers  
    Beyond the core schema, **Aggregates** and **Data Marts** play a crucial role in optimizing analytics:  

    - **Aggregates**: Precomputed summaries for fast analysis of sales trends, customer behavior, and payment insights.  
    - **Data Marts**: Optimized datasets designed for advanced business intelligence, enabling deeper market and product analysis.  

    While these are not directly represented in the schema, they **enhance performance** and enable **efficient data retrieval** for visualization and reporting.
    """)



# -------------------------------------- #
#  KPIs by State & City
# -------------------------------------- #
elif page == "KPIs":
    st.title("Key Performance Indicators (KPIs)")

   # Fetch unique states  
    location_query = """  
    SELECT DISTINCT customer_state  
    FROM `e-commerce-453806.New_E_Commerce.dim_customers`  
    ORDER BY customer_state;  
"""  
    location_df = fetch_data(location_query)  

# State Selection  
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

        st.success(f"📈 Most Sold Category: **{most_sold['category']}** ({most_sold['total_orders']} orders)")  
        st.error(f"📉 Least Sold Category: **{least_sold['category']}** ({least_sold['total_orders']} orders)")  

    # Visualization  
        fig_category = px.bar(df_category, x="category", y="total_orders", title="Sales by Category", text_auto=True)  
        st.plotly_chart(fig_category)  
    else:  
        st.warning("⚠ No data found for the selected state.")  


    # -------------------------------------- #
    # High-Performing Product Categories KPI
    # -------------------------------------- #
    st.title("High-Performing Product Categories Above Average Revenue")

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
# Yearly KPIs Section
# -------------------------------------- #
    st.title(" Yearly KPI Analysis")

    yearly_kpi_options = [
    "Kpi_yearly_revenue_growth",
    "Kpi_yearly_avg_order_value",
    "Kpi_yearly_order_volume",
    "Kpi_yearly_popular_category"]
    selected_yearly_kpi = st.selectbox("Select Yearly KPI", yearly_kpi_options, key="yearly_kpi_dropdown")

# Fetch Data
    query_yearly = f"SELECT * FROM `e-commerce-453806.New_E_Commerce.{selected_yearly_kpi}` ORDER BY year"
    df_yearly_kpi = fetch_data(query_yearly)

# Check if data exists
    if df_yearly_kpi.empty:
        st.warning("⚠ No data available for the selected KPI.")
    else:
        st.dataframe(df_yearly_kpi)

        #  **Yearly Revenue Growth**
    if selected_yearly_kpi == "Kpi_yearly_revenue_growth":
        st.subheader(" Yearly Revenue Growth")
    
        if "year" in df_yearly_kpi.columns and "revenue_growth_percentage" in df_yearly_kpi.columns and "total_revenue" in df_yearly_kpi.columns:
            # ✅ Handle NaN values
            df_yearly_kpi = df_yearly_kpi.dropna(subset=["revenue_growth_percentage", "total_revenue"])

        if df_yearly_kpi.empty:
            st.warning("⚠ No valid data available after removing NaN values.")
        else:
            # ✅ Dual-axis plot: Bar for revenue & Line for growth %
            fig = px.bar(
                df_yearly_kpi, 
                x="year", 
                y="total_revenue",
                text=df_yearly_kpi["total_revenue"].round(2),
                title="Yearly Revenue and Growth",
                labels={"total_revenue": "Total Revenue ($)", "year": "Year"},
                color="year"
            )

            # ✅ Add revenue growth as a line plot on the same figure
            fig.add_scatter(
                x=df_yearly_kpi["year"], 
                y=df_yearly_kpi["revenue_growth_percentage"], 
                mode="lines+markers+text",
                text=df_yearly_kpi["revenue_growth_percentage"].round(2),
                name="Revenue Growth (%)",
                textposition="top center"
            )

            st.plotly_chart(fig)

    

    # elif selected_yearly_kpi == "Kpi_yearly_customer_retention":
    #     st.subheader("👥 Yearly Customer Growth")

    #     if "year" in df_yearly_kpi.columns and "total_customers" in df_yearly_kpi.columns:
    #         fig = px.bar(
    #         df_yearly_kpi,
    #         x="year",
    #         y="new_customers",
    #         text=df_yearly_kpi["total_customers"],
    #         title="New Customers Per Year",
    #         labels={"total_customers": "total Customers"},
    #         color="total_customers",
    #         color_continuous_scale="blues"
    #     )
    #         fig.update_traces(textposition="outside")
    #         st.plotly_chart(fig)
    #     else:
    #         st.warning("⚠ Required columns missing: 'year' or 'new_customers'")






        #  **Average Order Value**
    elif selected_yearly_kpi == "Kpi_yearly_avg_order_value":
            if "avg_order_value" in df_yearly_kpi.columns:
                st.subheader(" Yearly Average Order Value")
                fig = px.line(df_yearly_kpi, x="year", y="avg_order_value", markers=True, 
                              title="Average Order Value Over the Years",
                              labels={"avg_order_value": "Average Order Value ($)"})
                st.plotly_chart(fig)
            else:
                st.warning("⚠ 'avg_order_value' column missing.")

    # **Yearly Order Volume**
    elif selected_yearly_kpi == "Kpi_yearly_order_volume":
        st.subheader(" Yearly Order Volume")

        if "year" in df_yearly_kpi.columns and "total_orders" in df_yearly_kpi.columns:
        # ✅ Handle NaN values in total_orders
            df_yearly_kpi = df_yearly_kpi.dropna(subset=["total_orders"])

            if df_yearly_kpi.empty:
                st.warning("⚠ No valid data available after removing NaN values.")
            else:
                fig = px.bar(
                df_yearly_kpi, 
                x="year", 
                y="total_orders",
                text=df_yearly_kpi["total_orders"],  # Show total orders on bars
                title="Total Orders Per Year",
                labels={"total_orders": "Total Orders", "year": "Year"},
                color="year"  # Different colors per year
            )

            # ✅ Ensure text labels are visible
                fig.update_traces(textposition="outside")  

                st.plotly_chart(fig)

        else:
            st.warning("⚠ Required columns missing: 'year' or 'total_orders'")



 
    #**Most Popular Product Category**
    elif selected_yearly_kpi == "Kpi_yearly_popular_category":
            if "total_orders" in df_yearly_kpi.columns and "category" in df_yearly_kpi.columns:
                st.subheader(" Yearly Most Popular Product Category")
                fig = px.bar(df_yearly_kpi, x="year", y="total_orders", color="category", text_auto=True,
                             title="Most Sold Product Category by Year",
                             labels={"total_orders": "Total Orders", "category": "Product Category"})
                st.plotly_chart(fig)
            else:
                st.warning("⚠ 'total_orders' or 'category' column missing.")



# -------------------------------------- #
#  Aggregates Page
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
        # if selected_agg == "agg_customer_orders":
        #     st.subheader(" Customer Order Distribution")
        #     fig = px.bar(df_agg, x="customer_state", y="total_orders", color="customer_state", 
        #                  title="Total Orders by State", text_auto=False)
        #     st.plotly_chart(fig)
        if selected_agg == "agg_customer_orders":
            st.subheader("Customer Order Distribution")

    # Ensure df_agg is not empty before processing
            if not df_agg.empty:
        # Aggregate total orders by state
                df_agg_grouped = df_agg.groupby("customer_state", as_index=False).agg({"total_orders": "sum"})

        # Plot the bar chart
                fig = px.bar(
            df_agg_grouped, 
            x="customer_state", 
            y="total_orders", 
            color="customer_state", 
            title="Total Orders by State", 
            text_auto=True  # Enables text labels on bars
            )

                st.plotly_chart(fig)
            else:
                st.warning("⚠ No data available for customer orders.")


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
                     title="Total Sales by State")
        st.plotly_chart(fig)

    elif selected_mart == "customer_insights_data_mart":
        st.subheader(" Customer Insights")
        fig = px.pie(df_mart, names="customer_state", values="total_orders", 
                     title="Customer Distribution by State")
        st.plotly_chart(fig)

    elif selected_mart == "product_performance_data_mart":
        st.subheader(" Product Performance Insights")
        fig = px.scatter(df_mart, x="category", y="total_orders",
                 size="total_revenue", color="category",
                 title="Total Orders vs Revenue by Product Category")
        st.plotly_chart(fig)