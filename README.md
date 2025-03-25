#  Event-Driven Data Pipeline for E-Commerce
## Project Overview
This project implements an event-driven data pipeline for processing and analyzing e-commerce data. It ingests customer orders, processes real-time events, and provides business insights through an interactive Streamlit dashboard powered by Google BigQuery.

##  Features
Incremental Data Loading: Efficient data ingestion to avoid redundancy.

Schema Validation: Ensures data consistency before storage.

Star Schema Modeling: Optimized for analytical queries.

BigQuery for Analytics: Handles large-scale data with partitioning & clustering.

Streamlit Dashboard: Visualizes key business metrics like revenue growth, customer retention, and order trends.

##  Data Pipeline Architecture
Ingestion: Captures real-time & batch data from e-commerce transactions.

Processing: Cleans and transforms data using Python & BigQuery SQL.

Storage: Organizes data into fact & dimension tables for analysis.

Visualization: Presents insights through interactive charts in Streamlit.

##  Tech Stack
Python: Data processing & transformation

Google BigQuery: Storage & SQL-based analytics

Streamlit: Frontend dashboard for visualization

Pandas: Data manipulation

Plotly: Interactive charts & graphs

##  Key Insights
Identified top revenue-generating product categories.

Analyzed customer order trends by state & city.

Measured revenue growth and customer retention over time.

Detected shopping behavior patterns to improve business strategies.

##  Challenges & Solutions
Handling Large Data: Used BigQuery partitioning & clustering.

Ensuring Data Accuracy: Applied schema validation & error handling.

Optimizing Query Performance: Designed Star Schema for faster analytics.
