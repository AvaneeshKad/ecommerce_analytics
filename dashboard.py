import streamlit as st
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os

# 1. Setup and Config
load_dotenv()
st.set_page_config(page_title="Olist E-Commerce Analytics", layout="wide")
st.title("📦 Olist E-Commerce Insights")

@st.cache_resource
def get_connect():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

try:
    conn = get_connect()

    # 2. Updated SQL Query with Latency Calculation
    query = """
    SELECT 
        payload:order_id::string as order_id,
        payload:order_status::string as order_status,
        DATEDIFF('day', 
            payload:order_purchase_timestamp::timestamp, 
            payload:order_delivered_customer_date::timestamp
        ) as days_to_deliver
    FROM ECOMMERCE_DB.RAW.OLIST_ORDERS_RAW
    LIMIT 1000
    """
    
    df = pd.read_sql(query, conn)
    df.columns = [col.lower().strip() for col in df.columns]

    # 3. KPI Metrics Section
    st.subheader("Quick Stats")
    col1, col2, col3, col4 = st.columns(4)
    
    total_orders = len(df)
    delivered_df = df[df['order_status'] == 'delivered']
    
    avg_delivery = delivered_df['days_to_deliver'].mean() if not delivered_df.empty else 0

    col1.metric("Total Orders", total_orders)
    col2.metric("Delivered", len(delivered_df))
    col3.metric("Canceled", len(df[df['order_status'] == 'canceled']))
    col4.metric("Avg Delivery Time", f"{avg_delivery:.1f} Days")

    # 4. Visualizations
    st.divider()
    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Delivery Latency (Days)")
        if not delivered_df.empty:
            # Histogram of delivery days
            st.bar_chart(delivered_df['days_to_deliver'].value_counts().sort_index())

    with right_col:
        st.subheader("Data Preview (Structured)")
        st.dataframe(df.head(10), use_container_width=True)

except Exception as e:
    st.error(f"❌ Dashboard Error: {e}")