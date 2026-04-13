import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import os
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

st.set_page_config(page_title="Sentinel Fraud Monitor", page_icon="🛡️", layout="wide")

@st.cache_resource
def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

st.title("🛡️ Sentinel: E-Commerce Risk Monitor")

try:
    conn = get_snowflake_conn()
    
    # Logic remains the same, but now it's secure
    query = """
    SELECT 
        ORDER_ID, 
        PAYMENT_TYPE, 
        PAYMENT_AMOUNT, 
        TRANSACTION_RISK_PROFILE 
    FROM ECOMMERCE_DB.DBT_AKAD.FCT_ORDER_PAYMENTS
    """
    
    df = pd.read_sql(query, conn)

    # Metrics Row
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Transactions", f"{len(df):,}")
    m2.metric("Flagged Risk", len(df[df['TRANSACTION_RISK_PROFILE'] != 'standard']))
    m3.metric("Avg Value", f"${df['PAYMENT_AMOUNT'].mean():.2f}")

    # Charts
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(px.pie(df, names='TRANSACTION_RISK_PROFILE', hole=0.4), use_container_width=True)
    with c2:
        st.plotly_chart(px.histogram(df, x='PAYMENT_TYPE', color='TRANSACTION_RISK_PROFILE', barmode='group'), use_container_width=True)

    st.subheader("🕵️ Transaction Audit")
    st.dataframe(df, width='stretch')

except Exception as e:
    st.error(f"Connection Error: {e}")