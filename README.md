# 🛡️ E-Commerce Analytics & Risk Pipeline

An end-to-end data engineering platform designed to ingest, transform, and visualize transactional data to identify high-risk e-commerce activity. This project demonstrates a high-performance stack combining systems programming (Rust) with modern cloud data warehousing (Snowflake) and real-time visualization (Streamlit).

## 🏗️ Architecture & Workflow

1.  **Ingestion (Rust):** A high-performance ingestion engine built in Rust to handle raw Olist dataset loading with memory safety and concurrency.
2.  **Warehousing (Snowflake):** Centralized cloud data warehouse where raw data is staged and prepared for transformation.
3.  **Transformation (dbt):** Modular SQL modeling using dbt (Data Build Tool) to engineer the `FCT_ORDER_PAYMENTS` core table and apply transaction risk scoring logic.
4.  **Monitoring (Streamlit):** A secure, interactive dashboard for auditing transaction KPIs and monitoring risk profiles.

## 🛠️ Tech Stack

* **Systems:** Rust (Cargo)
* **Data:** Snowflake, dbt (Data Build Tool)
* **Orchestration/UI:** Python 3.12, Streamlit, Pandas, Plotly
* **Environment:** WSL2 (Ubuntu), EndeavourOS

## 🚀 Getting Started

### Prerequisites
* A Snowflake Account
* Python 3.12+
* Rust/Cargo (to run the ingestion layer)

### Installation
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/AvaneeshKad/ecommerce_analytics.git]
    cd ecommerce_analytics
    ```

2.  **Set up the Virtual Environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Configure Environment Variables:**
    Create a `.env` file in the root directory (this file is git-ignored for security):
    ```text
    SNOWFLAKE_USER=your_user
    SNOWFLAKE_PASSWORD=your_password
    SNOWFLAKE_ACCOUNT=your_account_locator
    SNOWFLAKE_WAREHOUSE=your_wh
    SNOWFLAKE_DATABASE=ECOMMERCE_DB
    SNOWFLAKE_SCHEMA=DBT_AKAD
    ```

### Running the Dashboard
```bash
streamlit run dashboard.py
