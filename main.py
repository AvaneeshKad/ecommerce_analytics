import os
import subprocess
import snowflake.connector
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

# --- DYNAMIC CONFIGURATION ---
RUST_PROJECT_DIR = "./ingestor"
RUST_BINARY = "./ingestor/target/release/ingestor"
PAYMENTS_FILE = "./ingestor/data/orders_payments.parquet"

def run_rust_engine():
    """
    Automates the Rust lifecycle:
    1. Compiles the code (Cargo build)
    2. Executes the binary to process CSV -> Parquet
    """
    print("🚀 [1/3] Building and Launching Rust Muscle...")
    try:
        print("🛠️  Compiling Rust (Release Mode)...")
        subprocess.run(
            ["cargo", "build", "--release"], 
            check=True, 
            cwd=RUST_PROJECT_DIR
        )
        
        print("🏃 Running Ingestor to process Payments...")
        subprocess.run([RUST_BINARY], check=True)
        
        print("✅ Rust: Data processed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Rust Engine/Build Failed: {e}")
        return False
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        return False

def upload_payments_to_snowflake():
    """
    Connects to Snowflake and pushes the Parquet file into the OLIST_PAYMENTS_RAW table.
    """
    print(f"☁️  [2/3] Connecting to Snowflake...")
    
    try:
        ctx = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        cs = ctx.cursor()

        # Step A: Ensure the table structure matches our Parquet data
        print("🛠️  Ensuring OLIST_PAYMENTS_RAW table structure is correct...")
        cs.execute("DROP TABLE IF EXISTS OLIST_PAYMENTS_RAW")
        cs.execute("""
            CREATE TABLE OLIST_PAYMENTS_RAW (
                order_id STRING,
                payment_sequential INTEGER,
                payment_type STRING,
                payment_installments INTEGER,
                payment_value FLOAT
            )
        """)

        # Step B: Upload the Parquet file to the internal table stage
        print(f"📤 [3/3] Uploading {PAYMENTS_FILE} to Snowflake Stage...")
        cs.execute(f"PUT file://{PAYMENTS_FILE} @%OLIST_PAYMENTS_RAW AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

        # Step C: Load the data using explicit positional mapping
        print("📥 Finalizing: Copying Payments into Table...")
        cs.execute("""
            COPY INTO OLIST_PAYMENTS_RAW 
            FROM (
              SELECT 
                $1:order_id::string, 
                $1:payment_sequential::int, 
                $1:payment_type::string, 
                $1:payment_installments::int, 
                $1:payment_value::float
              FROM @%OLIST_PAYMENTS_RAW
            )
            FILE_FORMAT = (TYPE = PARQUET)
            PURGE = TRUE
        """)

        print("🏁 SUCCESS: Payments Pipeline Complete.")

    except Exception as e:
        print(f"❌ Snowflake Error: {e}")
    finally:
        if 'cs' in locals(): cs.close()
        if 'ctx' in locals(): ctx.close()

if __name__ == "__main__":
    if run_rust_engine():
        upload_payments_to_snowflake()