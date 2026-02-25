import pandas as pd
from google.cloud import storage
from sqlalchemy import create_engine
import io
import logging
import os


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SERVICE_ACCOUNT_JSON = "../credentials/brazil-olist-26-ba5639906dfd.json"
BUCKET_NAME = "brazil-olist-data-lake26"
GCS_FOLDER = "olist/2026-02-25"


DATETIME_CONFIG = {
    'olist_orders_dataset.csv': [
        'order_purchase_timestamp', 'order_approved_at', 
        'order_delivered_carrier_date', 'order_delivered_customer_date', 
        'order_estimated_delivery_date'
    ],
    'olist_order_reviews_dataset.csv': [
        'review_creation_date', 'review_answer_timestamp'
    ],
    'olist_order_payments_dataset.csv': [],
    'olist_order_items_dataset.csv' : ['shipping_limit_date']
}

PRIMARY_KEY_CONFIG = {
    'olist_orders_dataset.csv': 'order_id',
    'olist_products_dataset.csv': 'product_id',
    'olist_customers_dataset.csv': 'customer_id',
    'olist_sellers_dataset.csv': 'seller_id'
}

DB_USER = "admin"
DB_PASS = "admin"
DB_HOST = "localhost" 
DB_PORT = "5432"
DB_NAME = "olist_db"

def get_postgres_engine():
    conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)

def load_to_postgres(df, table_name, engine):
    clean_table_name = table_name.replace('.csv', '').replace('olist_', '').replace('_dataset', '')
    
    logging.info(f"Loading {len(df)} rows into table 'raw.{clean_table_name}'")
    
    try:
        df.to_sql(
            name=clean_table_name,
            con=engine,
            schema='raw',
            if_exists='replace',
            index=False,
            chunksize=10000
        )
        logging.info(f"Loaded {clean_table_name} successfully!")
    except Exception as e:
        logging.error(f"Error loading table {clean_table_name}: {e}")


def validate_and_clean(df, file_name):
    logging.info(f"Executing file: {file_name}")
    
    # transform date formats
    date_cols = DATETIME_CONFIG.get(file_name, [])
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    logging.info(f"Converted {len(date_cols)} columns.")

    # Data Quality
    issues = []

    # check duplicate primary key
    pk = PRIMARY_KEY_CONFIG.get(file_name)
    if pk and pk in df.columns:
        duplicate_count = df.duplicated(subset=[pk]).sum()
        if duplicate_count > 0:
            issues.append(f"Notice: {duplicate_count} duplicates rows {pk}.")

    # Check time logic for Orders's tables
    if file_name == 'olist_orders_dataset.csv':
        logic_errors = df[df['order_delivered_customer_date'] < df['order_purchase_timestamp']]
        if not logic_errors.empty:
            issues.append(f"Errors: {len(logic_errors)} rows with invalid delivery date.")

    # print validation results 
    if issues:
        for issue in issues:
            logging.warning(issue)
    else:
        logging.info("Data quality check passed without issues.")

    return df

def run_pipeline():
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=GCS_FOLDER)

    engine = get_postgres_engine()
    
    for blob in blobs:
        if blob.name.endswith(".csv"):
            file_name = blob.name.split('/')[-1]
            content = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(content))
            
            #Execute Validation & Cleaning
            cleaned_df = validate_and_clean(df, file_name)
            
            logging.info(f"Finished processing {file_name}. Total rows: {len(cleaned_df)}\n")

            load_to_postgres(cleaned_df, file_name, engine)

if __name__ == "__main__":
    run_pipeline()