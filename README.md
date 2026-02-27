# Brazil E-Commerce Data Pipeline (Olist)

A robust Data Engineering pipeline designed to ingest, validate, and store Brazilian e-commerce data from local sources to a Cloud Data Lake and finally into a structured PostgreSQL Data Warehouse.


## Project Overview
This project demonstrates an end-to-end ELT (Extract, Load, Transform) pattern using modern data stack tools. The goal is to handle the Olist dataset (100k+ orders) by ensuring data quality and proper schema management before analytical modeling.


## Tech Stack
* **Infrastructure as Code**: Terraform

* **Cloud Storage**: Google Cloud Storage (GCS)

* **Orchestration & Containerization**: Docker, Docker Compose

* **Language**: Python(Managed by uv)

* **Data Processing**: Pandas, SQLAlchemy

* **Database**: PostgreSQL (Medallion Architecture: raw, staging, analytics)

## Architecture

1. **Infrastructure Setup**: Provisioning GCS Buckets using Terraform to ensure reproducible environments.

2. **Land to Lake**: Uploading raw CSV files from local to GCS.

3. **Data Validation**: A Python script reads data from GCS, performs quality checks (nulls, duplicates, time-logic), and casts data types (String to Datetime).

4. **Loading**: Efficiently loading cleaned data into the PostgreSQL raw schema using SQLAlchemy and Pandas.

## Project Structure
````
Brazil_ECommerce_Pipeline/
├── credentials/          # (Ignored by Git) GCP Service Account keys
├── scripts/
│   ├── ingest_to_gcs.py      # Uploads local CSVs to Google Cloud
│   ├── validate_and_load.py  # Data Quality & Ingestion to Postgres
│   └── init_db.sql           # Database schema initialization
├── transform/                # using dbt
│   ├── analyses              # Some analyses (Retention Rate, Geo distribution)
│   ├── models                # raw --> staging --> marts
│   └── test                  # Automated data testing
├── terraform/
│   ├── main.tf               # GCS Bucket resources
│   └── variables.tf          # Terraform configurations
├── docker-compose.yml        # PostgreSQL & pgAdmin setup
├── .env                      # Secret credentials
├── .gitignore                # Security configurations
└── README.md
````

## Setup & Installation
1. Infrastructure
Initialize and apply Terraform to create the GCS bucket: 

``` 
cd terraform
terraform init
terraform apply 
``` 

2. Database Environment
Build the PostgreSQL and pgAdmin containers: 
``` 
docker compose up -d
```

3. Data pipeline 
Install dependencies using ```uv``` n run the ingestion script: 
``` 
uv pip install -r requirements.txt
python scripts/validate_and_load.py
```  

## Data quality features
* **Primary Key Validation**: Ensures no duplicate IDs in core tables (orders, products, etc.).
* **Schema Enforcement**: converts date columns from string to timestamp.
* **Logic Checks**: Validates business logic (e.g., ensuring order_delivered_customer_date is not earlier than order_purchase_timestamp).
* **Medallion Architecture**: Data is isolated into a ```raw``` schem, keeping it separate from future transformation layers.

