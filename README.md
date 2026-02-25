# h1 Brazil E-Commerce Data Pipeline (Olist)

A robust Data Engineering pipeline designed to ingest, validate, and store Brazilian e-commerce data from local sources to a Cloud Data Lake and finally into a structured PostgreSQL Data Warehouse.


# h2 Project Overview
This project demonstrates an end-to-end ELT (Extract, Load, Transform) pattern using modern data stack tools. The goal is to handle the Olist dataset (100k+ orders) by ensuring data quality and proper schema management before analytical modeling.


# h2 Tech Stack
* Infrastructure as Code: Terraform

* Cloud Storage: Google Cloud Storage (GCS)

* Orchestration & Containerization: Docker, Docker Compose

* Language: Python 3.13 (Managed by uv)

* Data Processing: Pandas, SQLAlchemy

* Database: PostgreSQL 15 (Medallion Architecture: raw, staging, analytics)

# h2 Architecture

1. Infrastructure Setup: Provisioning GCS Buckets using Terraform to ensure reproducible environments.

2. Land to Lake: Uploading raw CSV files from local to GCS.

3. Data Validation: A Python script reads data from GCS, performs quality checks (nulls, duplicates, time-logic), and casts data types (String to Datetime).

4. Loading: Efficiently loading cleaned data into the PostgreSQL raw schema using SQLAlchemy and Pandas.

# h2 Project Structure
Brazil_ECommerce_Pipeline/
├── credentials/          # (Ignored by Git) GCP Service Account keys
├── scripts/
│   ├── ingest_to_gcs.py      # Uploads local CSVs to Google Cloud
│   ├── validate_and_load.py  # Data Quality & Ingestion to Postgres
│   └── init_db.sql           # Database schema initialization
├── terraform/
│   ├── main.tf               # GCS Bucket resources
│   └── variables.tf          # Terraform configurations
├── docker-compose.yml        # PostgreSQL & pgAdmin setup
├── .env                      # Secret credentials
├── .gitignore                # Security configurations
└── README.md