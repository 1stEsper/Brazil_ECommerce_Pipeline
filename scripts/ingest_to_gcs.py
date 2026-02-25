import os 
from google.cloud import storage

SERVICE_ACCOUNT_JSON = "../credentials/brazil-olist-26-ba5639906dfd.json"
BUCKET_NAME = "brazil-olist-data-lake26"
LOCAL_DATA_PATH = "../data/raw"

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    print(f"Uploading file {source_file_path} to {destination_blob_name} in bucket {bucket_name}")

    blob.upload_from_filename(source_file_path)

    print("File uploaded successfully.")

def main(): 
    for filename in os.listdir(LOCAL_DATA_PATH): 
        if filename.endswith(".csv"):
            local_path = os.path.join(LOCAL_DATA_PATH, filename)

            import datetime
            today = datetime.date.today().strftime("%Y-%m-%d")
            destination_blob_name = f"olist/{today}/{filename}"
            upload_to_gcs(BUCKET_NAME, local_path, destination_blob_name)


if __name__ == "__main__": 
    main()
