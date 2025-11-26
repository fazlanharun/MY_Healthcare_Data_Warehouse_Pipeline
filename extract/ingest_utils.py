import requests
import pandas as pd
from google.cloud import storage,bigquery
from config import GCP_PROJECT_ID, BQ_DATASET


def create_bucket_if_not_exists(bucket_name):
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        bucket = storage_client.create_bucket(bucket_name,location='asia-southeast1')
        print(f"Bucket {bucket_name} created.")
    return bucket

def create_dataset_if_not_exists():
    client = bigquery.Client(project = GCP_PROJECT_ID)
    datasets =  [ 'raw_api_dump','staging','intermediate','mart']

    for dataset_id in datasets:
        dataset = bigquery.Dataset(f"{GCP_PROJECT_ID}.{dataset_id}")
        dataset.location = "asia-southeast1"
        try:
            client.create_dataset(dataset)
            print(f"Created dataset {dataset_id}")
        except Exception as e:
            print(f"Dataset {dataset_id} already exists.")
    

def fetch_data(url, file_type="json"):
        
    if file_type == "json":
        response_json = requests.get(url).json()
        df = pd.DataFrame(response_json)
    elif file_type == "csv":
        df = pd.read_csv(url)
    else:
        raise ValueError("Unsupported file type. Use 'json' or 'csv'.")
    print(f"\nFetched {len(df)} records from {url}")
    return df
    
def upload_df_to_gcs(df, bucket_name, blob_name):
    '''Args:blob_name (str): Path/filename in GCS.
    '''
    csv_data = df.to_csv(index=False)
    
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_string(csv_data, 'text/csv')
    gcs_uri = f"gs://{bucket_name}/{blob_name}"
    print(f"Uploaded dataframe to cloud_storage_bucket at {gcs_uri}")
    return gcs_uri

def load_to_bigquery(gcs_uri,table_name):
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True
        )
    job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows into [dataset: {BQ_DATASET}], [table: {table_name}] at {table_id}.")