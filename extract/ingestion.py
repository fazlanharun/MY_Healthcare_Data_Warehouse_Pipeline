
from extract.ingest_utils import create_bucket_if_not_exists, create_dataset_if_not_exists, fetch_data, upload_df_to_gcs, load_to_bigquery
from config import BUCKET_NAME
 
HOSPITAL_BEDS_URL = "https://api.data.gov.my/data-catalogue?id=hospital_beds" #data from 2015-2022
POPULATION_URL = "https://storage.dosm.gov.my/population/population_state.csv" #latest data 2025
HEALTHCARE_STAFF_URL = "https://api.data.gov.my/data-catalogue?id=healthcare_staff" #data from 2014-2022, public sector healthcare staff

def run_ingest():
    create_bucket_if_not_exists(BUCKET_NAME)
    create_dataset_if_not_exists()

    datasets = [
        (HOSPITAL_BEDS_URL, "hospital_beds","json"),
        (POPULATION_URL, "population_state","csv"),
        (HEALTHCARE_STAFF_URL, "healthcare_staff","json"),
    ]
    
    for url,table_name,file_type in datasets:
        data = fetch_data(url,file_type)
        #data.to_csv(f"extract/{table_name}.csv", index=False)
        gcs_uri = upload_df_to_gcs(data, BUCKET_NAME, f"raw/{table_name}.csv")
        load_to_bigquery(gcs_uri, table_name)

if __name__ == "__main__":
    run_ingest()