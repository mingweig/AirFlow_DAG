# load_to_bigquery.py
import pandas as pd
from google.cloud import bigquery

# Replace 'your_data_file.csv' with the actual path to your data file

def load_data_to_bigquery(data_file):
    # Read transformed data into a DataFrame
    try:
        transformed_data = pd.read_csv(data_file)
        transformed_data['time'] = pd.to_datetime(transformed_data['time'], errors='coerce')
    except Exception as e:
        print("Error reading transformed data:", str(e))
        return
    print(transformed_data.info())
    print(transformed_data['time'].apply(type).value_counts())
    print(transformed_data['movieid'].apply(type).value_counts())
    print(transformed_data['userid'].apply(type).value_counts())
    print(transformed_data['rating'].apply(type).value_counts())
    print(transformed_data['label_type'].apply(type).value_counts())

    destination_table = "skilful-grove-418313.mlip_film_streaming.streaming_rec"
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        schema = [
            bigquery.SchemaField("time", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("userid", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("rating", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("movieid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("label_type", "STRING", mode="REQUIRED"),
        ],
        write_disposition="WRITE_APPEND",
    )

    load_job = client.load_table_from_dataframe(
        dataframe=transformed_data,
        destination=destination_table,
        job_config=job_config
    )

    load_job.result()  # Wait for the job to complete

    print(f"Loaded {len(transformed_data)} records into {destination_table}")
    print("Data loaded successfully.")

if __name__ == '__main__':
    csv_path = '/transformed_data.csv'
    load_data_to_bigquery(csv_path)
