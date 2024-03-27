from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
import pandas as pd
from kafka import KafkaConsumer
from tqdm import tqdm
import re
import gc
from google.cloud import bigquery

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['mingweig99@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kafka_extraction_dag',
    default_args=default_args,
    description='Kafka ETL DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    def extract_kafka_data(**kwargs):
        ti = kwargs['ti']
        consumer = KafkaConsumer(
            'movielog12',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            group_id='team12_m2',
            # enable_auto_commit=True,
            # auto_commit_interval_ms=1000,
            consumer_timeout_ms=5000 
        )


        # Collect 10 seconds of data
        end_time = datetime.now() + timedelta(seconds=10)
        data = []
        while datetime.now() < end_time:
            for message in consumer:
                decoded_message = message.value.decode('utf-8')
                data.append(decoded_message)
                if datetime.now() >= end_time:
                    break
        

        # This example will just print the number of collected records
        print(f"Extracted {len(data)} records from Kafka.")

        # push this data to XCom for the next task or save it somewhere
        ti.xcom_push(key='data', value=data)

        # Don't forget to close the consumer
        consumer.close()

    extract_task = PythonOperator(
        task_id='extract_kafka_data',
        python_callable=extract_kafka_data,
    )

    def transform_data(**kwargs):
        
        def assign_rating(minute):

            if minute >= 130:
                return 5
            elif minute >= 100:
                return 4
            elif minute >= 75:
                return 3
            elif minute >= 30:
                return 2
            else:
                return 1

        ti = kwargs['ti']
        data_extracted = ti.xcom_pull(task_ids='extract_kafka_data', key='data')
        print(f"Transforming {len(data_extracted)} records...")
        # Split each string into a list of values

        split_data = [item.split(',') for item in tqdm(data_extracted) if len(item.split(',')) == 3]

        # Define column names
        columns = ['time', 'userid', 'request']

        pd.options.mode.chained_assignment = None  # default= 'warn'
        # Create a DataFrame
        data = pd.DataFrame(split_data, columns=columns)

        data['time'] = data['time'].apply(lambda x: re.sub(r"[^\d\-T:.]", "", x))
        data['time'] = pd.to_datetime(data['time'], errors='coerce')
        data.dropna(subset=['time'], inplace=True)
        data['path'] = data['request'].str.split(' ').str[1]
        data['rating'] = data['path'].str.extract(r'=(\d)$')
        data.drop(columns=['request'],inplace=True)

        print('Processing data with real rating...')
        data_ratings = data.dropna(subset=['rating']) # .drop(columns=['movieid', 'request', 'minute'])
        data_ratings['movieid'] = data_ratings['path'].str.extract(r'/rate/(.+?)=\d')[0]
        data_ratings.drop(columns=['path'],inplace=True)
        data_ratings['rating'] = data_ratings['rating'].astype(int)
        data_ratings['label_type'] = 'real'
        gc.collect()

        print('Cleaning log data...')
        data['movieid'] = data['path'].str.extract(r'/m/(.+?)/\d+\.mpg')[0]
        data['minute'] = data['path'].str.extract(r'/(\d+)\.mpg')[0]
        data.drop(columns=['path'],inplace=True)
        data.dropna(subset=['movieid'],inplace=True)
        data['minute'] = data['minute'].astype(int)
        gc.collect()

        print('Generating data with rating based on watching minutes...')
        max_time = data.groupby(['userid', 'movieid'])['time'].max().reset_index()
        stream_result = pd.merge(max_time, data, on=['userid', 'movieid', 'time'], how='left')
        del data, max_time
        gc.collect()

        stream_result['rating'] = stream_result['minute'].apply(assign_rating)
        stream_result.drop(columns = ['minute'],inplace=True)
        stream_result['label_type'] = 'gen'

        print('Merging Data...')
        merged_data = pd.concat([data_ratings, stream_result], ignore_index=True)
        del data_ratings, stream_result
        gc.collect()
        merged_data.drop_duplicates(subset=['userid', 'movieid', 'rating'], keep='first', inplace=True)
        merged_data['userid'] = pd.to_numeric(merged_data['userid'], errors='coerce')
        merged_data.dropna(subset=['userid'], inplace=True)
        merged_data['userid'] = merged_data['userid'].astype(int)

        print(f'Transformed data with {len(merged_data)} records...')

        ti.xcom_push(key='transformed_data', value=merged_data)

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    
    # Placeholder for load task
    # def load_data(**kwargs):
    #     ti = kwargs['ti']
    #     transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    #     print(f"Loading {len(transformed_data)} records into BigQuery...")

    #     # Specify your BigQuery dataset and table
    #     destination_table = "skilful-grove-418313.mlip_film_streaming.streaming_rec"
        
    #     # Initialize a BigQuery Client
    #     client = bigquery.Client()
        
    #     # Configure the load job
    #     job_config = bigquery.LoadJobConfig(
    #         schema = [
    #             bigquery.SchemaField("time", "TIME", mode="REQUIRED"),
    #             bigquery.SchemaField("userid", "INTEGER", mode="REQUIRED"),
    #             bigquery.SchemaField("rating", "FLOAT", mode="REQUIRED"),
    #             bigquery.SchemaField("movieid", "STRING", mode="REQUIRED"),
    #             bigquery.SchemaField("label_type", "STRING", mode="REQUIRED"),
    #             ],
    #         write_disposition="WRITE_APPEND",  # Use WRITE_TRUNCATE to overwrite the table
    #     )
        
    #     # Execute the load job
    #     load_job = client.load_table_from_dataframe(
    #         dataframe=transformed_data,
    #         destination=destination_table,
    #         job_config=job_config
    #     )
        
    #     # Wait for the load job to complete
    #     load_job.result()
        
    #     print(f"Loaded {len(transformed_data)} records into {destination_table}")

    # load_task = PythonOperator(
    #     task_id='load_data',
    #     python_callable=load_data,
    # )

    def load_data_ext(**kwargs):
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
        csv_path = '/Users/gaomingwei/Desktop/2024_Spring/11_695/my_etl_project/transformed_data.csv'
        transformed_data.to_csv(csv_path, index=False)
    
    load_task_ext = PythonOperator(
        task_id='load_data_ext',
        python_callable=load_data_ext,
    )
    
    run_external_load_script = BashOperator(
        task_id='run_external_load_script',
        bash_command='python /bigquery.py',
    )

    

    # Set task dependencies
    extract_task >> transform_task >> load_task_ext >> run_external_load_script
