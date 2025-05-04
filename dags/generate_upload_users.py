from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from faker import Faker
import pandas as pd
import boto3
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_users(**context):
    fake = Faker()
    users = []
    
    for _ in range(100):
        user = {
            'id': fake.uuid4(),
            'name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.address(),
            'company': fake.company(),
            'job': fake.job(),
            'created_at': datetime.now().isoformat()
        }
        users.append(user)
    
    df = pd.DataFrame(users)
    output_path = '/opt/airflow/data/users.csv'
    df.to_csv(output_path, index=False)
    
    return output_path

def upload_to_s3(**context):
    output_path = context['ti'].xcom_pull(task_ids='generate_users')
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'sa-east-1')
    )
    
    bucket_name = 'fakke-insights'
    s3_key = f'users/{datetime.now().strftime("%Y-%m-%d")}/users.csv'
    
    s3.upload_file(output_path, bucket_name, s3_key)
    
    return f's3://{bucket_name}/{s3_key}'

with DAG(
    'generate_upload_users',
    default_args=default_args,
    description='Generate fake users and upload to S3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['users', 's3'],
) as dag:
    
    generate_users_task = PythonOperator(
        task_id='generate_users',
        python_callable=generate_users,
        provide_context=True,
    )
    
    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        provide_context=True,
    )
    
    generate_users_task >> upload_to_s3_task 