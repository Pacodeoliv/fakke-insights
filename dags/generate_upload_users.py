from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from faker import Faker
import boto3
import os
import tempfile
import base64

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_users(**context):
    fake = Faker('pt_BR')
    users = []
    
    for _ in range(100):
        user = {
            'name': fake.name(),
            'email': fake.email(),
            'age': fake.random_int(min=18, max=80),
            'gender': fake.random_element(elements=('M', 'F')),
            'location': fake.city(),
            'registration_date': fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
        }
        users.append(user)
    
    df = pd.DataFrame(users)
    
    # Usando um diretório temporário
    with tempfile.TemporaryDirectory() as temp_dir:
        output_path = os.path.join(temp_dir, 'users.csv')
        df.to_csv(output_path, index=False)
        
        # Lendo o arquivo e convertendo para string base64
        with open(output_path, 'rb') as f:
            csv_bytes = f.read()
            return base64.b64encode(csv_bytes).decode('utf-8')

def upload_to_s3(**context):
    # Pegando o conteúdo do CSV da task anterior
    csv_base64 = context['ti'].xcom_pull(task_ids='generate_users')
    csv_bytes = base64.b64decode(csv_base64)
    
    # Criando um arquivo temporário
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
        # Escrevendo o conteúdo do CSV no arquivo temporário
        temp_file.write(csv_bytes)
        temp_file.flush()
        
        # Configurando o cliente S3
        s3 = boto3.client('s3')
        bucket_name = 'fakke-insights'
        s3_key = f"users/{datetime.now().strftime('%Y-%m-%d')}/users.csv"
        
        # Fazendo upload do arquivo temporário
        s3.upload_file(temp_file.name, bucket_name, s3_key)
        
        # Limpando o arquivo temporário
        os.unlink(temp_file.name)
        
        return f's3://{bucket_name}/{s3_key}'

with DAG(
    'generate_upload_users',
    default_args=default_args,
    description='Gera usuários falsos e faz upload para o S3',
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    generate_users_task = PythonOperator(
        task_id='generate_users',
        python_callable=generate_users,
        provide_context=True
    )
    
    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        provide_context=True
    )
    
    generate_users_task >> upload_to_s3_task 