from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from faker import Faker
import boto3
import os
import tempfile

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_users():
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
        
        # Lendo o arquivo para retornar o conteúdo
        with open(output_path, 'rb') as f:
            return f.read()

def upload_to_s3():
    # Criando um arquivo temporário
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
        # Escrevendo o conteúdo do CSV no arquivo temporário
        temp_file.write(generate_users())
        temp_file.flush()
        
        # Configurando o cliente S3
        s3 = boto3.client('s3')
        bucket_name = 'fakke-insights'
        s3_key = f"users/{datetime.now().strftime('%Y-%m-%d')}/users.csv"
        
        # Fazendo upload do arquivo temporário
        s3.upload_file(temp_file.name, bucket_name, s3_key)
        
        # Limpando o arquivo temporário
        os.unlink(temp_file.name)

with DAG(
    'generate_upload_users',
    default_args=default_args,
    description='Gera usuários falsos e faz upload para o S3',
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    ) 