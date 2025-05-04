import boto3
import logging
import os
from datetime import datetime

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_athena_table():
    """Cria tabela no Athena para consultar dados do Delta Lake."""
    try:
# Configuração do cliente Athena
        athena = boto3.client('athena')
        s3_bucket = os.getenv('S3_BUCKET')
        database = 'fakke_insights'
        table = 'users_silver'

        # Cria o banco de dados se não existir
    try:
            athena.start_query_execution(
                QueryString=f'CREATE DATABASE IF NOT EXISTS {database}',
                ResultConfiguration={'OutputLocation': f's3://{s3_bucket}/athena-results/'}
        )
            logger.info(f"✅ Banco de dados {database} criado/verificado")
    except Exception as e:
            logger.error(f"Erro ao criar banco de dados: {str(e)}")
            raise

        # Cria a tabela externa para o Delta Lake
        create_table_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table}
        USING DELTA
        LOCATION 's3://{s3_bucket}/delta/silver/'
        """
        
        response = athena.start_query_execution(
            QueryString=create_table_query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': f's3://{s3_bucket}/athena-results/'}
        )
        
        logger.info(f"✅ Tabela {table} criada no Athena")
        logger.info(f"Query ID: {response['QueryExecutionId']}")

    except Exception as e:
        logger.error(f"❌ Erro ao criar tabela no Athena: {str(e)}")
        raise

if __name__ == "__main__":
    create_athena_table() 