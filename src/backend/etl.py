from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from typing import Optional
from datetime import datetime
import os

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLProcessor:
    def __init__(self):
        """Inicializa o processador ETL com Spark."""
        self.spark = self._create_spark_session()
        self.setup_storage()

    def _create_spark_session(self) -> SparkSession:
        """Cria uma sessão Spark configurada."""
        return SparkSession.builder \
            .appName("fakke-insights") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

    def setup_storage(self):
        """Configura o armazenamento S3."""
        try:
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        except Exception as e:
            logger.error(f"Erro ao configurar armazenamento: {str(e)}")
            raise

    def extract(self, n_users: int = 100):
        """Extrai dados da API."""
        try:
            import requests
            response = requests.get(f"http://localhost:8000/users/?n={n_users}")
            if response.status_code == 200:
                data = response.json()
                df = self.spark.createDataFrame(data)
                logger.info(f"✅ Extraídos {df.count()} registros da API")
                return df
            else:
                raise Exception(f"Erro na API: {response.status_code}")
        except Exception as e:
            logger.error(f"Erro ao extrair dados: {str(e)}")
            raise

    def transform(self, df):
        """Transforma os dados usando PySpark."""
        try:
            # Limpeza e padronização
            df = df.withColumn("data_cadastro", to_timestamp(col("data_cadastro")))
            df = df.withColumn("nome", initcap(col("nome")))
            df = df.withColumn("email", lower(col("email")))
            
            # Extrai estado da cidade
            df = df.withColumn("estado", regexp_extract(col("cidade"), r"/([A-Z]{2})$", 1))
            
            # Cria faixas de salário
            df = df.withColumn("faixa_salario", 
                when(col("salario") <= 3000, "Baixo")
                .when(col("salario") <= 6000, "Médio")
                .when(col("salario") <= 10000, "Alto")
                .otherwise("Muito Alto")
            )
            
            # Cria colunas derivadas
            df = df.withColumn("ano_cadastro", year(col("data_cadastro")))
            df = df.withColumn("mes_cadastro", month(col("data_cadastro")))
            df = df.withColumn("dia_cadastro", dayofmonth(col("data_cadastro")))
            
            # Calcula métricas
            df = df.withColumn("idade_empresa_dias", 
                datediff(current_timestamp(), col("data_cadastro"))
            )
            
            logger.info("✅ Dados transformados com sucesso")
            return df
        except Exception as e:
            logger.error(f"Erro ao transformar dados: {str(e)}")
            raise

    def load(self, df, stage: str = "silver"):
        """Carrega os dados para Delta Lake no S3."""
        try:
            # Salva no Delta Lake no S3
            s3_path = f"s3a://{os.getenv('S3_BUCKET')}/delta/{stage}"
            df.write.format("delta") \
                .mode("overwrite") \
                .save(s3_path)
            logger.info(f"✅ Dados salvos no Delta Lake: {s3_path}")

        except Exception as e:
            logger.error(f"Erro ao carregar dados: {str(e)}")
            raise

    def run(self, n_users: int = 100):
        """Executa o pipeline ETL completo."""
        try:
            logger.info("🚀 Iniciando pipeline ETL")
            
    # Extrai
            df = self.extract(n_users)
    
    # Transforma
            df = self.transform(df)
    
    # Carrega
            self.load(df)
            
            logger.info("✅ Pipeline ETL concluído com sucesso")
            
        except Exception as e:
            logger.error(f"❌ Erro no pipeline ETL: {str(e)}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    processor = ETLProcessor()
    processor.run(n_users=100)