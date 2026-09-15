from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import boto3

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

db_user = os.getenv('user')
db_password = os.getenv('password')
db_database = os.getenv('database')
db_host = os.getenv('host')
db_port = os.getenv('port')


s3_bucket_name = os.getenv('S3_BUCKET_NAME')
s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION'))

def get_engine():
    logging.info(f"-> Conectando ao AWS RDS em {db_host}:{db_port}/{db_database}")
    return create_engine(
        f"postgresql+psycopg2://{db_user}:{quote_plus(db_password)}@{db_host}:{db_port}/{db_database}"
    )

def load_weather_data_to_s3(file_path='data/weather_data.json', s3_key='data/weather_data.json'):
    try:
        logging.info(f"Iniciando upload de {file_path} para o bucket {s3_bucket_name}...")
        s3_client.upload_file(file_path, s3_bucket_name, s3_key)
        logging.info("Upload para o S3 concluído com sucesso!")
    except Exception as e:
        logging.error(f"Erro ao enviar arquivo para o S3: {e}")
        raise e

def load_weather_data(table_name:str, df: pd.DataFrame):
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False
    )

    logging.info("Dados carregados com sucesso! \n")

    df_check = pd.read_sql(f'SELECT COUNT(*) FROM {table_name}', con=engine)
    logging.info(f"Total de registros na tabela: {df_check.iloc[0, 0]}\n")