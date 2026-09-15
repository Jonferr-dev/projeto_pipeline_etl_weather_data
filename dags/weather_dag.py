from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pathlib import Path
import sys, os

sys.path.insert(0, '/opt/airflow/src')

from extract_data import extract_weather_data
from load_data import load_weather_data
from load_data import load_weather_data_to_s3
from transform_data import data_transformations
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

API_KEY = os.getenv('API_KEY')
url = f"https://api.openweathermap.org/data/2.5/weather?q=Sao%20Paulo,BR&units=metric&appid={API_KEY}"

@dag(
    dag_id='projeto_pipeline_weather',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    description ='Pipeline ETL - Clima SP',
    schedule ='0 * * * *',
    start_date=datetime(2026, 9, 14),
    catchup=False,
    tags=['weather', 'etl']
)
def weather_pipeline():

    @task
    def extract():
        extract_weather_data(url)

    @task
    def transform():
        df = data_transformations()
        df.to_parquet('/opt/airflow/data/temp_data.parquet', index=False)

    @task
    def load_rds():
        import pandas as pd
        df = pd.read_parquet('/opt/airflow/data/temp_data.parquet')
        load_weather_data('sp_weather', df)

    @task
    def load_s3():
        load_weather_data_to_s3(
            file_path='/opt/airflow/data/temp_data.parquet',
            s3_key='data/sp_weather.parquet'
        )

    t_extract = extract()
    t_transform = transform()
    t_load_rds = load_rds()
    t_load_s3 = load_s3()
    
    t_extract >> t_transform >> [t_load_rds, t_load_s3]

weather_pipeline()

