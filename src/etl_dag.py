import csv
import time
import hashlib
import random
import logging
from io import StringIO, BytesIO
from datetime import timedelta

from boto3 import resource  # type: ignore
import pandas as pd  # type: ignore

from airflow import DAG  # type: ignore
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore


AWS_ACCESS_KEY_ID = 'YCAJEZBqrd9eCpbuwP9Naze_F'
AWS_SECRET_ACCESS_KEY = 'YCOIypzb_j8gMOb8Sunvo-raaxK2v07ybmgZTBUr'
TARGET_YEAR = 2000
PATH_TO_DATA = 'https://raw.githubusercontent.com/peter-and-wolf/otus_mlops_airflow/refs/heads/main/data/GlobalLandTemperaturesByCountry.csv'
S3_ENDPOINT_URL = 'http://storage.yandexcloud.net'
S3_BUCKET_NAME = 'bucket-43'


default_args = {
    "owner": "peter-and-wolf",
    "depends_on_past": False,
    "start_date": "2025-05-18",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def generate_key() -> str:
  return hashlib.md5(f'{random.random()}{time.time()}'.encode('utf-8')).hexdigest() + '.csv'


def extract_data(path: str, year: int = TARGET_YEAR) -> str:
  logging.info(f'Extracting data from {path} for year {year}')
  df = pd.read_csv(path)
  df['Year'] = pd.to_datetime(df['dt'], format="%Y-%m-%d").dt.year
  return df[df['Year'] >= year].reset_index().to_csv()


def transform_data(**kwargs) -> str:
  logging.info(f'Transforming data')
  
  # get data from extract_data task using xcom_pull
  ti = kwargs['ti']
  data = ti.xcom_pull(task_ids='extract_data')
  
  df = pd.read_csv(StringIO(data))
  gb = df.groupby('Country')['AverageTemperature'] 
  return gb.median().reset_index().to_csv()


def load_data(**kwargs) -> None:
  logging.info(f'Loading data to S3')

  ti = kwargs['ti']
  data = ti.xcom_pull(task_ids='transform_data')
  s3 = resource(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
  )
  bucket = s3.Bucket(S3_BUCKET_NAME)
  bucket.upload_fileobj(
    BytesIO(bytes(data, encoding='utf-8')),
    f'temperature/{generate_key()}'
  )

with DAG(
  'simple_etl',
  default_args=default_args,
  description="A simple ETL pipeline",
  schedule=timedelta(days=1),
  catchup=False,
  tags=['etl'],
) as dag:
  
  extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_kwargs={'path': PATH_TO_DATA},
  )

  transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
  )

  load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
  )

  extract_task >> transform_task >> load_task
